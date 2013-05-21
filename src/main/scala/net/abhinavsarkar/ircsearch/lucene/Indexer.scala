package net.abhinavsarkar.ircsearch.lucene

import java.io.File
import java.util.ArrayList
import java.util.Date
import java.util.concurrent.{ Executors, Future, PriorityBlockingQueue, TimeUnit }
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConversions._
import scala.collection.Seq
import scala.collection.mutable
import scala.math.Ordered

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{ Field, LongField, StringField, TextField }
import org.apache.lucene.index.{ IndexWriter, IndexWriterConfig }
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version

import com.google.common.util.concurrent.RateLimiter
import com.typesafe.scalalogging.slf4j.Logging

import net.abhinavsarkar.ircsearch.model._

object Indexer extends Logging {

  case class IndexRecord(
      server : String, channel : String, botName : String, chatLine : ChatLine,
      indexed : Boolean = false)
      extends Ordered[IndexRecord] {
    def compare(that : IndexRecord) = {
      val diff = this.chatLine.timestamp - that.chatLine.timestamp
      if (diff > 0) 1 else if (diff < 0) -1 else 0
    }
  }

  object IndexRecord {
    def fromIndexRequest(indexRequest : IndexRequest) = {
      val IndexRequest(server, channel, botName, chatLines) = indexRequest
      for {
        chatLine <- chatLines
      } yield new IndexRecord(server, channel, botName, chatLine)
    }
  }

  val LUCENE_VERSION = Version.LUCENE_43
  val ContextSize = 2
  val ContextDurationSecs = 20
  val IndexingDurationSecs = 10
  val FlushDurationSecs = 60
  val RateLimitPerSec = 1000

  private val indexQueue = new PriorityBlockingQueue[IndexRecord]
  private val scheduler = Executors.newScheduledThreadPool(2)
  private val runLock = new ReentrantLock
  private val rateLimiter = RateLimiter.create(RateLimitPerSec)
  private var indexingFuture : Future[_] = null
  private var flushFuture : Future[_] = null

  private val indexers = mutable.Map[String, IndexWriter]()

  private def close {
    for (indexer <- indexers.values)
      indexer.close
    logger.info("Closed Indexer")
  }

  private def flush {
    for (indexer <- indexers.values)
      indexer.commit
    logger.info("Flushed Indexer")
  }

  def mkAnalyzer : Analyzer = {
    val defAnalyzer = new StandardAnalyzer(LUCENE_VERSION)
    val fieldAnalyzers = Map(
        ChatLine.USER -> new KeywordAnalyzer,
        ChatLine.MSG -> new EnglishAnalyzer(LUCENE_VERSION),
        ChatLine.CTXB -> new EnglishAnalyzer(LUCENE_VERSION),
        ChatLine.CTXA -> new EnglishAnalyzer(LUCENE_VERSION))

    new PerFieldAnalyzerWrapper(defAnalyzer, fieldAnalyzers)
  }

  private def getIndexWriter(dirPath : String) : IndexWriter = {
    synchronized {
      if (!(indexers contains dirPath)) {
        val indexDir = new File(dirPath)
        if (indexDir.exists) {
          assert(indexDir.isDirectory)
        }
        val indexer = new IndexWriter(FSDirectory.open(indexDir),
            new IndexWriterConfig(LUCENE_VERSION, mkAnalyzer))
        indexers += (dirPath -> indexer)
      }
    }

    indexers(dirPath)
  }

  def getIndexDir(server : String, channel : String, botName : String) : String =
    s"index-$server-$channel-$botName"

  def index(indexRequest : IndexRequest) =
    IndexRecord.fromIndexRequest(indexRequest).foreach { rec =>
      rateLimiter.acquire
      indexQueue put rec
    }

  private def doInLock(f : => Unit) {
    try {
      runLock.lock
      f
    } finally {
      runLock.unlock
    }
  }

  implicit private def funcToRunnable(f : => Unit) : Runnable = new Runnable {
    def run {
      try { f }
      catch {
        case e : Throwable => logger.error("Exception while running", e)
      }
    }}

  private def schedule(initialDelay : Int, delay : Int, unit : TimeUnit)(f : => Unit) = {
    scheduler.scheduleWithFixedDelay(f, initialDelay, delay, unit)
  }

  private def fillContext(rec: IndexRecord, recs: Seq[IndexRecord], idx : Int) = {
    rec.copy(chatLine =
      rec.chatLine.copy(
        contextBefore = recs.slice(idx - ContextSize, idx).map(_.chatLine)
        .filter(_.timestamp >= rec.chatLine.timestamp - ContextDurationSecs * 1000)
        .toList,
        contextAfter = recs.slice(idx + 1, 2 * ContextSize + 1).map(_.chatLine)
        .filter(_.timestamp <= rec.chatLine.timestamp + ContextDurationSecs * 1000)
        .toList))
  }

  def start {
    logger.info("Starting indexer")
    indexingFuture = schedule(0, IndexingDurationSecs.max(ContextDurationSecs), TimeUnit.SECONDS) {
      if (!indexQueue.isEmpty) {
        val indexRecs = new ArrayList[IndexRecord]
        indexQueue drainTo indexRecs
        val indexRecsMap = indexRecs groupBy { r => (r.server, r.channel, r.botName) }

        val windowSize = 2 * ContextSize + 1
        for (indexRecBatch <- indexRecsMap.values) {
          for (recs <- indexRecBatch.sliding(windowSize)) {
            if (recs.size == windowSize) {
              doInLock {
                doIndex(fillContext(recs(ContextSize), recs, ContextSize))
              }
            } else {
              recs.foreach(indexQueue.put)
            }
          }

          if (indexRecBatch.size >= windowSize) {
            indexRecBatch.slice(indexRecBatch.length - 2 * ContextSize, indexRecBatch.length)
            .zipWithIndex
            .map { r => if (r._2 < ContextSize) r._1.copy(indexed = true) else r._1 }
            .foreach(indexQueue.put)
          }
        }
      }
    }
    flushFuture = schedule(0, FlushDurationSecs, TimeUnit.SECONDS) {
      doInLock(flush)
    }
  }

  def stop {
    doInLock {
      if (indexingFuture != null) {
        indexingFuture.cancel(false)
        indexingFuture = null
      }
      if (flushFuture != null) {
        flushFuture.cancel(false)
        flushFuture = null
      }
      close
      logger.info("Stopped indexer")
    }
  }

  private def ctxToStr(ctx : List[ChatLine]) =
    ctx.map { line => s"${line.timestamp} ${line.user}: ${line.message}" }  mkString "\n"

  private def doIndex(indexRecord: IndexRecord) {
    val IndexRecord(server, channel, botName, chatLine, indexed) = indexRecord
    if (!indexed) {
      val indexDir = getIndexDir(server, channel, botName)
      val indexWriter = getIndexWriter(indexDir)
      val ts = new LongField(ChatLine.TS, chatLine.timestamp, Field.Store.YES)
      val user = new StringField(ChatLine.USER, chatLine.user, Field.Store.YES)
      val msg = new TextField(ChatLine.MSG, chatLine.message, Field.Store.YES)
      val ctxBfr = new TextField(ChatLine.CTXB, ctxToStr(chatLine.contextBefore), Field.Store.YES)
      val ctxAft = new TextField(ChatLine.CTXA, ctxToStr(chatLine.contextAfter), Field.Store.YES)
      indexWriter.addDocument(List(ts, user, msg, ctxBfr, ctxAft), indexWriter.getAnalyzer)
      logger.debug("Indexed : [{} {} {}] [{}] {}: {}",
          server, channel, botName, new Date(chatLine.timestamp), chatLine.user, chatLine.message)
    }
  }

}