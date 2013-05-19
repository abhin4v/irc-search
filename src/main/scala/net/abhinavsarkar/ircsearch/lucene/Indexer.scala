package net.abhinavsarkar.ircsearch.lucene

import java.io.File
import java.util.ArrayList
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.LongField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import com.typesafe.scalalogging.slf4j.Logging
import net.abhinavsarkar.ircsearch.model._
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue

object Indexer extends Logging {

  val LUCENE_VERSION = Version.LUCENE_43

  private val indexReqQueue = new LinkedBlockingQueue[IndexRequest](10000)
  private val scheduler = Executors.newScheduledThreadPool(2)
  private val runLock = new ReentrantLock
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
        ChatLine.MSG -> new EnglishAnalyzer(LUCENE_VERSION))

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

  def index(indexRequest : IndexRequest) = indexReqQueue.put(indexRequest)

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

  def indexReqStream : Stream[IndexRequest] = Stream.cons(indexReqQueue.take, indexReqStream)

  def start {
    logger.info("Starting indexer")
    indexingFuture = scheduler.submit {
      for (indexReq <- indexReqStream)
        doInLock {
          doIndex(List(indexReq))
        }
    }
    flushFuture = scheduler.scheduleWithFixedDelay(doInLock(flush), 0, 10, TimeUnit.SECONDS)
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

  private def doIndex(indexReqs: List[IndexRequest]) {
    val indexRequests = indexReqs.groupBy { r =>
      (r.server, r.channel, r.botName)
    }

    for (((server, channel, botName), indexRequestBatch) <- indexRequests) {
      val indexDir = getIndexDir(server, channel, botName)
      val indexWriter = getIndexWriter(indexDir)
      for (indexRequest <- indexRequestBatch;
           chatLine     <- indexRequest.chatLines) {
        val tsField = new LongField(ChatLine.TS, chatLine.timestamp, Field.Store.YES)
        val userField = new StringField(ChatLine.USER, chatLine.user, Field.Store.YES)
        val msgField = new TextField(ChatLine.MSG, chatLine.message, Field.Store.YES)
        indexWriter.addDocument(List(tsField, userField, msgField), indexWriter.getAnalyzer)
        logger.debug("Indexed : [{} {} {}] [{}] {}: {}",
            server, channel, botName, chatLine.timestamp.toString, chatLine.user, chatLine.message)
      }
    }
  }

}