package net.abhinavsarkar.ircsearch.lucene

import java.io.File
import java.util.ArrayList
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConversions._

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

class Indexer extends Logging {

  import Indexer._

  private val indexQueue = new LinkedBlockingQueue[IndexRequest]
  private val scheduler = Executors.newSingleThreadScheduledExecutor
  private val runLock = new ReentrantLock
  private var runFuture : Future[_] = null

  def index(indexRequest : IndexRequest) = indexQueue.offer(indexRequest)

  def start {
    logger.info("Starting indexer")
    runFuture = scheduler.scheduleWithFixedDelay(
      new Runnable {
        def run {
          try {
            runLock.lock
            if (indexQueue.isEmpty)
              return

            val indexReqs = new ArrayList[IndexRequest]
            indexQueue.drainTo(indexReqs)
            doIndex(indexReqs.toList)
          } catch {
            case e : Throwable => logger.error("Exception while running indexer", e)
          } finally {
            runLock.unlock
          }
        }},
      0, 1, TimeUnit.SECONDS)
  }

  def stop {
    try {
      runLock.lock
      if (runFuture != null) {
        runFuture.cancel(false)
        runFuture = null
      }
      logger.info("Stopped indexer")
    } finally {
      runLock.unlock
    }
  }

  private def doIndex(indexReqs: List[IndexRequest]) {
    val indexRequests = indexReqs.groupBy { r =>
      (r.server, r.channel, r.botName)
    }

    for (((server, channel, botName), indexRequestBatch) <- indexRequests) {
      val indexDir = getIndexDir(server, channel, botName)
      val analyzer = mkAnalyzer
      val indexWriter = mkIndexWriter(indexDir, analyzer)
      try {
        for (indexRequest <- indexRequestBatch;
             chatLine     <- indexRequest.chatLines) {
          val tsField = new LongField(ChatLine.TS, chatLine.timestamp, Field.Store.YES)
          val userField = new StringField(ChatLine.USER, chatLine.user, Field.Store.YES)
          val msgField = new TextField(ChatLine.MSG, chatLine.message, Field.Store.YES)
          indexWriter.addDocument(List(tsField, userField, msgField), analyzer)
          logger.debug("Indexed : [{} {} {}] [{}] {}: {}",
              server, channel, botName, chatLine.timestamp.toString, chatLine.user, chatLine.message)
        }
      } finally {
        indexWriter.close
        analyzer.close
      }
    }
  }

}

object Indexer {

  val LUCENE_VERSION = Version.LUCENE_43

  def mkAnalyzer : Analyzer = {
    val defAnalyzer = new StandardAnalyzer(LUCENE_VERSION)
    val fieldAnalyzers = Map(
        ChatLine.USER -> new KeywordAnalyzer,
        ChatLine.MSG -> new EnglishAnalyzer(LUCENE_VERSION))

    new PerFieldAnalyzerWrapper(defAnalyzer, fieldAnalyzers)
  }

  private def mkIndexWriter(dirPath : String, analyzer : Analyzer) : IndexWriter = {
    val indexDir = new File(dirPath)
    if (indexDir.exists) {
      assert(indexDir.isDirectory)
    }
    new IndexWriter(FSDirectory.open(indexDir), new IndexWriterConfig(LUCENE_VERSION, analyzer))
  }

  def getIndexDir(server : String, channel : String, botName : String) : String =
    s"index-$server-$channel-$botName"

}