package net.abhinavsarkar.ircsearch.lucene

import java.io.File
import java.text.ParseException
import java.text.SimpleDateFormat

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.Buffer

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.queries.ChainedFilter
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Filter
import org.apache.lucene.search.FilteredQuery
import org.apache.lucene.search.NumericRangeFilter
import org.apache.lucene.search.Query
import org.apache.lucene.search.QueryWrapperFilter
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.SearcherManager
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.FSDirectory

import com.typesafe.scalalogging.slf4j.Logging

import net.abhinavsarkar.ircsearch.model._

object Searcher extends Logging {

  val MaxHits = 1000
  val MessageFieldBoost = java.lang.Float.valueOf(2.0f)

  private val searcherMgrs = mutable.Map[String, SearcherManager]()

  def close {
    for (searcherMgr <- searcherMgrs.values)
      searcherMgr.close
    logger.info("Closed Searcher")
  }

  private def getSearcherMgr(dirPath : String) : SearcherManager = {
    synchronized {
      if (!(searcherMgrs contains dirPath)) {
        val indexDir = new File(dirPath)
        assert(indexDir.exists && indexDir.isDirectory)

        val dir = FSDirectory.open(indexDir)
        searcherMgrs += (dirPath -> new SearcherManager(dir, new SearcherFactory))

      }
    }

    searcherMgrs(dirPath)
  }

  private def mkQueryParser(analyzer : Analyzer) =
    new MultiFieldQueryParser(Indexer.LUCENE_VERSION,
        List(ChatLine.MSG, ChatLine.CTXB, ChatLine.CTXA).toArray, analyzer,
        Map(ChatLine.MSG -> MessageFieldBoost))

  private def filterifyQuery(query : Query) : Query =
    query match {
      case boolQuery: BooleanQuery => {
        val newQuery = new BooleanQuery
        val filters = Buffer[Filter]()
        for (clause <- boolQuery.getClauses) {
          val subQuery = clause.getQuery
          if (subQuery.isInstanceOf[TermQuery]) {
            val termQuery = subQuery.asInstanceOf[TermQuery]
            val field = termQuery.getTerm.field
            val sdf = new SimpleDateFormat("yyMMdd")
            field match {
              case ChatLine.USER => {
                val filterQuery = new BooleanQuery
                clause.setOccur(BooleanClause.Occur.MUST)
                filterQuery.add(clause)
                filters += new QueryWrapperFilter(filterQuery)
              }
              case "before" => {
                  try {
                    val ts = sdf.parse(termQuery.getTerm.text).getTime
                    filters += NumericRangeFilter.newLongRange(
                        ChatLine.TS, 0, ts, true, true)
                  } catch {
                    case e : ParseException => {}
                  }
              }
              case "after" => {
                try {
                  val ts = sdf.parse(termQuery.getTerm.text).getTime
                  filters += NumericRangeFilter.newLongRange(
                      ChatLine.TS, ts, java.lang.Long.MAX_VALUE, true, true)
                } catch {
                  case e : ParseException => {}
                }
              }
              case _ => newQuery.add(clause)
            }
          } else {
            newQuery.add(clause)
          }
        }

        if (filters.isEmpty)
          newQuery
        else
          new FilteredQuery(newQuery, new ChainedFilter(filters.toArray, ChainedFilter.AND))
      }
      case _ => query
    }

  def search(searchRequest : SearchRequest) : SearchResult = {
    logger.debug("Searching : [{} {} {}] {}",
      searchRequest.server, searchRequest.channel, searchRequest.botName, searchRequest.query)

    val indexDir = Indexer.getIndexDir(searchRequest.server, searchRequest.channel, searchRequest.botName)
    val analyzer = Indexer.mkAnalyzer
    try {
      val queryParser = mkQueryParser(analyzer)
      val query = filterifyQuery(queryParser.parse(searchRequest.query))
      logger.debug("Query: {}", query)
      val (totalResults, results) = doSearch(indexDir, query, searchRequest.page, searchRequest.pageSize)
      val searchResults = SearchResult.fromSearchRequest(searchRequest)
        .copy(totalResults = totalResults, chatLines = results.map(_._1))
      logger.debug("Search results: {}", searchResults)
      searchResults
    } finally {
      analyzer.close
    }
  }

  private val DocFields = List(ChatLine.USER, ChatLine.TS, ChatLine.MSG, ChatLine.CTXB, ChatLine.CTXA)

  private def doSearch(indexDir : String, query : Query, page : Int, pageSize : Int)
    : (Int, List[(ChatLine, Float)]) = {
    val searcherMgr = getSearcherMgr(indexDir)
    searcherMgr.maybeRefresh
    val indexSearcher = searcherMgr.acquire()
    try {
      val topDocs = indexSearcher.search(query, MaxHits.min((page + 1) * pageSize),
          new Sort(SortField.FIELD_SCORE, new SortField(ChatLine.TS, SortField.Type.LONG, true)))
      val docs = topDocs.scoreDocs
      .drop(page * pageSize)
      .map { sd =>
        val score = sd.score
        val doc = indexSearcher.doc(sd.doc).getFields.foldLeft(mutable.Map[String, String]()) {
          (map, field) => map += (field.name -> field.stringValue)
        }

        val List(user, timestamp, message, contextBefore, contextAfter) = DocFields.map(doc)

        val LineRe = "(\\d+) (.*?): (.*)".r
        val List(ctxBefore, ctxAfter) = List(contextBefore, contextAfter).map {
          _.split('\n').filterNot(_.isEmpty).map {
            case LineRe(timestamp, user, message) => new ChatLine(user, timestamp.toLong, message)
          }}

        val chatLine = new ChatLine(user, timestamp.toLong, message, ctxBefore.toList, ctxAfter.toList)
        (chatLine, score)
      }
      (topDocs.totalHits, docs.toList)
    } finally {
      searcherMgr.release(indexSearcher)
    }
  }

}