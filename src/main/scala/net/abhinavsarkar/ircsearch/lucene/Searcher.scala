package net.abhinavsarkar.ircsearch.lucene

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.FilteredQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.search.QueryWrapperFilter
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.FSDirectory

import com.typesafe.scalalogging.slf4j.Logging

import net.abhinavsarkar.ircsearch.model._

object Searcher extends Logging {

  private def mkIndexSearcher(dirPath : String) : IndexSearcher = {
    val indexDir = new File(dirPath)
    assert(indexDir.exists && indexDir.isDirectory)

    new IndexSearcher(IndexReader.open(FSDirectory.open(indexDir)))
  }

  private def mkQueryParser(analyzer : Analyzer) =
    new QueryParser(Indexer.LUCENE_VERSION, ChatLine.MSG, analyzer)

  private def filterifyQuery(query : Query, mustFields : Set[String]) : Query =
    query match {
      case boolQuery: BooleanQuery => {
        val newQuery = new BooleanQuery
        val filterQuery = new BooleanQuery
        for (clause <- boolQuery.getClauses) {
          val subQuery = clause.getQuery
          if (subQuery.isInstanceOf[TermQuery]) {
            val termQuery = subQuery.asInstanceOf[TermQuery]
            val field = termQuery.getTerm.field
            if (mustFields contains field) {
              clause.setOccur(BooleanClause.Occur.MUST)
              filterQuery.add(clause)
            } else {
              newQuery.add(clause)
            }
          } else {
            newQuery.add(clause)
          }
        }

        if (filterQuery.clauses.isEmpty)
          newQuery
        else
          new FilteredQuery(newQuery, new QueryWrapperFilter(filterQuery))
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
      val query = filterifyQuery(queryParser.parse(searchRequest.query), Set(ChatLine.USER))
      logger.debug("Query: {}", query)
      val (totalResults, results) = doSearch(indexDir, query, searchRequest.pageSize)
      val searchResults = SearchResult.fromSearchRequest(searchRequest)
        .copy(totalResults = totalResults, chatLines = results.map(_._1))
      logger.debug("Search results: {}", searchResults)
      searchResults
    } finally {
      analyzer.close
    }
  }

  private def doSearch(indexDir : String, query : Query, maxHits : Int)
    : (Int, List[(ChatLine, Float)]) = {
    val indexSearcher = mkIndexSearcher(indexDir)
    val topDocs = indexSearcher.search(query, maxHits,
        new Sort(SortField.FIELD_SCORE, new SortField(ChatLine.TS, SortField.Type.LONG, true)))
    val docs = topDocs.scoreDocs.map { sd =>
      val score = sd.score
      val doc = indexSearcher.doc(sd.doc).getFields.foldLeft(mutable.Map[String, String]()) {
        (map, field) => map += (field.name -> field.stringValue)
      }

      val chatLine = new ChatLine(doc(ChatLine.USER), doc(ChatLine.TS).toLong, doc(ChatLine.MSG))
      (chatLine, score)
    }
    (topDocs.totalHits, docs.toList)
  }

}