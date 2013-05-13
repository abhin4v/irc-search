package net.abhinavsarkar.ircsearch.lucene

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.lucene.search.IndexSearcher
import java.io.File
import org.apache.lucene.index.IndexReader
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import scala.collection.immutable.Set
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.QueryWrapperFilter
import org.apache.lucene.search.Filter
import net.abhinavsarkar.ircsearch.model.SearchRequest
import net.abhinavsarkar.ircsearch.model.SearchResult
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import scala.collection.JavaConversions._
import scala.collection.mutable
import net.abhinavsarkar.ircsearch.model.ChatLine
import net.abhinavsarkar.ircsearch.model.ChatLine
import net.abhinavsarkar.ircsearch.model.SearchResult
import net.abhinavsarkar.ircsearch.model.SearchResult

object Searcher extends Logging {

  private def mkIndexSearcher(dirPath : String) : IndexSearcher = {
    val indexDir = new File(dirPath)
    assert(indexDir.exists && indexDir.isDirectory)

    new IndexSearcher(IndexReader.open(FSDirectory.open(indexDir)))
  }

  private def mkQueryParser(analyzer : Analyzer) =
    new QueryParser(Indexer.LUCENE_VERSION, "message", analyzer)

  private def filterifyQuery(query : Query, mustFields : Set[String]) : (Query, Option[Filter]) =
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

        (newQuery, if (filterQuery.clauses.isEmpty) None else Some(new QueryWrapperFilter(filterQuery)))
      }
      case _ => (query, None)
    }

  def search(searchRequest : SearchRequest) : SearchResult = {
    logger.debug("Searching : [{} {} {}] {}",
      searchRequest.server, searchRequest.channel, searchRequest.botName, searchRequest.query)

    val indexDir = Indexer.getIndexDir(searchRequest.server, searchRequest.channel, searchRequest.botName)
    val analyzer = Indexer.mkAnalyzer
    try {
      val queryParser = mkQueryParser(analyzer)
      val (query, filter) = filterifyQuery(queryParser.parse(searchRequest.query), Set("user"))
      logger.debug("Query: {}, Filter: {}", query, filter)
      val (totalResults, results) = doSearch(indexDir, query, filter, searchRequest.pageSize)
      val searchResults = SearchResult.fromSearchRequest(searchRequest)
        .copy(totalResults = totalResults, chatLines = results.map(_._1))
      logger.debug("Search results: {}", searchResults)
      searchResults
    } finally {
      analyzer.close
    }
  }

  private def doSearch(indexDir : String, query : Query, filter : Option[Filter], maxHits : Int)
    : (Int, List[(ChatLine, Float)]) = {
    val indexSearcher = mkIndexSearcher(indexDir)
    val topDocs = indexSearcher.search(query, filter.orNull, maxHits,
        new Sort(SortField.FIELD_SCORE, new SortField("timestamp", SortField.Type.LONG, true)))
    val docs = topDocs.scoreDocs.map { sd =>
      val score = sd.score
      val doc = indexSearcher.doc(sd.doc).getFields.foldLeft(mutable.Map[String, String]()) {
        (map, field) => map += (field.name -> field.stringValue)
      }

      val chatLine = new ChatLine(doc("user"), doc("timestamp").toLong, doc("message"))
      (chatLine, score)
    }
    (topDocs.totalHits, docs.toList)
  }

}