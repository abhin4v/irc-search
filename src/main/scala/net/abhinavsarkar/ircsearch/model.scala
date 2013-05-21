package net.abhinavsarkar.ircsearch.model


object ChatLine {
  val USER = "user"
  val TS = "ts"
  val MSG = "msg"
  val CTXB = "ctxb"
  val CTXA = "ctxa"
}

case class ChatLine(user : String, timestamp : Long, message : String,
    contextBefore : List[ChatLine] = List(),
    contextAfter : List[ChatLine] = List())

case class IndexRequest(
    server : String, channel : String, botName : String, chatLines : List[ChatLine])

case class SearchRequest(
    server : String, channel : String, botName : String, query: String,
    page : Int = 0, pageSize : Int = 10, details : Boolean = false)

case class SearchResult(
    server : String, channel : String, botName : String, query: String,
    page : Int, pageSize : Int, totalResults : Int, chatLines : List[ChatLine]) {
  def toSimpleSearchResult =
    SimpleSearchResult(server, channel, botName, query, page, pageSize, totalResults,
      chatLines map {
        case mline@ChatLine(_, _, _, contextBefore, contextAfter) =>
          ((contextBefore :+ mline) ++ contextAfter) map { line =>
            List(line.timestamp.toString, line.user, line.message)
          }
      })
}

object SearchResult {
  def fromSearchRequest(searchRequest : SearchRequest) = searchRequest match {
    case SearchRequest(server, channel, botName, query, page, pageSize, _) =>
      new SearchResult(server, channel, botName, query, page, pageSize, 0, List())
  }
}

case class SimpleSearchResult(
    server : String, channel : String, botName : String, query: String,
    page : Int, pageSize : Int, totalResults : Int, lines : List[List[List[String]]])

case class SearchError(error : String)