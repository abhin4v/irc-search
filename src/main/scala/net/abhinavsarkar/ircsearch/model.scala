package net.abhinavsarkar.ircsearch.model


case class ChatLine(user : String, timestamp : Long, message : String)

case class IndexRequest(
    server : String, channel : String, botName : String, chatLines : List[ChatLine])

case class SearchRequest(
    server : String, channel : String, botName : String, query: String,
    page : Int = 0, pageSize : Int = 10)

case class SearchResult(
    server : String, channel : String, botName : String, query: String,
    page : Int, pageSize : Int, totalResults : Int, chatLines : List[ChatLine])

object SearchResult {
  def fromSearchRequest(searchRequest : SearchRequest) = searchRequest match {
    case SearchRequest(server, channel, botName, query, page, pageSize) =>
      new SearchResult(server, channel, botName, query, page, pageSize, 0, List())
  }
}

