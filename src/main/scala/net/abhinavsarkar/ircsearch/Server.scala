package net.abhinavsarkar.ircsearch

import java.net.InetSocketAddress
import java.nio.charset.Charset

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.future

import com.typesafe.scalalogging.slf4j.Logging

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpChunkAggregator
import io.netty.handler.codec.http.HttpContentCompressor
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpRequestDecoder
import io.netty.handler.codec.http.HttpResponseEncoder
import net.abhinavsarkar.ircsearch.lucene.Indexer
import net.abhinavsarkar.ircsearch.lucene.Searcher
import net.abhinavsarkar.ircsearch.model.IndexRequest
import net.abhinavsarkar.ircsearch.model.SearchRequest
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization

object Server extends App with Logging {

  if (args.isEmpty) {
    println("Please specify port to run the server on")
    System.exit(1)
  } else {
    val port = args(0).toInt
    logger.info("Starting server at port {}", port: Integer)

    val httpRequestRouter = new HttpRequestRouter {
      val Echo = "^/echo$".r
      val Index = "^/index$".r
      val Search = "^/search$".r
      def route = {
        case Echo() => EchoHandler
        case Index() => IndexHandler
        case Search() => SearchHandler
      }
    }

    val server = (new ServerBootstrap)
      .group(new NioEventLoopGroup(1), new NioEventLoopGroup(1))
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        def initChannel(ch: SocketChannel) {
          val p = ch.pipeline
            .addLast("decoder", new HttpRequestDecoder)
            .addLast("aggregator", new HttpChunkAggregator(1048576))
            .addLast("encoder", new HttpResponseEncoder)
            .addLast("compressor", new HttpContentCompressor)
            .addLast("router", httpRequestRouter)
        }})
      .localAddress(new InetSocketAddress(port))

    Runtime.getRuntime.addShutdownHook(
      new Thread("ShutdownHook") {
        override def run {
          stopServer(server)
          IndexHandler.stop
        }
      })

    try {
      server.bind().sync.channel.closeFuture.sync
    } catch {
      case e : Exception => {
        logger.error("Exception while running server. Stopping server", e)
        stopServer(server)
        IndexHandler.stop
      }
    }
  }

  def stopServer(server : ServerBootstrap) {
    logger.info("Stopping server")
    server.shutdown
    logger.info("Stopped server")
  }

}

@Sharable
object EchoHandler extends HttpRequestHandler {
  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    val content = request.getContent().toString(Charset.forName("UTF-8"))
    logRequest(ctx, request, sendSuccess(ctx, request, content))
  }
}

@Sharable
object IndexHandler extends HttpRequestHandler {
  implicit val formats = DefaultFormats
  lazy val indexer = { val indexer = new Indexer; indexer.start; indexer }
  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    future {
      val content = request.getContent().toString(Charset.forName("UTF-8"))
      val indexRequest = Serialization.read[IndexRequest](content)
      indexer.index(indexRequest)
    }
    logRequest(ctx, request, sendDefaultResponse(ctx, request))
  }
  def stop = indexer.stop
}

@Sharable
object SearchHandler extends HttpRequestHandler {
  implicit val formats = DefaultFormats
  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    val content = request.getContent().toString(Charset.forName("UTF-8"))
    val searchRequest = Serialization.read[SearchRequest](content)
    val searchResult = Searcher.search(searchRequest)
    logRequest(ctx, request, sendSuccess(ctx, request, Serialization.write(searchResult)))
  }
}
