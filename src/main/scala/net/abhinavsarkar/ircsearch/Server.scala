package net.abhinavsarkar.ircsearch

import java.net.InetSocketAddress
import java.nio.charset.Charset

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.future

import com.typesafe.scalalogging.slf4j.Logging

import au.com.bytecode.opencsv.CSVParser

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ ChannelHandlerContext, ChannelInboundByteHandlerAdapter,
                          ChannelInboundMessageHandlerAdapter, ChannelInitializer }
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.{ NioEventLoopGroup, NioServerSocketChannel }
import io.netty.handler.codec.{ DelimiterBasedFrameDecoder, Delimiters }
import io.netty.handler.codec.http.{ HttpChunkAggregator, HttpContentCompressor, HttpMethod,
                                     HttpRequest, HttpRequestDecoder, HttpResponseEncoder,
                                     QueryStringDecoder }
import io.netty.handler.codec.string.StringDecoder

import net.abhinavsarkar.ircsearch.lucene._
import net.abhinavsarkar.ircsearch.model._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization

object Server extends App with Logging {

  if (args.isEmpty) {
    println("Please specify port to run the server on")
    System.exit(1)
  } else {
    val port = args(0).toInt
    logger.info("Starting server at port {}", port: Integer)

    val server = (new ServerBootstrap)
      .group(new NioEventLoopGroup(1), new NioEventLoopGroup(1))
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        def initChannel(ch: SocketChannel) {
          val p = ch.pipeline
            .addLast("unihandler", UnifiedHandler)
        }})
      .localAddress(new InetSocketAddress(port))

    val cleanup = { () =>
      stopServer(server)
      Indexer.stop
      Searcher.close
    }

    Runtime.getRuntime.addShutdownHook(
      new Thread("ShutdownHook") {
        override def run = cleanup()
      })

    try {
      Indexer.start
      server.bind().sync.channel.closeFuture.sync
    } catch {
      case e : Exception => {
        logger.error("Exception while running server. Stopping server", e)
        cleanup()
      }
    }
  }

  private def stopServer(server : ServerBootstrap) {
    logger.info("Stopping server")
    server.shutdown
    logger.info("Stopped server")
  }

}

@Sharable
private object UnifiedHandler extends ChannelInboundByteHandlerAdapter {

  val httpRequestRouter = new HttpRequestRouter {
    val Echo = "^/echo$".r
    val Index = "^/index$".r
    val Search = "^/search.*".r
    def route = {
      case Echo() => EchoHandler
      case Index() => new IndexHandler
      case Search() => SearchHandler
    }
  }

  override def inboundBufferUpdated(ctx : ChannelHandlerContext, in: ByteBuf) {
    if (in.readableBytes() < 5) {
      return;
    }

    val magic1 = in.getUnsignedByte(in.readerIndex())
    val magic2 = in.getUnsignedByte(in.readerIndex() + 1)
    if (isHttp(magic1, magic2)) {
      ctx.pipeline
        .addLast("decoder", new HttpRequestDecoder)
        .addLast("aggregator", new HttpChunkAggregator(1048576))
        .addLast("encoder", new HttpResponseEncoder)
        .addLast("compressor", new HttpContentCompressor)
        .addLast("router", httpRequestRouter)
        .remove(this)
    } else {
      ctx.pipeline
        .addLast("framedecoder", new DelimiterBasedFrameDecoder(1048576, Delimiters.lineDelimiter() : _*))
        .addLast("decoder", new StringDecoder(Charset.forName("UTF-8")))
        .addLast("csvhandler", new TcpIndexHandler)
        .remove(this)
    }
    ctx.nextInboundByteBuffer.writeBytes(in)
    ctx.fireInboundBufferUpdated
  }

  private def isHttp(magic1: Int, magic2: Int) = {
      magic1 == 'G' && magic2 == 'E' || // GET
      magic1 == 'P' && magic2 == 'O' || // POST
      magic1 == 'P' && magic2 == 'U' || // PUT
      magic1 == 'H' && magic2 == 'E' || // HEAD
      magic1 == 'O' && magic2 == 'P' || // OPTIONS
      magic1 == 'P' && magic2 == 'A' || // PATCH
      magic1 == 'D' && magic2 == 'E' || // DELETE
      magic1 == 'T' && magic2 == 'R' || // TRACE
      magic1 == 'C' && magic2 == 'O'    // CONNECT
  }

}

private class TcpIndexHandler extends ChannelInboundMessageHandlerAdapter[String] {
  var server: String = null
  var channel : String = null
  var botName : String = null
  var inited = false
  val parser = new CSVParser

  override def messageReceived(ctx: ChannelHandlerContext, content : String) {
    val values = parser.parseLine(content)
    if (!inited) {
      assert(values.length == 3, "Server, channel and botName should be provided first")
      server = values(0)
      channel = values(1)
      botName = values(2)
      inited = true
    } else {
      Indexer.index(IndexRequest(server, channel, botName,
          List(ChatLine(values(0), values(1).toLong, values(2)))))
    }
  }
}

@Sharable
private object EchoHandler extends HttpRequestHandler {
  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    val content = request.getContent().toString(Charset.forName("UTF-8"))
    logRequest(ctx, request, sendSuccess(ctx, request, content))
  }
}

@Sharable
private class IndexHandler extends HttpRequestHandler {
  implicit val formats = DefaultFormats
  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    future {
      val content = request.getContent().toString(Charset.forName("UTF-8"))
      val indexRequest = Serialization.read[IndexRequest](content)
      Indexer.index(indexRequest)
    }
    logRequest(ctx, request, sendDefaultResponse(ctx, request))
  }
}

@Sharable
private object SearchHandler extends HttpRequestHandler {
  implicit val formats = DefaultFormats
  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    val f = future {
      val method = request.getMethod()
      val searchRequest = if (HttpMethod.POST.equals(method)) {
        val content = request.getContent().toString(Charset.forName("UTF-8"))
        Serialization.read[SearchRequest](content)
      } else if (HttpMethod.GET.equals(method)) {
        val params = new QueryStringDecoder(request.getUri).getParameters.toMap
        val List(server, channel, botName, query) =
          List("server", "channel", "botName", "query").map(params(_).get(0))
        val List(page, pageSize, details) =
          List("page", "pageSize", "details").map(params.get(_).map({ case l => l.get(0) }))

        var sr = SearchRequest(server, channel, botName, query)
        if (page.isDefined)
          sr = sr.copy(page = page.get.toInt)
        if (pageSize.isDefined)
          sr = sr.copy(pageSize = pageSize.get.toInt)
        if (details.isDefined)
          sr = sr.copy(details = details.get.toBoolean)
        sr
      } else {
        throw new UnsupportedOperationException("HTTP method " + method + " is not supported")
      }

      (searchRequest, Searcher.search(searchRequest))
    }
    f onSuccess {
      case (searchRequest, searchResult) =>
        logRequest(ctx, request, sendSuccess(ctx, request,
          Serialization.write(
            if (searchRequest.details) searchResult else searchResult.toSimpleSearchResult)))
    }
    f onFailure {
      case e => {
        logger.error("Error", e)
        val body = Serialization.write(SearchError(e.getMessage))
        e match {
          case e : NoSuchElementException => {
            logRequest(ctx, request, sendClientError(ctx, request, body))
          }
          case _ => {
            logRequest(ctx, request, sendServerError(ctx, request, body))
          }
        }
      }
    }
  }
}
