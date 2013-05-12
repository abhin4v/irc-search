package net.abhinavsarkar.ircsearch

import java.net.InetSocketAddress
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
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.buffer.Unpooled
import java.nio.charset.Charset

object Server extends App with Logging {

  if (args.isEmpty) {
    println("Please specify port to run the server on")
    System.exit(1)
  } else {
    val port = args(0).toInt
    logger.info("Starting server at port {}", port: Integer)

    val httpRequestRouter = new HttpRequestRouter {
      val Echo = "^/echo$".r
      def route = {
        case Echo() => new HttpRequestHandler {
          override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
            val content = request.getContent().toString(Charset.forName("UTF-8"))
            logRequest(ctx, request, sendSuccess(ctx, request, content))
          }
        }
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
          stopServer(server);
        }
      })

    try {
      server.bind().sync.channel.closeFuture.sync
    } catch {
      case e : Exception => {
        logger.error("Exception while running server. Stopping server", e)
        stopServer(server);
      }
    }
  }

  def stopServer(server : ServerBootstrap) {
    logger.info("Stopping server")
    server.shutdown
    logger.info("Stopped server")
  }

}