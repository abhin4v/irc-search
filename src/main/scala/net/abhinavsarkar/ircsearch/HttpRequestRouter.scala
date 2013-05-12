package net.abhinavsarkar.ircsearch

import io.netty.channel.ChannelHandler.Sharable
import java.util.regex.Pattern
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.HttpRequest

@Sharable
abstract class HttpRequestRouter extends HttpRequestHandler {

  def route : PartialFunction[String, HttpRequestHandler]

  override def messageReceived(ctx: ChannelHandlerContext, request: HttpRequest) {
    if (request.getDecoderResult.isSuccess) {
      val uri = request.getUri
      if (route.isDefinedAt(uri)) {
        val routeHandler = route.apply(uri)
        ctx.pipeline.addLast("handler", routeHandler)
        try {
            ctx.nextInboundMessageBuffer.add(request)
            ctx.fireInboundBufferUpdated
        } finally {
            ctx.pipeline.remove("handler")
        }
      } else {
        logRequest(ctx, request, sendNotFound(ctx, request))
      }
    } else {
      logger.warn("Could not decode request: {}", request)
      logRequest(ctx, request, sendDefaultResponse(ctx, request))
    }
  }

}