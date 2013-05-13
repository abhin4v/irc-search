package net.abhinavsarkar.ircsearch

import io.netty.channel.ChannelInboundMessageHandlerAdapter
import io.netty.handler.codec.http.HttpRequest
import com.typesafe.scalalogging.slf4j.Logging
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.HttpResponse
import io.netty.channel.ChannelFuture
import io.netty.handler.codec.http.HttpHeaders.isKeepAlive
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders
import io.netty.channel.ChannelFutureListener
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.buffer.Unpooled


trait HttpRequestHandler extends ChannelInboundMessageHandlerAdapter[HttpRequest] with Logging {

  protected def sendDefaultResponse(ctx : ChannelHandlerContext, request : HttpRequest) : HttpResponse = {
    val response = new DefaultHttpResponse(
        HTTP_1_1, if (request.getDecoderResult.isSuccess) OK else BAD_REQUEST)
    writeResponse(ctx, request, response)
    response
  }

  protected def sendNotFound(ctx : ChannelHandlerContext, request : HttpRequest) : HttpResponse = {
    val response = new DefaultHttpResponse(HTTP_1_1, NOT_FOUND)
    writeResponse(ctx, request, response)
    response
  }

  protected def sendSuccess(ctx : ChannelHandlerContext, request : HttpRequest, body : String) : HttpResponse = {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    response.setContent(Unpooled.copiedBuffer(body.getBytes))
    response.setHeader(CONTENT_TYPE, "application/json")
    writeResponse(ctx, request, response)
    response
  }

  protected def writeResponse(
      ctx : ChannelHandlerContext, request : HttpRequest, response : HttpResponse) {
    response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes())

    if (isKeepAlive(request)) {
        response.setHeader(CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
    }

    val future = ctx.write(response)

    if (!isKeepAlive(request) || response.getStatus().getCode() != 200) {
        future.addListener(ChannelFutureListener.CLOSE)
    }
  }

  protected def logRequest(ctx: ChannelHandlerContext, request: HttpRequest, response: HttpResponse) {
    logger.info("{} {} {} {}",
      response.getStatus().getCode() : Integer,
      request.getMethod(),
      request.getUri(),
      ctx.channel().remoteAddress())
  }

  override def exceptionCaught(ctx : ChannelHandlerContext, cause : Throwable) {
    logger.warn("Exception in handling request", cause)
    ctx.close()
  }

}