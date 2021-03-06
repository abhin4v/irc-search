package net.abhinavsarkar.ircsearch

import com.typesafe.scalalogging.slf4j.Logging

import io.netty.buffer.Unpooled
import io.netty.channel.{ ChannelFutureListener, ChannelHandlerContext, ChannelInboundMessageHandlerAdapter }
import io.netty.handler.codec.http.{ DefaultHttpResponse, HttpHeaders, HttpRequest, HttpResponse }
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders.isKeepAlive
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1


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

  private def sendReponse(ctx: ChannelHandlerContext, request: HttpRequest,
      response: HttpResponse, body: String): HttpResponse = {
    response.setContent(Unpooled.copiedBuffer(body.getBytes))
    response.setHeader(CONTENT_TYPE, "application/json")
    writeResponse(ctx, request, response)
    response
  }

  protected def sendSuccess(ctx : ChannelHandlerContext, request : HttpRequest, body : String) = {
    sendReponse(ctx, request, new DefaultHttpResponse(HTTP_1_1, OK), body)
  }

  protected def sendServerError(ctx : ChannelHandlerContext, request : HttpRequest, body : String) = {
    sendReponse(ctx, request, new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR), body)
  }

  protected def sendClientError(ctx : ChannelHandlerContext, request : HttpRequest, body : String) = {
    sendReponse(ctx, request, new DefaultHttpResponse(HTTP_1_1, BAD_REQUEST), body)
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