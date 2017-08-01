package org.broadinstitute.dsde.rawls.util

import java.util.concurrent.TimeUnit

import akka.actor.ActorRefFactory
import akka.util.Timeout
import nl.grons.metrics.scala.{Counter, Timer}
import spray.client.pipelining._
import spray.http.HttpEncodings.gzip
import spray.http.HttpHeaders.`Accept-Encoding`
import spray.http.{HttpRequest, HttpResponse}
import spray.httpx.encoding.Gzip

import scala.concurrent.ExecutionContext
import scala.util.Success

/**
  * Created by rtitle on 7/7/17.
  */
object SprayClientUtils {
  /**
    * sendReceive, but with gzip.
    * Two versions, one of which overrides the timeout.
    */
  def gzSendReceive(timeout: Timeout)(implicit actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive = {
    implicit val to = timeout //make the explicit implicit, a process known as "tactfulization"
    addHeaders(`Accept-Encoding`(gzip)) ~> sendReceive ~> decode(Gzip)
  }

  def gzSendReceive(implicit actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive =
    addHeaders(`Accept-Encoding`(gzip)) ~> sendReceive ~> decode(Gzip)

  /**
    * Instruments the provided sendReceive by updating a given request counter and timer.
    */
  def instrument(sendReceive: SendReceive, retryCount: Int = 0)(implicit requestCounter: (HttpRequest, HttpResponse, Boolean) => Counter, requestTimer: (HttpRequest, HttpResponse) => Timer, actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive =
    request => (sendReceive andThen { future =>
      val start = System.currentTimeMillis()
      future.andThen { case Success(response) =>
        requestCounter(request, response, retryCount != 0) += 1
        requestTimer(request, response).update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
      }
    })(request)

  /**
    * Instruments the default sendReceive by updating a given request counter and timer.
    */
  def instrumentedSendReceive(retryCount: Int = 0)(implicit requestCounter: (HttpRequest, HttpResponse, Boolean) => Counter, requestTimer: (HttpRequest, HttpResponse) => Timer, actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive =
    instrument(sendReceive, retryCount)

  /**
    * Instruments [[gzSendReceive]] by updating a given request counter and timer.
    */
  def instrumentedGzSendReceive(retryCount: Int = 0)(implicit requestCounter: (HttpRequest, HttpResponse, Boolean) => Counter, requestTimer: (HttpRequest, HttpResponse) => Timer, actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive =
    instrument(gzSendReceive, retryCount)

  /**
    * Instruments [[gzSendReceive(timeout)]] by updating a given request counter and timer.
    */
  def instrumentedGzSendReceive(timeout: Timeout, retryCount: Int)(implicit requestCounter: (HttpRequest, HttpResponse, Boolean) => Counter, requestTimer: (HttpRequest, HttpResponse) => Timer, actorRefFactory: ActorRefFactory, executionContext: ExecutionContext): SendReceive =
    instrument(gzSendReceive(timeout), retryCount)
}