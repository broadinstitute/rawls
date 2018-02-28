package org.broadinstitute.dsde.rawls.util

import java.util.concurrent.TimeUnit

import akka.actor.ActorRefFactory
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpEncodings, `Accept-Encoding`}
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.{Counter, Timer}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.ErrorReport

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
  * Created by rtitle on 7/7/17.
  */
trait HttpClientUtils extends LazyLogging {
  implicit val materializer: Materializer
  implicit val executionContext: ExecutionContext

  def addHeader(httpRequest: HttpRequest, header: HttpHeader) = {
    httpRequest.copy(headers = httpRequest.headers :+ header)
  }

  def executeRequest(http: HttpExt, httpRequest: HttpRequest): Future[HttpResponse] = http.singleRequest(httpRequest)

  def executeRequestUnmarshalResponse[T](http: HttpExt, httpRequest: HttpRequest)(implicit unmarshaller: Unmarshaller[ResponseEntity, T]): Future[T] = {
    executeRequest(http, httpRequest) recover { case t: Throwable =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"http call failed: ${httpRequest.uri}: ${t.getMessage}", t))
    } flatMap { response =>
      if (response.status.isSuccess) {
        Unmarshal(response.entity).to[T]
      } else {
        Unmarshal(response.entity).to[String] map { entityAsString =>
          logger.debug(s"http error status ${response.status} calling uri ${httpRequest.uri}, response: ${entityAsString}")
          throw new RawlsExceptionWithErrorReport(ErrorReport(response.status, s"http error calling uri ${httpRequest.uri}"))
        }
      }
    }
  }
}

case class HttpClientUtilsStandard(implicit val materializer: Materializer, val executionContext: ExecutionContext) extends HttpClientUtils

case class HttpClientUtilsGzip(implicit val materializer: Materializer, val executionContext: ExecutionContext) extends HttpClientUtils {
  override def executeRequest(http: HttpExt, httpRequest: HttpRequest): Future[HttpResponse] = {
    http.singleRequest(addHeader(httpRequest, `Accept-Encoding`(HttpEncodings.gzip))).map { response =>
      Gzip.decodeMessage(response)
    }
  }
}

case class HttpClientUtilsInstrumented(implicit val materializer: Materializer, requestCounter: (HttpRequest, HttpResponse) => Counter, requestTimer: (HttpRequest, HttpResponse) => Timer, actorRefFactory: ActorRefFactory, val executionContext: ExecutionContext) extends HttpClientUtils {
  override def executeRequest(http: HttpExt, httpRequest: HttpRequest): Future[HttpResponse] = {
    val start = System.currentTimeMillis()
    http.singleRequest(httpRequest) andThen {
      case Success(httpResponse) =>
        requestCounter(httpRequest, httpResponse) += 1
        requestTimer(httpRequest, httpResponse).update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
    }
  }
}

case class HttpClientUtilsGzipInstrumented(implicit val materializer: Materializer, requestCounter: (HttpRequest, HttpResponse) => Counter, requestTimer: (HttpRequest, HttpResponse) => Timer, actorRefFactory: ActorRefFactory, val executionContext: ExecutionContext) extends HttpClientUtils {
  override def executeRequest(http: HttpExt, httpRequest: HttpRequest): Future[HttpResponse] = {
    val start = System.currentTimeMillis()
    http.singleRequest(addHeader(httpRequest, `Accept-Encoding`(HttpEncodings.gzip))) andThen {
      case Success(httpResponse) =>
        requestCounter(httpRequest, httpResponse) += 1
        requestTimer(httpRequest, httpResponse).update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
    } map { response =>
      Gzip.decodeMessage(response)
    }
  }
}
