package org.broadinstitute.dsde.rawls.google

import akka.http.scaladsl.model.StatusCode
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.HttpResponseException
import com.google.api.client.http.json.JsonHttpContent
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.Histogram
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented.GoogleCounters
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, InstrumentedRetry}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, JsonSupport, WorkspaceJsonSupport}
import spray.json.{JsValue, RootJsonFormat}

import java.io.{ByteArrayOutputStream, IOException, InputStream}
import java.util.concurrent.TimeUnit
import scala.concurrent._
import scala.util.{Failure, Success, Try}

/**
 * Created by mbemis on 5/10/16.
 */
trait GoogleUtilities extends LazyLogging with InstrumentedRetry with GoogleInstrumented {
  implicit val executionContext: ExecutionContext

  protected def when500or400orGoogleError(throwable: Throwable): Boolean =
    throwable match {
      case t: HttpResponseException => t.getStatusCode / 100 == 5 || t.getStatusCode / 100 == 4
      case ioe: IOException         => true
      case _                        => false
    }

  protected def when500orGoogleError(throwable: Throwable): Boolean =
    throwable match {
      case t: HttpResponseException => t.getStatusCode / 100 == 5
      case ioe: IOException         => true
      case _                        => false
    }

  // Sadly when500or400orGoogleError retries on 404s which is not always the behaviour we want. 404s
  // mean different things in different contexts. Sometimes they mean the resource does not
  // exist/you can't access it; sometimes it means it does not exist *yet* and sometimes something
  // else. As such, it should have been handled explicitly depending on the context and not rolled
  // into a default retrying handler. This ship has sailed - it's used everywhere and changing it
  // might cause a bunch of failures.
  def when500orNon404GoogleError(throwable: Throwable): Boolean = throwable match {
    case e: HttpResponseException if e.getStatusCode == 404 => false
    case t                                                  => when500or400orGoogleError(t)
  }

  // Predicate to exclude certain Google status codes from retries
  def whenGoogleStatusDoesntContain(throwable: Throwable, excludedStatusCodes: Set[StatusCode]): Boolean =
    throwable match {
      case e: HttpResponseException if excludedStatusCodes.contains(StatusCode.int2StatusCode(e.getStatusCode)) => false
      case t                                                                                                    => true
    }

  protected def retryWhen500orGoogleError[T](
    op: () => T
  )(implicit histo: Histogram, executionContext: ExecutionContext): Future[T] =
    retryExponentially(when500or400orGoogleError)(() => Future(blocking(op())))

  protected def retryWithRecoverWhen500orGoogleError[T](op: () => T)(recover: PartialFunction[Throwable, T])(implicit
    histo: Histogram
  ): Future[T] =
    retryExponentially(when500or400orGoogleError)(() => Future(blocking(op())).recover(recover))

  protected def executeGoogleRequest[T](request: AbstractGoogleClientRequest[T], logRequest: Boolean = true)(implicit
    counters: GoogleCounters
  ): T =
    executeGoogleCall(request, logRequest) { response =>
      response.parseAs(request.getResponseClass)
    }

  protected def executeGoogleRequestWithRetry[T](
    request: AbstractGoogleClientRequest[T]
  )(implicit counters: GoogleCounters, histo: Histogram, executionContext: ExecutionContext): Future[T] =
    retryWhen500orGoogleError(() => executeGoogleRequest(request))

  protected def executeGoogleFetch[A, B](
    request: AbstractGoogleClientRequest[A]
  )(f: (InputStream) => B)(implicit counters: GoogleCounters): B =
    executeGoogleCall(request) { response =>
      val stream = response.getContent
      try
        f(stream)
      finally
        stream.close()
    }

  protected def executeGoogleCall[A, B](
    request: AbstractGoogleClientRequest[A],
    logRequest: Boolean = true
  )(processResponse: (com.google.api.client.http.HttpResponse) => B)(implicit counters: GoogleCounters): B = {
    val start = System.currentTimeMillis()
    Try {
      request.executeUnparsed()
    } match {
      case Success(response) =>
        if (logRequest) logGoogleRequest(request, start, response)
        instrumentGoogleRequest(request, start, Right(response))
        try
          processResponse(response)
        finally
          response.disconnect()
      case Failure(httpRegrets: HttpResponseException) =>
        if (logRequest) logGoogleRequest(request, start, httpRegrets)
        instrumentGoogleRequest(request, start, Left(httpRegrets))
        throw httpRegrets
      case Failure(regrets) =>
        if (logRequest) logGoogleRequest(request, start, regrets)
        instrumentGoogleRequest(request, start, Left(regrets))
        throw regrets
    }
  }

  private def logGoogleRequest[A](request: AbstractGoogleClientRequest[A],
                                  startTime: Long,
                                  response: com.google.api.client.http.HttpResponse
  ): Unit =
    logGoogleRequest(request, startTime, Option(response.getStatusCode), None)

  private def logGoogleRequest[A](request: AbstractGoogleClientRequest[A], startTime: Long, regrets: Throwable): Unit =
    regrets match {
      case e: HttpResponseException => logGoogleRequest(request, startTime, Option(e.getStatusCode), None)
      case t: Throwable             => logGoogleRequest(request, startTime, None, Option(ErrorReport(t)))
    }

  private def logGoogleRequest[A](request: AbstractGoogleClientRequest[A],
                                  startTime: Long,
                                  statusCode: Option[Int],
                                  errorReport: Option[ErrorReport]
  ): Unit = {
    import GoogleRequestJsonSupport._
    import spray.json._

    val payload =
      if (logger.underlying.isDebugEnabled) {
        Option(request.getHttpContent) match {
          case Some(content: JsonHttpContent) =>
            Try {
              val outputStream = new ByteArrayOutputStream()
              content.writeTo(outputStream)
              outputStream.toString.parseJson
            }.toOption
          case _ => None
        }
      } else {
        None
      }

    logger.debug(
      GoogleRequest(request.getRequestMethod,
                    request.buildHttpRequestUrl().toString,
                    payload,
                    System.currentTimeMillis() - startTime,
                    statusCode,
                    errorReport
      ).toJson(GoogleRequestFormat).compactPrint
    )
  }

  private def instrumentGoogleRequest[A](request: AbstractGoogleClientRequest[A],
                                         startTime: Long,
                                         responseOrException: Either[Throwable, com.google.api.client.http.HttpResponse]
  )(implicit counters: GoogleCounters): Unit = {
    val (counter, timer) = counters(request, responseOrException)
    counter += 1
    timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
  }
}

protected[google] case class GoogleRequest(method: String,
                                           url: String,
                                           payload: Option[JsValue],
                                           time_ms: Long,
                                           statusCode: Option[Int],
                                           errorReport: Option[ErrorReport]
)
protected[google] object GoogleRequestJsonSupport extends JsonSupport {
  import WorkspaceJsonSupport.ErrorReportFormat
  import spray.json.DefaultJsonProtocol._
  implicit val GoogleRequestFormat: RootJsonFormat[GoogleRequest] = jsonFormat6(GoogleRequest)
}
