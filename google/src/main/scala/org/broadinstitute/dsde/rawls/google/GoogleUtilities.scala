package org.broadinstitute.dsde.rawls.google

import java.io.{ByteArrayOutputStream, IOException, InputStream}
import java.util.concurrent.TimeUnit

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.HttpResponseException
import com.google.api.client.http.json.JsonHttpContent
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Histogram
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented.GoogleCounters
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, InstrumentedRetry}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, JsonSupport, WorkspaceJsonSupport}
import spray.json.JsValue

import scala.collection.JavaConversions._
import scala.concurrent._
import scala.util.{Failure, Success, Try}

/**
 * Created by mbemis on 5/10/16.
 */
trait GoogleUtilities extends LazyLogging with InstrumentedRetry with GoogleInstrumented {
  implicit val executionContext: ExecutionContext

  protected def when500orGoogleError(throwable: Throwable): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => {
        ((t.getStatusCode == 403 || t.getStatusCode == 429) && t.getDetails.getErrors.head.getDomain.equalsIgnoreCase("usageLimits")) ||
          (t.getStatusCode == 400 && t.getDetails.getErrors.head.getReason.equalsIgnoreCase("invalid")) ||
          t.getStatusCode == 404 ||
          t.getStatusCode/100 == 5
      }
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case ioe: IOException => true
      case _ => false
    }
  }

  protected def retryWhen500orGoogleError[T](op: () => T)(implicit histo: Histogram): Future[T] = {
    retryExponentially(when500orGoogleError)(() => Future(blocking(op())))
  }

  protected def retryWithRecoverWhen500orGoogleError[T](op: () => T)(recover: PartialFunction[Throwable, T])(implicit histo: Histogram): Future[T] = {
    retryExponentially(when500orGoogleError)(() => Future(blocking(op())).recover(recover))
  }

  protected def executeGoogleRequest[T](request: AbstractGoogleClientRequest[T])(implicit counters: GoogleCounters): T = {
    executeGoogleCall(request) { response =>
      response.parseAs(request.getResponseClass)
    }
  }

  protected def executeGoogleFetch[A,B](request: AbstractGoogleClientRequest[A])(f: (InputStream) => B)(implicit counters: GoogleCounters): B = {
    executeGoogleCall(request) { response =>
      val stream = response.getContent
      try {
        f(stream)
      } finally {
        stream.close()
      }
    }
  }

  protected def executeGoogleCall[A,B](request: AbstractGoogleClientRequest[A])(processResponse: (com.google.api.client.http.HttpResponse) => B)(implicit counters: GoogleCounters): B = {
    val start = System.currentTimeMillis()
    Try {
      request.executeUnparsed()
    } match {
      case Success(response) =>
        logGoogleRequest(request, start, response)
        instrumentGoogleRequest(request, start, Right(response))
        try {
          processResponse(response)
        } finally {
          response.disconnect()
        }
      case Failure(httpRegrets: HttpResponseException) =>
        logGoogleRequest(request, start, httpRegrets)
        instrumentGoogleRequest(request, start, Left(httpRegrets))
        throw httpRegrets
      case Failure(regrets) =>
        logGoogleRequest(request, start, regrets)
        instrumentGoogleRequest(request, start, Left(regrets))
        throw regrets
    }
  }

  private def logGoogleRequest[A](request: AbstractGoogleClientRequest[A], startTime: Long, response: com.google.api.client.http.HttpResponse): Unit = {
    logGoogleRequest(request, startTime, Option(response.getStatusCode), None)
  }

  private def logGoogleRequest[A](request: AbstractGoogleClientRequest[A], startTime: Long, regrets: Throwable): Unit = {
    regrets match {
      case e: HttpResponseException => logGoogleRequest(request, startTime, Option(e.getStatusCode), None)
      case t: Throwable => logGoogleRequest(request, startTime, None, Option(ErrorReport(t)))
    }
  }

  private def logGoogleRequest[A](request: AbstractGoogleClientRequest[A], startTime: Long, statusCode: Option[Int], errorReport: Option[ErrorReport]): Unit = {
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

    logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - startTime, statusCode, errorReport).toJson(GoogleRequestFormat).compactPrint)
  }

  private def instrumentGoogleRequest[A](request: AbstractGoogleClientRequest[A], startTime: Long, responseOrException: Either[Throwable, com.google.api.client.http.HttpResponse])(implicit counters: GoogleCounters): Unit = {
    val (counter, timer) = counters(request, responseOrException)
    counter += 1
    timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS)
  }
}

protected[google] case class GoogleRequest(method: String, url: String, payload: Option[JsValue], time_ms: Long, statusCode: Option[Int], errorReport: Option[ErrorReport])
protected[google] object GoogleRequestJsonSupport extends JsonSupport {
  import WorkspaceJsonSupport.ErrorReportFormat
  import spray.json.DefaultJsonProtocol._
  implicit val GoogleRequestFormat = jsonFormat6(GoogleRequest)
}