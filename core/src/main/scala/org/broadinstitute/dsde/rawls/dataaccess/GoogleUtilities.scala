package org.broadinstitute.dsde.rawls.dataaccess

import java.io.{IOException, InputStream, ByteArrayOutputStream}

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.HttpResponseException
import com.google.api.client.http.json.JsonHttpContent
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{WorkspaceJsonSupport, JsonSupport, ErrorReport}
import spray.json.JsValue

import scala.collection.JavaConversions._

import scala.concurrent._
import scala.util.{Failure, Success, Try}

/**
 * Created by mbemis on 5/10/16.
 */
trait GoogleUtilities extends LazyLogging with Retry {
  implicit val executionContext: ExecutionContext

  protected def when500orGoogleError( throwable: Throwable ): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => {
        ((t.getStatusCode == 403 || t.getStatusCode == 429) && t.getDetails.getErrors.head.getDomain.equalsIgnoreCase("usageLimits")) ||
          (t.getStatusCode == 400 && t.getDetails.getErrors.head.getReason.equalsIgnoreCase("invalid")) ||
          t.getStatusCode == 404 ||
          t.getStatusCode/100 == 5
      }
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case gse: GoogleServiceException => true
      case ioe: IOException => true
      case _ => false
    }
  }

  protected def retryWhen500orGoogleError[T](op: () => T) = {
    retryExponentially(when500orGoogleError)(() => Future(blocking(op())))
  }

  protected def retryWithRecoverWhen500orGoogleError[T](op: () => T)(recover: PartialFunction[Throwable, T]) = {
    retryExponentially(when500orGoogleError)(() => Future(blocking(op())).recover(recover))
  }

  protected def executeGoogleRequest[T](request: AbstractGoogleClientRequest[T]): T = {
    executeGoogleCall(request) { response =>
      response.parseAs(request.getResponseClass)
    }
  }

  protected def executeGoogleFetch[A,B](request: AbstractGoogleClientRequest[A])(f: (InputStream) => B): B = {
    executeGoogleCall(request) { response =>
      val stream = response.getContent
      try {
        f(stream)
      } finally {
        stream.close()
      }
    }
  }

  protected def executeGoogleCall[A,B](request: AbstractGoogleClientRequest[A])(processResponse: (com.google.api.client.http.HttpResponse) => B): B = {
    val start = System.currentTimeMillis()
    Try {
      request.executeUnparsed()
    } match {
      case Success(response) =>
        logGoogleRequest(request, start, response)
        try {
          processResponse(response)
        } finally {
          response.disconnect()
        }
      case Failure(httpRegrets: HttpResponseException) =>
        logGoogleRequest(request, start, httpRegrets)
        throw httpRegrets
      case Failure(regrets) =>
        logGoogleRequest(request, start, regrets)
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
    import spray.json._
    import GoogleRequestJsonSupport._

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

  protected case class GoogleRequest(method: String, url: String, payload: Option[JsValue], time_ms: Long, statusCode: Option[Int], errorReport: Option[ErrorReport])
  protected object GoogleRequestJsonSupport extends JsonSupport {
    import WorkspaceJsonSupport.ErrorReportFormat
    val GoogleRequestFormat = jsonFormat6(GoogleRequest)
  }
}

class GoogleServiceException(message: String) extends RawlsException(message)