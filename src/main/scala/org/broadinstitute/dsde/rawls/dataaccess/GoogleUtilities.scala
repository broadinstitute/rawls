package org.broadinstitute.dsde.rawls.dataaccess

import java.io.ByteArrayOutputStream

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.HttpResponseException
import com.google.api.client.http.json.JsonHttpContent
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{WorkspaceJsonSupport, JsonSupport, ErrorReport}
import spray.json.JsValue

import scala.collection.JavaConversions._

import scala.concurrent._
import scala.util.{Failure, Success, Try}

/**
 * Created by mbemis on 5/10/16.
 */
trait GoogleUtilities extends LazyLogging with Retry {

  protected def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case _ => false
    }
  }

  protected def when500orGoogleError( throwable: Throwable ): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => {
        ((t.getStatusCode == 403 || t.getStatusCode == 429) && t.getDetails.getErrors.head.getDomain.equalsIgnoreCase("usageLimits")) ||
          (t.getStatusCode == 400 && t.getDetails.getErrors.head.getReason.equalsIgnoreCase("invalid")) ||
          (t.getStatusCode == 404)
      }
      case h: HttpResponseException => when500(throwable)
      case _ => false
    }
  }

  protected def executeGoogleRequest[T](request: AbstractGoogleClientRequest[T]): T = {
    import spray.json._
    import GoogleRequestJsonSupport._

    if (logger.underlying.isDebugEnabled) {
      val payload = Option(request.getHttpContent) match {
        case Some(content: JsonHttpContent) =>
          Try {
            val outputStream = new ByteArrayOutputStream()
            content.writeTo(outputStream)
            outputStream.toString.parseJson
          }.toOption
        case _ => None
      }

      val start = System.currentTimeMillis()
      Try {
        request.executeUnparsed()
      } match {
        case Success(response) =>
          logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - start, Option(response.getStatusCode), None).toJson(GoogleRequestFormat).compactPrint)
          response.parseAs(request.getResponseClass)
        case Failure(httpRegrets: HttpResponseException) =>
          logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - start, Option(httpRegrets.getStatusCode), None).toJson(GoogleRequestFormat).compactPrint)
          throw httpRegrets
        case Failure(regrets) =>
          logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - start, None, Option(ErrorReport(regrets))).toJson(GoogleRequestFormat).compactPrint)
          throw regrets
      }
    } else {
      request.execute()
    }
  }

  protected case class GoogleRequest(method: String, url: String, payload: Option[JsValue], time_ms: Long, statusCode: Option[Int], errorReport: Option[ErrorReport])
  protected object GoogleRequestJsonSupport extends JsonSupport {
    import WorkspaceJsonSupport.ErrorReportFormat
    val GoogleRequestFormat = jsonFormat6(GoogleRequest)
  }

}
