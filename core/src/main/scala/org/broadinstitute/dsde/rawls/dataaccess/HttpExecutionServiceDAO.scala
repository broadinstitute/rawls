package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, Retry}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import spray.client.pipelining._
import spray.http.{BodyPart, FormData, MultipartFormData}
import spray.httpx.SprayJsonSupport._
import spray.http.HttpHeaders.`Accept-Encoding`
import spray.http.HttpEncodings._
import spray.httpx.encoding.Gzip
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject

import scala.util.Try

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String, submissionTimeout: FiniteDuration )( implicit val system: ActorSystem ) extends ExecutionServiceDAO with DsdeHttpDAO with Retry with FutureSupport with LazyLogging {
  import system.dispatcher

  //sendReceive, but with gzip.
  //Two versions, one of which overrides the timeout.
  def gzSendReceive(timeout: Timeout) = {
    implicit val to = timeout //make the explicit implicit, a process known as "tactfulization"
    addHeaders(`Accept-Encoding`(gzip)) ~> sendReceive ~> decode(Gzip)
  }
  def gzSendReceive = addHeaders(`Accept-Encoding`(gzip)) ~> sendReceive ~> decode(Gzip)

  override def submitWorkflows(wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]] = {
    val timeout = Timeout(submissionTimeout)
    val url = executionServiceURL+"/workflows/v1/batch"

    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive(timeout) ~> unmarshal[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]]
    val formData = Map("workflowSource" -> BodyPart(wdl), "workflowInputs" -> BodyPart(inputs.mkString("[", ",", "]"))) ++ options.map("workflowOptions" -> BodyPart(_))
    pipeline(Post(url, MultipartFormData(formData)))
  }

  override def status(id: String, userInfo: UserInfo): Future[ExecutionServiceStatus] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive ~> unmarshal[ExecutionServiceStatus]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def callLevelMetadata(id: String, userInfo: UserInfo): Future[JsObject] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/metadata"
    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive ~> unmarshal[JsObject]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def outputs(id: String, userInfo: UserInfo): Future[ExecutionServiceOutputs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive ~> unmarshal[ExecutionServiceOutputs]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def logs(id: String, userInfo: UserInfo): Future[ExecutionServiceLogs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive ~> unmarshal[ExecutionServiceLogs]
    retry(when500) { () => pipeline(Get(url)) }
  }

  override def abort(id: String, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive ~> unmarshal[ExecutionServiceStatus]
    retry(when500) { () => toFutureTry(pipeline(Post(url))) }
  }

  override def version(userInfo: UserInfo): Future[ExecutionServiceVersion] = {
    val url = executionServiceURL + s"/engine/v1/version"
    val pipeline = addAuthHeader(userInfo) ~> gzSendReceive ~> unmarshal[ExecutionServiceVersion]
    retry(when500) { () => pipeline(Get(url)) }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue/100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue/100 == 5
      case _ => false
    }
  }
}