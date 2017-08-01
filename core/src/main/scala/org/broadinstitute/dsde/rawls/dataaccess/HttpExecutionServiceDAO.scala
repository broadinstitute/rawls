package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.SprayClientUtils._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, Retry}
import spray.client.pipelining._
import spray.http.{BodyPart, MultipartFormData}
import spray.httpx.SprayJsonSupport._
import spray.httpx.unmarshalling.FromResponseUnmarshaller
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String, submissionTimeout: FiniteDuration, override val workbenchMetricBaseName: String )( implicit val system: ActorSystem ) extends ExecutionServiceDAO with DsdeHttpDAO with Retry with FutureSupport with LazyLogging with RawlsInstrumented {
  import system.dispatcher

  private implicit lazy val baseMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Cromwell)

  private def pipeline[A: FromResponseUnmarshaller](userInfo: UserInfo, retryCount: Int = 0) =
    addAuthHeader(userInfo) ~> instrumentedGzSendReceive(retryCount) ~> unmarshal[A]

  private def pipelineWithTimeout[A: FromResponseUnmarshaller](userInfo: UserInfo, timeout: Timeout, retryCount: Int = 0) =
    addAuthHeader(userInfo) ~> instrumentedGzSendReceive(timeout, retryCount) ~> unmarshal[A]

  override def submitWorkflows(wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]] = {
    val timeout = Timeout(submissionTimeout)
    val url = executionServiceURL+"/workflows/v1/batch"
    val formData = Map("workflowSource" -> BodyPart(wdl), "workflowInputs" -> BodyPart(inputs.mkString("[", ",", "]"))) ++ options.map("workflowOptions" -> BodyPart(_))
    pipelineWithTimeout[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]](userInfo, timeout) apply (Post(url, MultipartFormData(formData)))
  }

  override def status(id: String, userInfo: UserInfo): Future[ExecutionServiceStatus] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    retry(when500) { count => pipeline[ExecutionServiceStatus](userInfo, count) apply Get(url) }
  }

  override def callLevelMetadata(id: String, userInfo: UserInfo): Future[JsObject] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/metadata"
    retry(when500) { count => pipeline[JsObject](userInfo, count) apply Get(url) }
  }

  override def outputs(id: String, userInfo: UserInfo): Future[ExecutionServiceOutputs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    retry(when500) { count => pipeline[ExecutionServiceOutputs](userInfo, count) apply Get(url) }
  }

  override def logs(id: String, userInfo: UserInfo): Future[ExecutionServiceLogs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    retry(when500) { count => pipeline[ExecutionServiceLogs](userInfo, count) apply Get(url) }
  }

  override def abort(id: String, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    retry(when500) { count => toFutureTry(pipeline[ExecutionServiceStatus](userInfo, count) apply Post(url)) }
  }

  override def version(userInfo: UserInfo): Future[ExecutionServiceVersion] = {
    val url = executionServiceURL + s"/engine/v1/version"
    retry(when500) { count => pipeline[ExecutionServiceVersion](userInfo, count) apply Get(url) }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue/100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue/100 == 5
      case _ => false
    }
  }
}
