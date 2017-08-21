package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.{Expansion, InstrumentedRetry, RawlsExpansion, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.FutureSupport
import org.broadinstitute.dsde.rawls.util.SprayClientUtils._
import spray.client.pipelining._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.httpx.unmarshalling.FromResponseUnmarshaller
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import spray.routing.directives.PathDirectives._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO( executionServiceURL: String, submissionTimeout: FiniteDuration, override val workbenchMetricBaseName: String )( implicit val system: ActorSystem ) extends ExecutionServiceDAO with DsdeHttpDAO with InstrumentedRetry with FutureSupport with LazyLogging with RawlsInstrumented {
  import system.dispatcher

  private implicit lazy val baseMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Cromwell)

  // Strip out workflow IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    (Slash ~ "api").? / "workflows" / "v1" / Segment / Neutral
  )

  private def pipeline[A: FromResponseUnmarshaller](userInfo: UserInfo) =
    addAuthHeader(userInfo) ~> instrumentedGzSendReceive ~> unmarshal[A]

  private def pipelineWithTimeout[A: FromResponseUnmarshaller](userInfo: UserInfo, timeout: Timeout) =
    addAuthHeader(userInfo) ~> instrumentedGzSendReceive(timeout) ~> unmarshal[A]

  override def submitWorkflows(wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]] = {
    val timeout = Timeout(submissionTimeout)
    val url = executionServiceURL+"/workflows/v1/batch"
    val formData = Map("workflowSource" -> BodyPart(wdl), "workflowInputs" -> BodyPart(inputs.mkString("[", ",", "]"))) ++ options.map("workflowOptions" -> BodyPart(_))
    pipelineWithTimeout[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]](userInfo, timeout) apply (Post(url, MultipartFormData(formData)))
  }

  override def status(id: String, userInfo: UserInfo): Future[ExecutionServiceStatus] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/status"
    retry(when500) { () => pipeline[ExecutionServiceStatus](userInfo) apply Get(url) }
  }

  override def callLevelMetadata(id: String, userInfo: UserInfo): Future[JsObject] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/metadata"
    retry(when500) { () => pipeline[JsObject](userInfo) apply Get(url) }
  }

  override def outputs(id: String, userInfo: UserInfo): Future[ExecutionServiceOutputs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/outputs"
    retry(when500) { () => pipeline[ExecutionServiceOutputs](userInfo) apply Get(url) }
  }

  override def logs(id: String, userInfo: UserInfo): Future[ExecutionServiceLogs] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/logs"
    retry(when500) { () => pipeline[ExecutionServiceLogs](userInfo) apply Get(url) }
  }

  override def abort(id: String, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] = {
    val url = executionServiceURL + s"/workflows/v1/${id}/abort"
    retry(when500) { () => toFutureTry(pipeline[ExecutionServiceStatus](userInfo) apply Post(url)) }
  }

  override def version(userInfo: UserInfo): Future[ExecutionServiceVersion] = {
    val url = executionServiceURL + s"/engine/v1/version"
    retry(when500) { () => pipeline[ExecutionServiceVersion](userInfo) apply Get(url) }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue/100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue/100 == 5
      case _ => false
    }
  }
}
