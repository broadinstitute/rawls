package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{Multipart, RequestEntity, Uri}
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.directives.PathDirectives._
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.{Expansion, InstrumentedRetry, RawlsExpansion, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsGzipInstrumented}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * @author tsharpe
 */
class HttpExecutionServiceDAO(executionServiceURL: String, override val workbenchMetricBaseName: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends ExecutionServiceDAO
    with DsdeHttpDAO
    with InstrumentedRetry
    with FutureSupport
    with LazyLogging
    with RawlsInstrumented {

  implicit private lazy val baseMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Cromwell)

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsGzipInstrumented()

  // Strip out workflow IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    Seq((Slash ~ "api").? / "workflows" / "v1" / Segment / Neutral)
  )

  override def submitWorkflows(wdl: WDL,
                               inputs: Seq[String],
                               options: Option[String],
                               labels: Option[Map[String, String]],
                               workflowCollection: Option[String],
                               userInfo: UserInfo
  ): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]] = {
    val url = executionServiceURL + "/api/workflows/v1/batch"
    val labelsBodyPart = labels.map(labelsMap => Multipart.FormData.BodyPart("labels", labelsMap.toJson.compactPrint))
    val wfc = workflowCollection.map(Multipart.FormData.BodyPart("collectionName", _))

    val wdlSourceOrUrl = wdl match {
      case wdl: WdlUrl    => Multipart.FormData.BodyPart("workflowUrl", wdl.url)
      case wdl: WdlSource => Multipart.FormData.BodyPart("workflowSource", wdl.source)
    }

    val inputsJsonArray = inputs.mkString("[", ",", "]")

    // Map over inputs to make this list the same size, but the inputs themselves are ignored.
    val requestedWorkflowIdsJsonArray = inputs.map(_ => s""""${UUID.randomUUID().toString}"""").mkString("[", ",", "]")

    val bodyParts = Seq(
      wdlSourceOrUrl,
      Multipart.FormData.BodyPart("workflowInputs", inputsJsonArray),
      Multipart.FormData.BodyPart("requestedWorkflowId", requestedWorkflowIdsJsonArray)
    ) ++ options.map(Multipart.FormData.BodyPart("workflowOptions", _)) ++ labelsBodyPart ++ wfc

    val formData = Multipart.FormData(bodyParts: _*)

    retry(anyOf(when5xx, DsdeHttpDAO.whenUnauthorized)) { () =>
      pipeline[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]](userInfo) apply (Post(
        url,
        Marshal(formData).to[RequestEntity]
      ))
    }
  }

  override def status(id: String, userInfo: UserInfo): Future[ExecutionServiceStatus] = {
    val url = executionServiceURL + s"/api/workflows/v1/${id}/status"
    retry(when5xx)(() => pipeline[ExecutionServiceStatus](userInfo) apply Get(url))
  }

  // break out uri generation into a separate method so it's easily unit-testable
  def getExecutionServiceMetadataUri(id: String, metadataParams: MetadataParams): Uri = {
    val params = metadataParams.includeKeys.map(("includeKey", _)) ++
      metadataParams.excludeKeys.map(("excludeKey", _)) +
      (("expandSubWorkflows", metadataParams.expandSubWorkflows.toString))

    Uri(executionServiceURL + s"/api/workflows/v1/${id}/metadata").withQuery(Query(params.toList: _*))
  }

  override def callLevelMetadata(id: String, metadataParams: MetadataParams, userInfo: UserInfo): Future[JsObject] =
    retry(when5xx)(() => pipeline[JsObject](userInfo) apply Get(getExecutionServiceMetadataUri(id, metadataParams)))

  override def outputs(id: String, userInfo: UserInfo): Future[ExecutionServiceOutputs] = {
    val url = executionServiceURL + s"/api/workflows/v1/${id}/outputs"
    retry(when5xx)(() => pipeline[ExecutionServiceOutputs](userInfo) apply Get(url))
  }

  override def logs(id: String, userInfo: UserInfo): Future[ExecutionServiceLogs] = {
    val url = executionServiceURL + s"/api/workflows/v1/${id}/logs"
    retry(when5xx)(() => pipeline[ExecutionServiceLogs](userInfo) apply Get(url))
  }

  override def abort(id: String, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]] = {
    val url = executionServiceURL + s"/api/workflows/v1/${id}/abort"
    retry(when5xx)(() => toFutureTry(pipeline[ExecutionServiceStatus](userInfo) apply Post(url)))
  }

  override def getLabels(id: String, userInfo: UserInfo): Future[ExecutionServiceLabelResponse] = {
    val url = executionServiceURL + s"/api/workflows/v1/${id}/labels"
    retry(when5xx)(() => pipeline[ExecutionServiceLabelResponse](userInfo) apply Get(url))
  }

  override def patchLabels(id: String,
                           userInfo: UserInfo,
                           labels: Map[String, String]
  ): Future[ExecutionServiceLabelResponse] = {
    val url = executionServiceURL + s"/api/workflows/v1/${id}/labels"
    retry(when5xx)(() => pipeline[ExecutionServiceLabelResponse](userInfo) apply Patch(url, labels))
  }

  override def version(): Future[ExecutionServiceVersion] = {
    val url = executionServiceURL + s"/engine/v1/version"
    retry(when5xx)(() => httpClientUtils.executeRequestUnmarshalResponse[ExecutionServiceVersion](http, Get(url)))
  }

  override def getStatus(): Future[Map[String, SubsystemStatus]] = {
    val url = executionServiceURL + s"/engine/v1/status"
    // we're explicitly not retrying on 500 here
    httpClientUtils.executeRequestUnmarshalResponse[Map[String, SubsystemStatus]](http, Get(url))
  }
}
