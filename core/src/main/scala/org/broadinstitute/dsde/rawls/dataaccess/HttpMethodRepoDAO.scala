package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.{Expansion, InstrumentedRetry, RawlsExpansion, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import akka.http.scaladsl.server.directives.PathDirectives._
import akka.http.scaladsl.server.PathMatchers.Segment
import org.broadinstitute.dsde.rawls.util.HttpClientUtilsGzipInstrumented
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.Materializer
import spray.json.DefaultJsonProtocol._
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport._

/**
 * @author tsharpe
 */
class HttpMethodRepoDAO(baseAgoraServiceURL: String, agoraApiPath: String, baseDockstoreServiceURL: String, dockstoreApiPath: String, override val workbenchMetricBaseName: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends MethodRepoDAO with DsdeHttpDAO with InstrumentedRetry with LazyLogging with RawlsInstrumented with ServiceDAOWithStatus {
  import system.dispatcher

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsGzipInstrumented()

  private val agoraServiceURL = baseAgoraServiceURL + agoraApiPath
  private val dockstoreServiceURL = baseDockstoreServiceURL + dockstoreApiPath
  protected val statusUrl = s"$agoraServiceURL/status"

  private lazy implicit val baseMetricBuilder: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Agora)

  // Strip out unique IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    Seq((Slash ~ "api" ~ Slash ~ "v1").? / ("configurations" | "methods") / Segment / Segment / Segment)
  )

  private def getAgoraEntity( url: String, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    retry(when500) { () =>
      pipeline[Option[AgoraEntity]](userInfo) apply Get(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
      }
    }
  }

  private def postAgoraEntity( url: String, agoraEntity: AgoraEntity, userInfo: UserInfo): Future[AgoraEntity] = {
    retry(when500) { () => pipeline[AgoraEntity](userInfo) apply Post(url, agoraEntity) }
  }

  override def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    getAgoraEntity(s"$agoraServiceURL/configurations/$namespace/$name/$version",userInfo)
  }

  override def getMethod( method: MethodRepoMethod, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    method match {
      case agoraMethod: AgoraMethod =>
        getAgoraEntity(s"$agoraServiceURL/methods/${agoraMethod.methodNamespace}/${agoraMethod.methodName}/${agoraMethod.methodVersion}", userInfo)
      case dockstoreMethod: DockstoreMethod =>
        getDockstoreEntity(dockstoreMethod) flatMap { response: GA4GHToolDescriptor =>
          // TODO: re-using AgoraEntity feels sketchy to me. It seems to work without any changes, but should we create a DockstoreEntity?
          Future(Some(AgoraEntity(payload = Some(response.descriptor), entityType = Some(AgoraEntityType.Workflow))))
        }
    }
  }

  private def getDockstoreEntity(method: DockstoreMethod): Future[GA4GHToolDescriptor] = {
    httpClientUtils.executeRequestUnmarshalResponse[GA4GHToolDescriptor](http, Get(method.ga4ghDescriptorUrl(dockstoreServiceURL)))
  }

  override def postMethodConfig(namespace: String, name: String, methodConfiguration: MethodConfiguration, userInfo: UserInfo): Future[AgoraEntity] = {
    methodConfiguration.methodRepoMethod match {
      case _ : AgoraMethod =>
        val agoraEntity = AgoraEntity(
          namespace = Option(namespace),
          name = Option(name),
          payload = Option(methodConfiguration.toJson.toString),
          entityType = Option(AgoraEntityType.Configuration)
        )
        postAgoraEntity(s"$agoraServiceURL/configurations", agoraEntity, userInfo)
      case otherMethod =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Action not supported for method repo '${otherMethod.repo.scheme}'"))
    }


  }

  // TODO: if we ever want Dockstore healthchecks, we will need to split this DAO in two
  override def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus] = {
    val url = s"$baseAgoraServiceURL/status"
    // Don't retry on the status check
    httpClientUtils.executeRequestUnmarshalResponse[StatusCheckResponse](http, RequestBuilding.Get(url)) map { statusCheck =>
      SubsystemStatus(ok = statusCheck.ok, None)
    } recover {
      case NonFatal(e) => SubsystemStatus(ok = false, Some(List(e.getMessage)))
    }
  }

}