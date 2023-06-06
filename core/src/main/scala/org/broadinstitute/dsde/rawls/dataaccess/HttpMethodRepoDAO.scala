package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.directives.PathDirectives._
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.{Expansion, InstrumentedRetry, RawlsExpansion, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.HttpClientUtilsGzipInstrumented
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * @author tsharpe
 */
class HttpMethodRepoDAO(agoraConfig: MethodRepoConfig[Agora.type],
                        dockstoreConfig: MethodRepoConfig[Dockstore.type],
                        override val workbenchMetricBaseName: String
)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
    extends MethodRepoDAO
    with DsdeHttpDAO
    with InstrumentedRetry
    with LazyLogging
    with RawlsInstrumented
    with ServiceDAOWithStatus {

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsGzipInstrumented()

  private val agoraServiceURL = agoraConfig.serviceUrl
  private val dockstoreServiceURL = dockstoreConfig.serviceUrl
  protected val statusUrl = s"$agoraServiceURL/status"

  implicit private lazy val baseMetricBuilder: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Agora)

  // Strip out unique IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    Seq((Slash ~ "api" ~ Slash ~ "v1").? / ("configurations" | "methods") / Segment / Segment / Segment)
  )

  private def postAgoraEntity(url: String, agoraEntity: AgoraEntity, userInfo: UserInfo): Future[AgoraEntity] =
    retry(when5xx)(() => pipeline[AgoraEntity](userInfo) apply Post(url, agoraEntity))

  override def getMethodConfig(namespace: String,
                               name: String,
                               version: Int,
                               userInfo: UserInfo
  ): Future[Option[AgoraEntity]] =
    retry(when5xx) { () =>
      getAgoraEntity(s"$agoraServiceURL/configurations/$namespace/$name/$version", userInfo)
    }

  private def noneIfNotFound[T]: PartialFunction[Throwable, Option[T]] = {
    case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
  }

  override def getMethod(method: MethodRepoMethod, userInfo: UserInfo): Future[Option[WDL]] =
    retry(anyOf(when5xx, DsdeHttpDAO.whenUnauthorized)) { () =>
      method match {
        case agoraMethod: AgoraMethod =>
          getAgoraEntity(
            s"$agoraServiceURL/methods/${agoraMethod.methodNamespace}/${agoraMethod.methodName}/${agoraMethod.methodVersion}",
            userInfo
          ) map { maybeEntity =>
            for {
              entity: AgoraEntity <- maybeEntity
              payload <- entity.payload
            } yield WdlSource(payload)
          }
        case dockstoreMethod: DockstoreMethod =>
          getDockstoreMethod(dockstoreMethod) map { maybeTool =>
            for {
              tool: GA4GHTool <- maybeTool
            } yield WdlUrl(tool.url) // We submit the Github URL to Cromwell so that relative imports work.
          }
        case method: DockstoreToolsMethod =>
          for {
            maybeTool <- pipeline[Option[GA4GHTool]] apply Get(
              method.ga4ghDescriptorUrl(dockstoreServiceURL)
            ) recover noneIfNotFound
          } yield maybeTool.map(tool => WdlUrl(tool.url))
      }
    }

  private def getAgoraEntity(url: String, userInfo: UserInfo): Future[Option[AgoraEntity]] =
    retry(when5xx) { () =>
      pipeline[Option[AgoraEntity]](userInfo) apply Get(url) recover noneIfNotFound
    }

  private def getDockstoreMethod(method: DockstoreMethod): Future[Option[GA4GHTool]] =
    pipeline[Option[GA4GHTool]] apply Get(method.ga4ghDescriptorUrl(dockstoreServiceURL)) recover noneIfNotFound

  override def postMethodConfig(namespace: String,
                                name: String,
                                methodConfiguration: MethodConfiguration,
                                userInfo: UserInfo
  ): Future[AgoraEntity] =
    methodConfiguration.methodRepoMethod match {
      case _: AgoraMethod =>
        val agoraEntity = AgoraEntity(
          namespace = Option(namespace),
          name = Option(name),
          payload = Option(methodConfiguration.toJson.toString),
          entityType = Option(AgoraEntityType.Configuration)
        )
        postAgoraEntity(s"$agoraServiceURL/configurations", agoraEntity, userInfo)
      case otherMethod =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Action not supported for method repo '${otherMethod.repo.scheme}'")
        )
    }

  // TODO: if we ever want Dockstore healthchecks, we will need to split this DAO in two
  override def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus] = {
    val url = s"${agoraConfig.baseUrl}/status"
    // Don't retry on the status check
    httpClientUtils.executeRequestUnmarshalResponse[StatusCheckResponse](http, RequestBuilding.Get(url)) map {
      statusCheck =>
        SubsystemStatus(ok = statusCheck.ok, None)
    } recover { case NonFatal(e) =>
      SubsystemStatus(ok = false, Some(List(e.getMessage)))
    }
  }

}
