package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.{Expansion, InstrumentedRetry, RawlsExpansion, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.util.SprayClientUtils._
import spray.client.pipelining._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.httpx.UnsuccessfulResponseException
import spray.httpx.unmarshalling.FromResponseUnmarshaller
import spray.json._
import spray.routing.directives.PathDirectives._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * @author tsharpe
 */
class HttpMethodRepoDAO(baseMethodRepoServiceURL: String, apiPath: String = "", override val workbenchMetricBaseName: String)(implicit val system: ActorSystem) extends MethodRepoDAO with DsdeHttpDAO with InstrumentedRetry with LazyLogging with RawlsInstrumented {
  import system.dispatcher

  private val methodRepoServiceURL = baseMethodRepoServiceURL + apiPath

  private lazy implicit val baseMetricBuilder: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Agora)

  // Strip out unique IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    (Slash ~ "api" ~ Slash ~ "v1").? / ("configurations" | "methods") / Segment / Segment / Segment
  )

  private def pipeline[A: FromResponseUnmarshaller](userInfo: UserInfo) =
    addAuthHeader(userInfo) ~> instrumentedSendReceive ~> unmarshal[A]


  private def getAgoraEntity( url: String, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    retry(when500) { () =>
      pipeline[Option[AgoraEntity]](userInfo) apply Get(url) recover {
        case notOK: UnsuccessfulResponseException if StatusCodes.NotFound == notOK.response.status => None
      }
    }
  }

  private def postAgoraEntity( url: String, agoraEntity: AgoraEntity, userInfo: UserInfo): Future[AgoraEntity] = {
    retry(when500) { () => pipeline[AgoraEntity](userInfo) apply Post(url, agoraEntity) }
  }

  override def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    getAgoraEntity(s"${methodRepoServiceURL}/configurations/${namespace}/${name}/${version}",userInfo)
  }

  override def getMethod( method: MethodRepoMethod, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    method match {
      case agoraMethod: AgoraMethod =>
        getAgoraEntity(s"${methodRepoServiceURL}/methods/${agoraMethod.methodNamespace}/${agoraMethod.methodName}/${agoraMethod.methodVersion}", userInfo)
      case _ =>
        throw new RawlsException(s"Method repo \'${method.repo.scheme}\' not yet supported")
    }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue / 100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue / 100 == 5
      case _ => false
    }
  }

  override def postMethodConfig(namespace: String, name: String, methodConfiguration: MethodConfiguration, userInfo: UserInfo): Future[AgoraEntity] = {
    val agoraEntity = AgoraEntity(
      namespace = Option(namespace),
      name = Option(name),
      payload = Option(methodConfiguration.toJson.toString),
      entityType = Option(AgoraEntityType.Configuration)
    )
    postAgoraEntity(s"${methodRepoServiceURL}/configurations", agoraEntity, userInfo)
  }

  override def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus] = {
    val url = s"${baseMethodRepoServiceURL}/status"
    val pipeline = sendReceive ~> unmarshal[StatusCheckResponse]
    // Don't retry on the status check
    pipeline(Get(url)).map { statusCheck =>
      SubsystemStatus(ok = statusCheck.ok, None)
    } recover {
      case NonFatal(e) => SubsystemStatus(ok = false, Some(List(e.getMessage)))
    }
  }

}