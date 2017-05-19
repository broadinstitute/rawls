package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AgoraEntity, AgoraEntityType, AgoraStatus, MethodConfiguration, UserInfo}
import org.broadinstitute.dsde.rawls.util.Retry
import spray.client.pipelining._
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.httpx.UnsuccessfulResponseException
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * @author tsharpe
 */
class HttpMethodRepoDAO( methodRepoServiceURL: String)( implicit val system: ActorSystem ) extends MethodRepoDAO with DsdeHttpDAO with Retry with LazyLogging {

  private def getAgoraEntity( url: String, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[Option[AgoraEntity]]
    retry(when500) { () => pipeline(Get(url)) } recover {
      case notOK: UnsuccessfulResponseException if StatusCodes.NotFound == notOK.response.status => None
    }
  }

  private def postAgoraEntity( url: String, agoraEntity: AgoraEntity, userInfo: UserInfo): Future[AgoraEntity] = {
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[AgoraEntity]
    retry(when500) { () => pipeline(Post(url, agoraEntity)) }
  }

  override def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    getAgoraEntity(s"${methodRepoServiceURL}/configurations/${namespace}/${name}/${version}",userInfo)
  }

  override def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Future[Option[AgoraEntity]] = {
    getAgoraEntity(s"${methodRepoServiceURL}/methods/${namespace}/${name}/${version}",userInfo)
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

  override def getStatus(implicit executionContext: ExecutionContext): Future[AgoraStatus] = {
    val url = s"${methodRepoServiceURL}/status"
    val pipeline = sendReceive ~> unmarshal[AgoraStatus]
    // Don't retry on the status check
    pipeline(Get(url)) recover {
      case NonFatal(e) => AgoraStatus(false, Seq(e.getMessage))
    }
  }

}