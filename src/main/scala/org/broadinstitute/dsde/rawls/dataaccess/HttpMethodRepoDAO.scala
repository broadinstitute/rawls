package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model.{UserInfo, AgoraEntity}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success,Failure,Try}
import spray.httpx.UnsuccessfulResponseException
import spray.client.pipelining._
import spray.http.{StatusCodes, HttpCookie}
import spray.http.HttpHeaders.Cookie
import spray.httpx.SprayJsonSupport._

/**
 * @author tsharpe
 */
class HttpMethodRepoDAO( methodRepoServiceURL: String )( implicit system: ActorSystem ) extends MethodRepoDAO with DsdeHttpDAO with Retry {

  private def getAgoraEntity( url: String, userInfo: UserInfo ): Option[AgoraEntity] = {
    import system.dispatcher
    val pipeline = addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[AgoraEntity]
    tryRetry(Await.result(pipeline(Get(url)),Duration.Inf),when500) match {
      case Success(entity) => Option(entity)
      case Failure(notOK: UnsuccessfulResponseException) if StatusCodes.NotFound == notOK.response.status => None
      case Failure(exception) => throw exception
    }
  }

  override def getMethodConfig( namespace: String, name: String, version: Int, userInfo: UserInfo ): Option[AgoraEntity] = {
    getAgoraEntity(s"${methodRepoServiceURL}/configurations/${namespace}/${name}/${version}",userInfo)
  }

  override def getMethod( namespace: String, name: String, version: Int, userInfo: UserInfo ): Option[AgoraEntity] = {
    getAgoraEntity(s"${methodRepoServiceURL}/methods/${namespace}/${name}/${version}",userInfo)
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue/100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue/100 == 5
      case _ => false
    }
  }
}