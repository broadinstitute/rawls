package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.model.AgoraEntity
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
class HttpMethodRepoDAO( methodRepoServiceURL: String )( implicit system: ActorSystem ) extends MethodRepoDAO {

  private def getAgoraEntity( url: String, authCookie: HttpCookie ): Option[AgoraEntity] = {
    import system.dispatcher
    val pipeline = addHeader(Cookie(authCookie)) ~> sendReceive ~> unmarshal[AgoraEntity]
    Try(Await.result(pipeline(Get(url)),Duration.Inf)) match {
      case Success(entity) => Option(entity)
      case Failure(notOK: UnsuccessfulResponseException) if StatusCodes.NotFound == notOK.response.status => None
      case Failure(exception) => throw exception
    }
  }

  override def getMethodConfig( namespace: String, name: String, version: String, authCookie: HttpCookie ): Option[AgoraEntity] = {
    getAgoraEntity(s"${methodRepoServiceURL}/configurations/${namespace}/${name}/${version}",authCookie)
  }

  override def getMethod( namespace: String, name: String, version: String, authCookie: HttpCookie ): Option[AgoraEntity] = {
    getAgoraEntity(s"${methodRepoServiceURL}/methods/${namespace}/${name}/${version}",authCookie)
  }
}