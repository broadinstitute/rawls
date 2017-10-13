package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.{SubsystemStatus, UserInfo, UserStatus}
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.util.SprayClientUtils._
import spray.client.pipelining.sendReceive
import spray.client.pipelining._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.httpx.UnsuccessfulResponseException
import spray.httpx.unmarshalling.FromResponseUnmarshaller
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue}

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String)(implicit val system: ActorSystem) extends SamDAO with DsdeHttpDAO with Retry with LazyLogging {
  import system.dispatcher

  private val samServiceURL = baseSamServiceURL

  private def pipeline[A: FromResponseUnmarshaller](userInfo: UserInfo) =
    addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[A]

  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val url = samServiceURL + "/register/user"
    retry(when500) { () =>
      pipeline[Option[UserStatus]](userInfo) apply Post(url) recover {
        case notOK: UnsuccessfulResponseException if StatusCodes.Conflict == notOK.response.status => None
      }
    }
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case ure: spray.client.UnsuccessfulResponseException => ure.responseStatus.intValue / 100 == 5
      case ure: spray.httpx.UnsuccessfulResponseException => ure.response.status.intValue / 100 == 5
      case _ => false
    }
  }

  override def getStatus(): Future[SubsystemStatus] = {
    val url = samServiceURL + "/status"
    val pipeline = sendReceive
    pipeline(Get(url)) map { response =>
      val ok = response.status.isSuccess
      SubsystemStatus(ok, if (ok) None else Option(List(response.entity.asString)))
    }
  }

}
