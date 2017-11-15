package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.{SubsystemStatus, UserInfo, UserStatus}
import org.broadinstitute.dsde.rawls.util.Retry
import spray.client.pipelining.{sendReceive, _}
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.httpx.UnsuccessfulResponseException
import spray.httpx.marshalling.Marshaller
import spray.httpx.unmarshalling.{Unmarshaller, _}
import spray.json.{DefaultJsonProtocol, JsBoolean, JsValue, JsonParser, JsonPrinter, JsonReader, JsonWriter, PrettyPrinter, RootJsonReader, RootJsonWriter, jsonReader}

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String)(implicit val system: ActorSystem) extends SamDAO with DsdeHttpDAO with Retry with LazyLogging  with spray.httpx.RequestBuilding {
  import system.dispatcher

  private val samServiceURL = baseSamServiceURL

  private def pipeline[A: Unmarshaller](userInfo: UserInfo) =
    addAuthHeader(userInfo) ~> sendReceive ~> unmarshal[A]

  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val url = samServiceURL + "/register/user"
    retry(when500) { () =>
      pipeline[Option[UserStatus]](userInfo) apply Post(url) recover {
        case notOK: UnsuccessfulResponseException if StatusCodes.Conflict == notOK.response.status => None
      }
    }
  }

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/${resourceId}/action/${action.value}"

    // special RootJsonReader because DefaultJsonProtocol.BooleanJsonFormat is not root and the implicit
    // conversion to an Unmarshaller needs a root
    implicit val rootJsBooleanReader = new RootJsonReader[Boolean] {
      override def read(json: JsValue): Boolean = DefaultJsonProtocol.BooleanJsonFormat.read(json)
    }
    
    retry(when500) { () => pipeline[Boolean](userInfo) apply Get(url) }
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
