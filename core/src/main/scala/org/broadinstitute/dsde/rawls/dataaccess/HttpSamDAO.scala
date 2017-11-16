package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.{RawlsGroupEmail, SubsystemStatus, SyncReportItem, UserInfo, UserStatus, WorkspaceJsonSupport}
import org.broadinstitute.dsde.rawls.util.Retry
import spray.client.pipelining.{sendReceive, _}
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.httpx.UnsuccessfulResponseException
import spray.httpx.marshalling.Marshaller
import spray.httpx.unmarshalling.{Unmarshaller, _}
import spray.json.DefaultJsonProtocol.jsonFormat3
import spray.json.{DefaultJsonProtocol, JsBoolean, JsValue, JsonParser, JsonPrinter, JsonReader, JsonWriter, PrettyPrinter, RootJsonReader, RootJsonWriter, jsonReader}

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String)(implicit val system: ActorSystem) extends SamDAO with DsdeHttpDAO with Retry with LazyLogging {
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

  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Boolean] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId"
    val httpRequest = Post(url)
    val pipeline = addAuthHeader(userInfo) ~> sendReceive
    val result: Future[HttpResponse] = pipeline(httpRequest)

    retry(when500) { () =>
      result.map { response =>
        response.status match {
          case s if s.isSuccess => true
          case _ => false
        }
      }
    }
  }

  override def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Boolean] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId"
    val httpRequest = Delete(url)
    val pipeline = addAuthHeader(userInfo) ~> sendReceive
    val result: Future[HttpResponse] = pipeline(httpRequest)

    retry(when500) { () =>
      result.map { response =>
        response.status match {
          case s if s.isSuccess => true
          case _ => false
        }
      }
    }
  }

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/action/${action.value}"

    // special RootJsonReader because DefaultJsonProtocol.BooleanJsonFormat is not root and the implicit
    // conversion to an Unmarshaller needs a root
    implicit val rootJsBooleanReader = new RootJsonReader[Boolean] {
      override def read(json: JsValue): Boolean = DefaultJsonProtocol.BooleanJsonFormat.read(json)
    }

    retry(when500) { () => pipeline[Boolean](userInfo) apply Get(url) }
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Boolean] = {
    implicit val SamPolicyFormat = jsonFormat3(SamPolicy)

    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies/$policyName"
    val httpRequest = Put(url, policy)
    val pipeline = addAuthHeader(userInfo) ~> sendReceive
    val result: Future[HttpResponse] = pipeline(httpRequest)

    retry(when500) { () =>
      result.map { response =>
        response.status match {
          case s if s.isSuccess => true
          case _ => false
        }
      }
    }
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Boolean] = {
    getResourcePolicies(resourceTypeName, resourceId, userInfo).flatMap { resourcePolicies =>
      val targetPolicy = resourcePolicies.filter(_.policyName.equalsIgnoreCase(policyName)).head //get or else return 404 or something
      val updatedMembers = targetPolicy.policy.memberEmails :+ memberEmail
      val updatedPolicy = targetPolicy.policy.copy(memberEmails = updatedMembers)

      overwritePolicy(resourceTypeName, resourceId, policyName, updatedPolicy, userInfo)
    }
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Boolean] = {
    getResourcePolicies(resourceTypeName, resourceId, userInfo).flatMap { resourcePolicies =>
      val targetPolicy = resourcePolicies.filter(_.policyName.equalsIgnoreCase(policyName)).head //get or else return 404 or something
      val updatedMembers = targetPolicy.policy.memberEmails.filterNot(_.equalsIgnoreCase(memberEmail))
      val updatedPolicy = targetPolicy.policy.copy(memberEmails = updatedMembers)

      overwritePolicy(resourceTypeName, resourceId, policyName, updatedPolicy, userInfo)
    }
  }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[Map[RawlsGroupEmail, Seq[SyncReportItem]]] = {
//    val url = samServiceURL + s"/api/google/${resourceTypeName.value}/$resourceId/$policyName/sync"
//
//    import spray.json.DefaultJsonProtocol._
//    import WorkspaceJsonSupport.ErrorReportFormat
//    implicit val SyncReportItemFormat = jsonFormat3(SyncReportItem)
//
//    pipeline[Map[RawlsGroupEmail, Seq[SyncReportItem]]](userInfo) apply Post(url) //todo: retries

    Future.successful(Map.empty)
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    implicit val SamResourceIdWithPolicyNameFormat = jsonFormat2(SamResourceIdWithPolicyName)
    import spray.json.DefaultJsonProtocol._

    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}"
    pipeline[Set[SamResourceIdWithPolicyName]](userInfo) apply Get(url) //todo: retries
  }

  override def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
    implicit val SamPolicyWithNameFormat = jsonFormat2(SamPolicyWithName)
    import spray.json.DefaultJsonProtocol._
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/${resourceId}/policies"
    pipeline[Set[SamPolicyWithName]](userInfo) apply Get(url) //todo: retries
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
