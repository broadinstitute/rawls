package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.dataaccess.SamModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.{ErrorReport, ManagedGroupAccessResponse, ManagedRoles, RawlsGroupEmail, RawlsUserEmail, SubsystemStatus, SyncReportItem, UserInfo, UserStatus, WorkspaceJsonSupport}
import org.broadinstitute.dsde.rawls.util.{HttpClientUtils, HttpClientUtilsGzipInstrumented, HttpClientUtilsStandard, Retry}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport.WorkbenchEmailFormat
import spray.json.{DefaultJsonProtocol, JsBoolean, JsValue, JsonParser, JsonPrinter, JsonReader, JsonWriter, PrettyPrinter, RootJsonReader, RootJsonWriter, jsonReader}
import DefaultJsonProtocol._
import akka.http.scaladsl.client.RequestBuilding
import akka.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String, serviceAccountCreds: Credential)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends SamDAO with DsdeHttpDAO with Retry with LazyLogging with ServiceDAOWithStatus {
  import system.dispatcher

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsStandard()

  private val samServiceURL = baseSamServiceURL
  protected val statusUrl = samServiceURL + "/status"

  private def asRawlsSAPipeline[A](implicit um: Unmarshaller[ResponseEntity, A]) = executeRequestWithToken[A](OAuth2BearerToken(getServiceAccountAccessToken)) _

  private def doSuccessOrFailureRequest(request: HttpRequest, userInfo: UserInfo) = {
    retry(when500) { () =>
      httpClientUtils.executeRequest(http, httpClientUtils.addHeader(request, authHeader(userInfo))).map { response =>
        response.status match {
          case s if s.isSuccess => ()
          case f => throw new RawlsExceptionWithErrorReport(ErrorReport(f, s"Sam call to ${request.method} ${request.uri.path} failed"))
        }
      }
    }
  }

  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val url = samServiceURL + "/register/user"
    retry(when500) { () =>
      pipeline[Option[UserStatus]](userInfo) apply RequestBuilding.Post(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.Conflict) => None
      }
    }
  }

  override def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val url = samServiceURL + "/register/user"
    retry(when500) { () =>
      pipeline[Option[UserStatus]](userInfo) apply RequestBuilding.Get(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
      }
    }
  }

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = {
    val url = samServiceURL + s"/api/google/user/proxyGroup/$targetUserEmail"
    retry(when500) { () =>
      pipeline[WorkbenchEmail](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId"
    val httpRequest = HttpRequest(POST, Uri(url))

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
    // special RootJsonReader because DefaultJsonProtocol.BooleanJsonFormat is not root and the implicit
    // conversion to an Unmarshaller needs a root
    implicit val rootJsBooleanReader = new RootJsonReader[Boolean] {
      override def read(json: JsValue): Boolean = DefaultJsonProtocol.BooleanJsonFormat.read(json)
    }

    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/action/${action.value}"

    retry(when500) { () => pipeline[Boolean](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicy] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies/${policyName.toLowerCase}"
    val httpRequest = RequestBuilding.Get(url)

    retry(when500) { () => pipeline[SamPolicy](userInfo) apply httpRequest }
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies/${policyName.toLowerCase}"

    Marshal(policy).to[RequestEntity].flatMap { policyEntity =>
      val httpRequest = HttpRequest(PUT, Uri(url), entity = policyEntity)
      doSuccessOrFailureRequest(httpRequest, userInfo)
    }
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies/${policyName.toLowerCase}/memberEmails/$memberEmail"
    val httpRequest = RequestBuilding.Put(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies/${policyName.toLowerCase}/memberEmails/$memberEmail"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String): Future[Map[RawlsGroupEmail, Seq[SyncReportItem]]] = {
    val url = samServiceURL + s"/api/google/resource/${resourceTypeName.value}/$resourceId/${policyName.toLowerCase}/sync"
    retry(when500) { () => asRawlsSAPipeline[Map[RawlsGroupEmail, Seq[SyncReportItem]]] apply HttpRequest(POST, Uri(url)) }
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}"
    retry(when500) { () => pipeline[Set[SamResourceIdWithPolicyName]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies"
    retry(when500) { () => pipeline[Set[SamPolicyWithName]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String] = {
    val url = samServiceURL + s"/api/google/petServiceAccount/$googleProject/${userEmail.value}"
    retry(when500) { () => asRawlsSAPipeline[String] apply RequestBuilding.Get(url) }
  }


  //managed group apis

  override def createGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/group/${groupName.value}"
    val httpRequest = RequestBuilding.Post(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def deleteGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/group/${groupName.value}"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def listGroupPolicyEmails(groupName: WorkbenchGroupName, policyName: ManagedRoles.ManagedRole, userInfo: UserInfo): Future[List[WorkbenchEmail]] = {
    val url = samServiceURL + s"/api/group/${groupName.value}/${policyName.toString.toLowerCase}"

    retry(when500) { () => pipeline[List[WorkbenchEmail]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getGroupEmail(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[WorkbenchEmail] = {
    val url = samServiceURL + s"/api/group/${groupName.value}"

    retry(when500) { () => pipeline[WorkbenchEmail](userInfo) apply RequestBuilding.Get(url) }
  }

  override def listManagedGroups(userInfo: UserInfo): Future[List[ManagedGroupAccessResponse]] = {
    val url = samServiceURL + s"/api/groups"

    retry(when500) { () => pipeline[List[ManagedGroupAccessResponse]](userInfo) apply RequestBuilding.Get(url) }
  }


  override def addUserToManagedGroup(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmail: WorkbenchEmail, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/group/${groupName.value}/${role.toString.toLowerCase}/${memberEmail.value}"
    val httpRequest = RequestBuilding.Put(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def removeUserFromManagedGroup(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmail: WorkbenchEmail, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/group/${groupName.value}/${role.toString.toLowerCase}/${memberEmail.value}"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def overwriteManagedGroupMembership(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmails: Seq[WorkbenchEmail], userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/group/${groupName.value}/${role.toString.toLowerCase}"
    val httpRequest = RequestBuilding.Put(url, memberEmails)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }


  override def requestAccessToManagedGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/group/${groupName.value}/requestAccess"
    val httpRequest = RequestBuilding.Post(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }


  private def getServiceAccountAccessToken = {
    val expiresInSeconds = Option(serviceAccountCreds.getExpiresInSeconds).map(_.longValue()).getOrElse(0L)
    if (expiresInSeconds < 60*5) {
      serviceAccountCreds.refreshToken()
    }
    serviceAccountCreds.getAccessToken
  }
}
