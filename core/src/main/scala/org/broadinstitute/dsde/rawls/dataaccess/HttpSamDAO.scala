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
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtils, HttpClientUtilsGzipInstrumented, HttpClientUtilsStandard, Retry}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport.WorkbenchEmailFormat
import spray.json.{DefaultJsonProtocol, JsBoolean, JsValue, JsonParser, JsonPrinter, JsonReader, JsonWriter, PrettyPrinter, RootJsonReader, RootJsonWriter, jsonReader}
import DefaultJsonProtocol._
import akka.http.scaladsl.client.RequestBuilding
import akka.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String, serviceAccountCreds: Credential)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
  extends SamDAO with DsdeHttpDAO with Retry with LazyLogging with ServiceDAOWithStatus with FutureSupport {
  import system.dispatcher

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsStandard()

  private val samServiceURL = baseSamServiceURL
  protected val statusUrl = samServiceURL + "/status"

  private def asRawlsSAPipeline[A](implicit um: Unmarshaller[ResponseEntity, A]) = executeRequestWithToken[A](OAuth2BearerToken(getServiceAccountAccessToken)) _

  protected def when401or500(throwable: Throwable): Boolean = {
    throwable match {
      case t: RawlsExceptionWithErrorReport =>
        t.errorReport.statusCode.exists(status => (status.intValue/100 == 5) || status.intValue == 401)
      case _ => false
    }
  }

  private def doSuccessOrFailureRequest(request: HttpRequest, userInfo: UserInfo): RetryableFuture[Unit] = {
    retry(when401or500) { () =>
      httpClientUtils.executeRequest(http, httpClientUtils.addHeader(request, authHeader(userInfo))).flatMap { response =>
        response.status match {
          case s if s.isSuccess => Future(())
          case f =>
            // attempt to propagate an ErrorReport from Sam. If we can't understand Sam's response as an ErrorReport,
            // create our own error message.
            import WorkspaceJsonSupport.ErrorReportFormat
            toFutureTry(Unmarshal(response.entity).to[ErrorReport]) map {
              case Success(err) =>
                logger.error(s"Sam call to ${request.method} ${request.uri.path} failed with error $err")
                throw new RawlsExceptionWithErrorReport(err)
              case Failure(_) =>
                throw new RawlsExceptionWithErrorReport(ErrorReport(f, s"Sam call to ${request.method} ${request.uri.path} failed with error ${response.entity}"))
            }
        }
      }
    }
  }

  override def registerUser(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val url = samServiceURL + "/register/user"
    retry(when401or500) { () =>
      pipeline[Option[UserStatus]](userInfo) apply RequestBuilding.Post(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.Conflict) => None
      }
    }
  }

  override def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]] = {
    val url = samServiceURL + "/register/user"
    retry(when401or500) { () =>
      pipeline[Option[UserStatus]](userInfo) apply RequestBuilding.Get(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
      }
    }
  }

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = {
    val url = samServiceURL + s"/api/google/user/proxyGroup/$targetUserEmail"
    retry(when401or500) { () =>
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

    retry(when401or500) { () => pipeline[Boolean](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicy] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies/${policyName.toLowerCase}"
    val httpRequest = RequestBuilding.Get(url)

    retry(when401or500) { () => pipeline[SamPolicy](userInfo) apply httpRequest }
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
    retry(when401or500) { () => asRawlsSAPipeline[Map[RawlsGroupEmail, Seq[SyncReportItem]]] apply HttpRequest(POST, Uri(url)) }
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}"
    retry(when401or500) { () => pipeline[Set[SamResourceIdWithPolicyName]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]] = {
    val url = samServiceURL + s"/api/resource/${resourceTypeName.value}/$resourceId/policies"
    retry(when401or500) { () => pipeline[Set[SamPolicyWithName]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String] = {
    val url = samServiceURL + s"/api/google/petServiceAccount/$googleProject/${userEmail.value}"
    retry(when401or500) { () => asRawlsSAPipeline[String] apply RequestBuilding.Get(url) }
  }

  override def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String] = {
    val url = samServiceURL + "/api/google/v1/user/petServiceAccount/key"
    retry(when401or500) { () => pipeline[String](userInfo) apply RequestBuilding.Get(url) }
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

    retry(when401or500) { () => pipeline[List[WorkbenchEmail]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getGroupEmail(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[WorkbenchEmail] = {
    val url = samServiceURL + s"/api/group/${groupName.value}"

    retry(when401or500) { () => pipeline[WorkbenchEmail](userInfo) apply RequestBuilding.Get(url) }
  }

  override def listManagedGroups(userInfo: UserInfo): Future[List[ManagedGroupAccessResponse]] = {
    val url = samServiceURL + s"/api/groups"

    retry(when401or500) { () => pipeline[List[ManagedGroupAccessResponse]](userInfo) apply RequestBuilding.Get(url) }
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
