package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.stream.Materializer
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.SamModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard, Retry}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport.WorkbenchEmailFormat
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}
import spray.json.DefaultJsonProtocol._
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by mbemis on 9/11/17.
  */
class HttpSamDAO(baseSamServiceURL: String, serviceAccountCreds: Credential)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
  extends SamDAO with DsdeHttpDAO with Retry with LazyLogging with ServiceDAOWithStatus with FutureSupport {

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsStandard()

  private val samServiceURL = baseSamServiceURL
  protected val statusUrl = samServiceURL + "/status"

  private def asRawlsSAPipeline[A](implicit um: Unmarshaller[ResponseEntity, A]) = executeRequestWithToken[A](OAuth2BearerToken(getServiceAccountAccessToken)) _

  protected def when401or5xx: Predicate[Throwable] = anyOf(when5xx, DsdeHttpDAO.whenUnauthorized)

  private def doSuccessOrFailureRequest(request: HttpRequest, userInfo: UserInfo): RetryableFuture[Unit] = {
    retry(when401or5xx) { () =>
      httpClientUtils.executeRequest(http, httpClientUtils.addHeader(request, authHeader(userInfo))).flatMap { response =>
        response.status match {
          case s if s.isSuccess =>
            response.discardEntityBytes()
            Future(())
          case f =>
            // attempt to propagate an ErrorReport from Sam. If we can't understand Sam's response as an ErrorReport,
            // create our own error message.
            import WorkspaceJsonSupport.ErrorReportFormat
            toFutureTry(Unmarshal(response.entity).to[ErrorReport]) flatMap {
              case Success(err) =>
                logger.error(s"Sam call to ${request.method} ${request.uri.path} failed with error $err")
                throw new RawlsExceptionWithErrorReport(err)
              case Failure(_) =>
                // attempt to extract something useful from the response entity, even though it's not an ErrorReport
                toFutureTry(Unmarshal(response.entity).to[String]) map { maybeString =>
                  val stringErrMsg = maybeString match {
                    case Success(stringErr) => stringErr
                    case Failure(_) => response.entity.toString
                  }
                  throw new RawlsExceptionWithErrorReport(ErrorReport(f, s"Sam call to ${request.method} ${request.uri.path} failed with error '$stringErrMsg'"))
                }
            }
        }
      }
    }
  }

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicySyncStatus] = {
    val url = samServiceURL + s"/api/google/v1/resource/${resourceTypeName.value}/$resourceId/$policyName/sync"

    retry(when401or5xx) { () =>
      pipeline[SamPolicySyncStatus](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamResourceRole]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/roles"

    retry(when401or5xx) { () =>
      pipeline[Set[SamResourceRole]](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def listUserActionsForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamResourceAction]] = {
    val url = samServiceURL + s"/api/resources/v2/${resourceTypeName.value}/$resourceId/actions"

    retry(when401or5xx) { () =>
      pipeline[Set[SamResourceAction]](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], userInfo: UserInfo, parent: Option[SamFullyQualifiedResourceId]): Future[SamCreateResourceResponse] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}"

    val httpRequest = RequestBuilding.Post(url, SamResourceWithPolicies(resourceId, policies.map(x => x._1 -> x._2), authDomain, returnResource = true, parent = parent))

    retry(when401or5xx) { () =>
      pipeline[SamCreateResourceResponse](userInfo) apply httpRequest
    }
  }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies"

    retry(when401or5xx) { () =>
      pipeline[Set[SamPolicyWithNameAndEmail]](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def registerUser(userInfo: UserInfo): Future[Option[RawlsUser]] = {
    val url = samServiceURL + "/register/user/v2/self"
    retry(when401or5xx) { () =>
      pipeline[Option[RawlsUser]](userInfo) apply RequestBuilding.Post(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.Conflict) => None
      }
    }
  }

  override def getUserStatus(userInfo: UserInfo): Future[Option[RawlsUser]] = {
    val url = samServiceURL + "/register/user/v2/info"
    retry(when401or5xx) { () =>
      pipeline[Option[RawlsUser]](userInfo) apply RequestBuilding.Get(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
      }
    }
  }

  override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[SamDAO.GetUserIdInfoResult] = {
    val url = samServiceURL + s"/api/users/v1/${URLEncoder.encode(userEmail, UTF_8.name)}"
    val httpRequest = RequestBuilding.Get(url).addHeader(authHeader(userInfo))
    retry(when401or5xx) { () =>
      httpClientUtils.executeRequestUnmarshalResponseAcceptNoContent[UserIdInfo](http, httpRequest).map {
        case None => SamDAO.NotUser
        case Some(idInfo) => SamDAO.User(idInfo)
      } recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => SamDAO.NotFound
      }
    }
  }

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = {
    val url = samServiceURL + s"/api/google/v1/user/proxyGroup/${URLEncoder.encode(targetUserEmail.value, UTF_8.name)}"
    retry(when401or5xx) { () =>
      pipeline[WorkbenchEmail](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId"
    val httpRequest = HttpRequest(POST, Uri(url))

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
    // special RootJsonReader because DefaultJsonProtocol.BooleanJsonFormat is not root and the implicit
    // conversion to an Unmarshaller needs a root
    implicit val rootJsBooleanReader = new RootJsonReader[Boolean] {
      override def read(json: JsValue): Boolean = DefaultJsonProtocol.BooleanJsonFormat.read(json)
    }

    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/action/${action.value}"

    retry(when401or5xx) { () => pipeline[Boolean](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicy] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}"
    val httpRequest = RequestBuilding.Get(url)

    retry(when401or5xx) { () => pipeline[SamPolicy](userInfo) apply httpRequest }
  }

  override def listResourceChildren(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Seq[SamFullyQualifiedResourceId]] = {
    val url = samServiceURL + s"/api/resources/v2/${resourceTypeName.value}/$resourceId/children"
    val httpRequest = RequestBuilding.Get(url)

    retry(when401or5xx) { () => pipeline[Seq[SamFullyQualifiedResourceId]](userInfo) apply httpRequest }
  }

  override def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}"

    Marshal(policy).to[RequestEntity].flatMap { policyEntity =>
      val httpRequest = HttpRequest(PUT, Uri(url), entity = policyEntity)
      doSuccessOrFailureRequest(httpRequest, userInfo)
    }
  }

  override def overwritePolicyMembership(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberList: Set[WorkbenchEmail], userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails"

    Marshal(memberList).to[RequestEntity].flatMap { membershipEntity =>
      val httpRequest = HttpRequest(PUT, Uri(url), entity = membershipEntity)
      doSuccessOrFailureRequest(httpRequest, userInfo)
    }
  }

  override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails/${URLEncoder.encode(memberEmail, UTF_8.name)}"
    val httpRequest = RequestBuilding.Put(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails/${URLEncoder.encode(memberEmail, UTF_8.name)}"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/users/v1/invite/${URLEncoder.encode(userEmail, UTF_8.name)}"
    val httpRequest = RequestBuilding.Post(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] = {
    val url = samServiceURL + s"/api/google/v1/resource/${resourceTypeName.value}/$resourceId/${policyName.value.toLowerCase}/sync"
    retry(when401or5xx) { () => asRawlsSAPipeline[Map[WorkbenchEmail, Seq[SyncReportItem]]] apply HttpRequest(POST, Uri(url)) }
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}"
    retry(when401or5xx) { () => pipeline[Set[SamResourceIdWithPolicyName]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def listUserResources(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Seq[SamUserResource]] = {
    val url = samServiceURL + s"/api/resources/v2/${resourceTypeName.value}"
    retry(when401or5xx) { () => pipeline[Seq[SamUserResource]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPetServiceAccountKeyForUser(googleProject: GoogleProjectId, userEmail: RawlsUserEmail): Future[String] = {
    val url = samServiceURL + s"/api/google/v1/petServiceAccount/${googleProject.value}/${URLEncoder.encode(userEmail.value, UTF_8.name)}"
    retryUntilSuccessOrTimeout(when401or5xx)(interval = 5.seconds, timeout = 55.seconds) { () => asRawlsSAPipeline[String] apply RequestBuilding.Get(url) }
  }

  override def deleteUserPetServiceAccount(googleProject: GoogleProjectId, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/google/v1/user/petServiceAccount/${googleProject.value}"
    doSuccessOrFailureRequest(RequestBuilding.Delete(url), userInfo)
  }

  override def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String] = {
    val url = samServiceURL + "/api/google/v1/user/petServiceAccount/key"
    retry(when401or5xx) { () => pipeline[String](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPetServiceAccountToken(googleProject: GoogleProjectId, scopes: Set[String], userInfo: UserInfo): Future[String] = {
    val url = samServiceURL + s"/api/google/v1/user/petServiceAccount/${googleProject.value}/token"
    retry(when401or5xx) { () => pipeline[String](userInfo) apply RequestBuilding.Post(url, scopes) }
  }

  private def getServiceAccountAccessToken = {
    val expiresInSeconds = Option(serviceAccountCreds.getExpiresInSeconds).map(_.longValue()).getOrElse(0L)
    if (expiresInSeconds < 60*5) {
      serviceAccountCreds.refreshToken()
    }
    serviceAccountCreds.getAccessToken
  }

  override def getResourceAuthDomain(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Seq[String]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/authDomain"
    retry(when401or5xx) { () => pipeline[Seq[String]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def listAllResourceMemberIds(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[UserIdInfo]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/allUsers"
    retry(when401or5xx) { () => pipeline[Set[UserIdInfo]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getAccessInstructions(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Option[String]] = {
    val url = samServiceURL + s"/api/groups/v1/${groupName.value}/accessInstructions"
    val httpRequest = RequestBuilding.Get(url).addHeader(authHeader(userInfo))
    retry(when401or5xx) { () => httpClientUtils.executeRequestUnmarshalResponseAcceptNoContent[String](http, httpRequest) }
  }

  override def admin: SamAdminDAO = new SamAdminDAO {
    override def listPolicies(resourceType: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
      val url = samServiceURL + s"/api/admin/v1/resources/$resourceType/$resourceId/policies"
      retry(when401or5xx) { () => pipeline[Set[SamPolicyWithNameAndEmail]](userInfo) apply RequestBuilding.Get(url) }
    }

    override def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
      val url = samServiceURL + s"/api/admin/v1/resources/$resourceTypeName/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails/${URLEncoder.encode(memberEmail, UTF_8.name)}"
      doSuccessOrFailureRequest(RequestBuilding.Put(url), userInfo)
    }

    override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
      val url = samServiceURL + s"/api/admin/v1/resources/$resourceTypeName/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails/${URLEncoder.encode(memberEmail, UTF_8.name)}"
      doSuccessOrFailureRequest(RequestBuilding.Delete(url), userInfo)
    }
  }
}
