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
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.SamModelJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard, Retry}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport.WorkbenchEmailFormat
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}
import spray.json.DefaultJsonProtocol._
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonReader}

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
          case s if s.isSuccess =>
            response.discardEntityBytes()
            Future(())
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

  override def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicySyncStatus] = {
    val url = samServiceURL + s"/api/google/v1/resource/${resourceTypeName.value}/$resourceId/$policyName/sync"

    retry(when401or500) { () =>
      pipeline[SamPolicySyncStatus](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamResourceRole]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/roles"

    retry(when401or500) { () =>
      pipeline[Set[SamResourceRole]](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], userInfo: UserInfo): Future[SamCreateResourceResponse] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}"

    val httpRequest = RequestBuilding.Post(url, SamResourceWithPolicies(resourceId, policies.map(x => x._1 -> x._2), authDomain, returnResource = true))

    retry(when401or500) { () =>
      pipeline[SamCreateResourceResponse](userInfo) apply httpRequest
    }
  }

  override def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies"

    retry(when401or500) { () =>
      pipeline[Set[SamPolicyWithNameAndEmail]](userInfo) apply RequestBuilding.Get(url)
    }
  }

  override def registerUser(userInfo: UserInfo): Future[Option[RawlsUser]] = {
    val url = samServiceURL + "/register/user/v2/self"
    retry(when401or500) { () =>
      pipeline[Option[RawlsUser]](userInfo) apply RequestBuilding.Post(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.Conflict) => None
      }
    }
  }

  override def getUserStatus(userInfo: UserInfo): Future[Option[RawlsUser]] = {
    val url = samServiceURL + "/register/user/v2/info"
    retry(when401or500) { () =>
      pipeline[Option[RawlsUser]](userInfo) apply RequestBuilding.Get(url) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
      }
    }
  }

  override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[SamDAO.GetUserIdInfoResult] = {
    val url = samServiceURL + s"/api/users/v1/$userEmail"
    val httpRequest = RequestBuilding.Get(url).addHeader(authHeader(userInfo))
    retry(when401or500) { () =>
      httpClientUtils.executeRequestUnmarshalResponseAcceptNoContent[UserIdInfo](http, httpRequest).map {
        case None => SamDAO.NotUser
        case Some(idInfo) => SamDAO.User(idInfo)
      } recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => SamDAO.NotFound
      }
    }
  }

  override def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail] = {
    val url = samServiceURL + s"/api/google/v1/user/proxyGroup/$targetUserEmail"
    retry(when401or500) { () =>
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

    retry(when401or500) { () => pipeline[Boolean](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicy] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}"
    val httpRequest = RequestBuilding.Get(url)

    retry(when401or500) { () => pipeline[SamPolicy](userInfo) apply httpRequest }
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
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails/$memberEmail"
    val httpRequest = RequestBuilding.Put(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/policies/${policyName.value.toLowerCase}/memberEmails/$memberEmail"
    val httpRequest = RequestBuilding.Delete(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/users/v1/invite/$userEmail"
    val httpRequest = RequestBuilding.Post(url)

    doSuccessOrFailureRequest(httpRequest, userInfo)
  }

  override def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]] = {
    val url = samServiceURL + s"/api/google/v1/resource/${resourceTypeName.value}/$resourceId/${policyName.value.toLowerCase}/sync"
    retry(when401or500) { () => asRawlsSAPipeline[Map[WorkbenchEmail, Seq[SyncReportItem]]] apply HttpRequest(POST, Uri(url)) }
  }

  override def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}"
    retry(when401or500) { () => pipeline[Set[SamResourceIdWithPolicyName]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String] = {
    val url = samServiceURL + s"/api/google/v1/petServiceAccount/$googleProject/${userEmail.value}"
    retry(when401or500) { () => asRawlsSAPipeline[String] apply RequestBuilding.Get(url) }
  }

  override def deleteUserPetServiceAccount(googleProject: String, userInfo: UserInfo): Future[Unit] = {
    val url = samServiceURL + s"/api/google/v1/user/petServiceAccount/$googleProject"
    doSuccessOrFailureRequest(RequestBuilding.Delete(url), userInfo)
  }

  override def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String] = {
    val url = samServiceURL + "/api/google/v1/user/petServiceAccount/key"
    retry(when401or500) { () => pipeline[String](userInfo) apply RequestBuilding.Get(url) }
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
    retry(when401or500) { () => pipeline[Seq[String]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def listAllResourceMemberIds(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[UserIdInfo]] = {
    val url = samServiceURL + s"/api/resources/v1/${resourceTypeName.value}/$resourceId/allUsers"
    retry(when401or500) { () => pipeline[Set[UserIdInfo]](userInfo) apply RequestBuilding.Get(url) }
  }

  override def getAccessInstructions(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Option[String]] = {
    val url = samServiceURL + s"/api/groups/v1/${groupName.value}/accessInstructions"
    val httpRequest = RequestBuilding.Get(url).addHeader(authHeader(userInfo))
    retry(when401or500) { () => httpClientUtils.executeRequestUnmarshalResponseAcceptNoContent[String](http, httpRequest) }
  }


}
