package org.broadinstitute.dsde.rawls.dataaccess

import cats.effect.Async
import cats.effect.kernel.Resource
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsRequestContext, RawlsUser, RawlsUserEmail, SamCreateResourceResponse, SamFullyQualifiedResourceId, SamPolicy, SamPolicySyncStatus, SamPolicyWithNameAndEmail, SamResourceAction, SamResourceIdWithPolicyName, SamResourcePolicyName, SamResourceRole, SamResourceTypeName, SamUserResource, SubsystemStatus, SyncReportItem, UserIdInfo, UserInfo}
import org.broadinstitute.dsde.workbench.client.sam.model.{SyncStatus, UserResourcesResponse}
import org.broadinstitute.dsde.workbench.model._

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
trait SamDAO {
  val errorReportSource = ErrorReportSource("sam")

  def registerUser(userInfo: UserInfo): Future[Option[RawlsUser]]

  def getUserStatus(userInfo: UserInfo): Future[Option[RawlsUser]]

  def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[SamDAO.GetUserIdInfoResult]

  def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail]

  def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit]

  def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String], userInfo: UserInfo, parent: Option[SamFullyQualifiedResourceId]): Future[SamCreateResourceResponse]

  def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit]

  def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean]

  def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicy]

  def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, policy: SamPolicy, userInfo: UserInfo): Future[Unit]

  def overwritePolicyMembership(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberList: Set[WorkbenchEmail], userInfo: UserInfo): Future[Unit]

  def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit]

  def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit]

  def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit]

  def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]]

  def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]]

  def listUserResources(resourceTypeName: SamResourceTypeName, ctx: RawlsRequestContext): Future[Seq[UserResourcesResponse]]

  def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]]

  def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamResourceRole]]

  def listUserActionsForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamResourceAction]]

  def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, ctx: RawlsRequestContext): Future[SyncStatus]

  def getResourceAuthDomain(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Seq[String]]

  def getAccessInstructions(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Option[String]]

  def listAllResourceMemberIds(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[UserIdInfo]]

  /**
    * @return a json blob
    */
  def getPetServiceAccountKeyForUser(googleProject: GoogleProjectId, userEmail: RawlsUserEmail): Future[String]

  def getPetServiceAccountToken(googleProject: GoogleProjectId, scopes: Set[String], userInfo: UserInfo): Future[String]

  def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String]

  def deleteUserPetServiceAccount(googleProject: GoogleProjectId, userInfo: UserInfo): Future[Unit]

  def getStatus(): Future[SubsystemStatus]

  def listResourceChildren(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Seq[SamFullyQualifiedResourceId]]

  def admin: SamAdminDAO
}

trait SamAdminDAO {
  def listPolicies(resourceType: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]]
  def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit]
  def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, memberEmail: String, userInfo: UserInfo): Future[Unit]
}

object SamDAO {
  sealed trait GetUserIdInfoResult extends Product with Serializable
  case object NotFound extends GetUserIdInfoResult
  case object NotUser extends GetUserIdInfoResult
  final case class User(userIdInfo: UserIdInfo) extends GetUserIdInfoResult
  val defaultScopes = Set(
    com.google.api.services.oauth2.Oauth2Scopes.USERINFO_EMAIL,
    com.google.api.services.oauth2.Oauth2Scopes.USERINFO_PROFILE
  )

  implicit class SamExtensions(samDAO: SamDAO) {
    def asResourceAdmin[A, F[_]](resourceTypeName: SamResourceTypeName,
                                 resourceId: String,
                                 policyName: SamResourcePolicyName,
                                 userInfo: UserInfo
                                )
                                (runAsAdmin: => F[A])
                                (implicit F: Async[F]): F[A] = {
      def invoke(f: (SamResourceTypeName, String, SamResourcePolicyName, String, UserInfo) => Future[Unit]) =
        F.fromFuture(F.delay {
          f(resourceTypeName, resourceId, policyName, userInfo.userEmail.value, userInfo)
        })

      Resource
        .make(invoke(samDAO.admin.addUserToPolicy))(_ => invoke(samDAO.admin.removeUserFromPolicy))
        .use(_ => runAsAdmin)
    }
  }
}
