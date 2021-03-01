package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsUser, RawlsUserEmail, SamCreateResourceResponse, SamFullyQualifiedResourceId, SamPolicy, SamPolicySyncStatus, SamPolicyWithNameAndEmail, SamResourceAction, SamResourceIdWithPolicyName, SamResourcePolicyName, SamResourceRole, SamResourceTypeName, SamUserResource, SubsystemStatus, SyncReportItem, UserIdInfo, UserInfo}
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

  def listUserResources(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Seq[SamUserResource]]

  def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]]

  def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamResourceRole]]

  def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicySyncStatus]

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
}

object SamDAO {
  sealed trait GetUserIdInfoResult extends Product with Serializable
  case object NotFound extends GetUserIdInfoResult
  case object NotUser extends GetUserIdInfoResult
  final case class User(userIdInfo: UserIdInfo) extends GetUserIdInfoResult
  val defaultScopes = Set(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile"
  )
  val bigQueryScope = "https://www.googleapis.com/auth/bigquery.readonly"
}
