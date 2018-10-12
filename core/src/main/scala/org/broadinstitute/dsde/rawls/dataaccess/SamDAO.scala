package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, SamPolicy, SamPolicySyncStatus, SamPolicyWithName, SamPolicyWithNameAndEmail, SamResourceAction, SamResourceIdWithPolicyName, SamResourcePolicyName, SamResourceTypeName, SubsystemStatus, SyncReportItem, UserIdInfo, UserInfo, UserStatus}
import org.broadinstitute.dsde.workbench.model._

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
trait SamDAO {
  val errorReportSource = ErrorReportSource("sam")
  def registerUser(userInfo: UserInfo): Future[Option[UserStatus]]
  def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]]
  def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[Either[Unit, Option[UserIdInfo]]]
  def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail]
  def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit]
  def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[_ <: SamResourcePolicyName, SamPolicy], authDomain: Set[String], userInfo: UserInfo): Future[Unit]
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
  def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]]
  def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]]
  def listUserPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]]
  def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[String]]
  def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: SamResourcePolicyName, userInfo: UserInfo): Future[SamPolicySyncStatus]

  @deprecated
  def requestAccessToManagedGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit]

  /**
    * @return a json blob
    */
  def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String]
  def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String]

  def getStatus(): Future[SubsystemStatus]
}
