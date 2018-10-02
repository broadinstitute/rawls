package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{JsonSupport, ManagedGroupAccessResponse, ManagedRoles, RawlsUserEmail, SubsystemStatus, SyncReportItem, UserIdInfo, UserInfo, UserStatus}
import spray.json.DefaultJsonProtocol._
import org.broadinstitute.dsde.workbench.model.{ErrorReport, ErrorReportSource, WorkbenchEmail, WorkbenchExceptionWithErrorReport, WorkbenchGroupName}

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
  def createResourceFull(resourceTypeName: SamResourceTypeName, resourceId: String, policies: Map[String, SamPolicy], authDomain: Set[String], userInfo: UserInfo): Future[Unit]
  def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit]
  def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean]
  def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicy]
  def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Unit]
  def overwritePolicyMembership(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberList: Set[WorkbenchEmail], userInfo: UserInfo): Future[Unit]
  def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit]
  def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit]
  def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit]
  def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String): Future[Map[WorkbenchEmail, Seq[SyncReportItem]]]
  def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]]
  def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]]
  def listPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]]
  def listUserPoliciesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]]
  def listUserRolesForResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[String]]
  def getPolicySyncStatus(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicySyncStatus]

  @deprecated
  def createGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit]
  @deprecated
  def deleteGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit]

  @deprecated
  def listGroupPolicyEmails(groupName: WorkbenchGroupName, policyName: ManagedRoles.ManagedRole, userInfo: UserInfo): Future[List[WorkbenchEmail]]
  @deprecated
  def getGroupEmail(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[WorkbenchEmail]
  @deprecated
  def listManagedGroups(userInfo: UserInfo): Future[List[ManagedGroupAccessResponse]]

  @deprecated
  def addUserToManagedGroup(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmail: WorkbenchEmail, userInfo: UserInfo): Future[Unit]
  @deprecated
  def removeUserFromManagedGroup(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmail: WorkbenchEmail, userInfo: UserInfo): Future[Unit]
  @deprecated
  def overwriteManagedGroupMembership(groupName: WorkbenchGroupName, role: ManagedRoles.ManagedRole, memberEmails: Seq[WorkbenchEmail], userInfo: UserInfo): Future[Unit]

  @deprecated
  def requestAccessToManagedGroup(groupName: WorkbenchGroupName, userInfo: UserInfo): Future[Unit]

  /**
    * @return a json blob
    */
  def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String]
  def getDefaultPetServiceAccountKeyForUser(userInfo: UserInfo): Future[String]

  def getStatus(): Future[SubsystemStatus]
}

object SamResourceActions {
  case class SamResourceAction(value: String)

  val createWorkspace = SamResourceAction("create_workspace")
  val launchBatchCompute = SamResourceAction("launch_batch_compute")
  val alterPolicies = SamResourceAction("alter_policies")
  val readPolicies = SamResourceAction("read_policies")
  val alterGoogleRole = SamResourceAction("alter_google_role")
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
  val workspaceCanCatalog = SamResourceAction("can_catalog")
  val workspaceOwn = SamResourceAction("own")
  val workspaceWrite = SamResourceAction("write")
  val workspaceRead = SamResourceAction("read")
}

object SamResourceTypeNames {
  case class SamResourceTypeName(value: String)

  val billingProject = SamResourceTypeName("billing-project")
  val managedGroup = SamResourceTypeName("managed-group")
  val workspace = SamResourceTypeName("workspace")
}

trait SamResourceRoles
trait SamResourcePolicyNames

object SamProjectRoles extends SamResourceRoles {
  val workspaceCreator = "workspace-creator"
  val batchComputeUser = "batch-compute-user"
  val notebookUser = "notebook-user"
  val owner = "owner"
}

object SamWorkspacePolicyNames extends SamResourcePolicyNames {
  case class SamWorkspacePolicyName(value: String)

  val owner = SamWorkspacePolicyName("owner")
  val writer = SamWorkspacePolicyName("writer")
  val reader = SamWorkspacePolicyName("reader")
  val shareWriter = SamWorkspacePolicyName("share-writer")
  val shareReader = SamWorkspacePolicyName("share-reader")
  val canCompute = SamWorkspacePolicyName("can-compute")
}

case class SamPolicy(memberEmails: Set[String], actions: Set[String], roles: Set[String])
case class SamPolicyWithName(policyName: String, policy: SamPolicy)
case class SamPolicyWithNameAndEmail(policyName: String, policy: SamPolicy, email: String)
case class SamResourceWithPolicies(resourceId: String, policies: Map[String, SamPolicy], authDomain: Set[String])
case class SamResourceIdWithPolicyName(resourceId: String, accessPolicyName: String)
case class SamPolicySyncStatus(lastSyncDate: String, email: String)

object SamModelJsonSupport extends JsonSupport {
  implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
  implicit val SamPolicyWithNameFormat = jsonFormat2(SamPolicyWithName)
  implicit val SamPolicyWithNameAndEmailFormat = jsonFormat3(SamPolicyWithNameAndEmail)
  implicit val SamResourceWithPoliciesFormat = jsonFormat3(SamResourceWithPolicies)
  implicit val SamResourceIdWithPolicyNameFormat = jsonFormat2(SamResourceIdWithPolicyName)
  implicit val SamPolicySyncStatusFormat = jsonFormat2(SamPolicySyncStatus)
}
