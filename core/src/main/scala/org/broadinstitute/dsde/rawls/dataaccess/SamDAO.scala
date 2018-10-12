package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{JsonSupport, ManagedGroupAccessResponse, ManagedRoles, RawlsUserEmail, SubsystemStatus, SyncReportItem, UserIdInfo, UserInfo, UserStatus}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model._
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue, RootJsonFormat}

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

/*
  Resource type names
 */

case class SamResourceTypeName(value: String)

object SamResourceTypeNames {
  val billingProject = SamResourceTypeName("billing-project")
  val managedGroup = SamResourceTypeName("managed-group")
  val workspace = SamResourceTypeName("workspace")
}

/*
  Resource roles
 */

case class SamResourceRole(value: String) extends ValueObject

object SamWorkspaceRoles {
  val owner = SamResourceRole("owner")
  val writer = SamResourceRole("writer")
  val reader = SamResourceRole("reader")
  val shareWriter = SamResourceRole("share-writer")
  val shareReader = SamResourceRole("share-reader")
  val canCompute = SamResourceRole("can-compute")
  val canCatalog = SamResourceRole("can-catalog")
}

object SamProjectRoles {
  val workspaceCreator = SamResourceRole("workspace-creator")
  val batchComputeUser = SamResourceRole("batch-compute-user")
  val notebookUser = SamResourceRole("notebook-user")
  val owner = SamResourceRole("owner")
}

/*
  Resource action
 */

case class SamResourceAction(value: String) extends ValueObject

object SamWorkspaceActions {
  val catalog = SamResourceAction("catalog")
  val own = SamResourceAction("own")
  val write = SamResourceAction("write")
  val read = SamResourceAction("read")
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
}

object SamBillingProjectActions {
  val createWorkspace = SamResourceAction("create_workspace")
  val launchBatchCompute = SamResourceAction("launch_batch_compute")
  val alterPolicies = SamResourceAction("alter_policies")
  val readPolicies = SamResourceAction("read_policies")
  val alterGoogleRole = SamResourceAction("alter_google_role")
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
}

/*
  Resource policy names
 */

case class SamResourcePolicyName(value: String) extends ValueObject

object SamWorkspacePolicyNames {
  val projectOwner = SamResourcePolicyName("project-owner")
  val owner = SamResourcePolicyName("owner")
  val writer = SamResourcePolicyName("writer")
  val reader = SamResourcePolicyName("reader")
  val shareWriter = SamResourcePolicyName("share-writer")
  val shareReader = SamResourcePolicyName("share-reader")
  val canCompute = SamResourcePolicyName("can-compute")
  val canCatalog = SamResourcePolicyName("can-catalog")
}

object SamBillingProjectPolicyNames {
  val owner = SamResourcePolicyName("owner")
  val workspaceCreator = SamResourcePolicyName("workspace-creator")
  val canComputeUser = SamResourcePolicyName("can-compute-user")
}

case class SamPolicy(memberEmails: Set[WorkbenchEmail], actions: Set[SamResourceAction], roles: Set[SamResourceRole])
case class SamPolicyWithName(policyName: SamResourcePolicyName, policy: SamPolicy)
case class SamPolicyWithNameAndEmail(policyName: SamResourcePolicyName, policy: SamPolicy, email: WorkbenchEmail)
case class SamResourceWithPolicies(resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String])
case class SamResourceIdWithPolicyName(resourceId: String, accessPolicyName: SamResourcePolicyName, authDomains: Set[String], missingAuthDomains: Set[String], public: Option[Boolean])
case class SamPolicySyncStatus(lastSyncDate: String, email: WorkbenchEmail)

object SamModelJsonSupport extends JsonSupport {
  implicit val SamResourcePolicyNameFormat = ValueObjectFormat(SamResourcePolicyName)
  implicit val SamResourceActionFormat = ValueObjectFormat(SamResourceAction)
  implicit val SamResourceRoleFormat = ValueObjectFormat(SamResourceRole)

  implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
  implicit val SamPolicyWithNameFormat = jsonFormat2(SamPolicyWithName)
  implicit val SamPolicyWithNameAndEmailFormat = jsonFormat3(SamPolicyWithNameAndEmail)
  implicit val SamResourceWithPoliciesFormat = jsonFormat3(SamResourceWithPolicies)
  implicit val SamResourceIdWithPolicyNameFormat = jsonFormat5(SamResourceIdWithPolicyName)
  implicit val SamPolicySyncStatusFormat = jsonFormat2(SamPolicySyncStatus)
}
