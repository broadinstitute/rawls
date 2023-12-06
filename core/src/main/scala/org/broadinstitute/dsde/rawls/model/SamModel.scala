package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model._
import spray.json.DefaultJsonProtocol._

/*
  Resource type names
 */

case class SamResourceTypeName(value: String) extends ValueObject

object SamResourceTypeNames {
  val billingProject = SamResourceTypeName("billing-project")
  val managedGroup = SamResourceTypeName("managed-group")
  val workspace = SamResourceTypeName("workspace")
  val workflowCollection = SamResourceTypeName("workflow-collection")
  val servicePerimeter = SamResourceTypeName("service-perimeter")
  val googleProject = SamResourceTypeName("google-project")
}

/*
  Resource roles
 */

case class SamResourceRole(value: String) extends ValueObject

object SamWorkspaceRoles {
  val projectOwner = SamResourceRole("project-owner")
  val owner = SamResourceRole("owner")
  val writer = SamResourceRole("writer")
  val reader = SamResourceRole("reader")

  val shareWriter = SamResourceRole("share-writer")
  val shareReader = SamResourceRole("share-reader")
  val canCompute = SamResourceRole("can-compute")
  val canCatalog = SamResourceRole("can-catalog")

  val rolesContainingWritePermissions =
    Set(SamWorkspaceRoles.owner, SamWorkspaceRoles.writer, SamWorkspaceRoles.projectOwner)
}

object SamBillingProjectRoles {
  val workspaceCreator = SamResourceRole("workspace-creator")
  val batchComputeUser = SamResourceRole("batch-compute-user")
  val notebookUser = SamResourceRole("notebook-user")
  val owner = SamResourceRole("owner")
  // if you're looking for a user role, check `def addUserToBillingProject` for the relevant project roles that make up a project user.
}

object SamWorkflowCollectionRoles {
  val owner = SamResourceRole("owner")
  val writer = SamResourceRole("writer")
  val reader = SamResourceRole("reader")
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
  val compute = SamResourceAction("compute")
  val delete = SamResourceAction("delete")
  val migrate = SamResourceAction("migrate")
  val viewMigrationStatus = SamResourceAction("view_migration_status")
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
}

object SamBillingProjectActions {
  val createWorkspace = SamResourceAction("create_workspace")
  val launchBatchCompute = SamResourceAction("launch_batch_compute")
  val alterPolicies = SamResourceAction("alter_policies")
  val readPolicies = SamResourceAction("read_policies")
  val addToServicePerimeter = SamResourceAction("add_to_service_perimeter")
  val alterSpendReportConfiguration = SamResourceAction("alter_spend_report_configuration")
  val readSpendReportConfiguration = SamResourceAction("read_spend_report_configuration")
  val readSpendReport = SamResourceAction("read_spend_report")
  val updateBillingAccount = SamResourceAction("update_billing_account")
  val deleteBillingProject = SamResourceAction("delete")
  val own = SamResourceAction("own")
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
  def readPolicy(policy: SamResourcePolicyName) = SamResourceAction(s"read_policy::${policy.value}")
}

object SamServicePerimeterActions {
  val addProject = SamResourceAction("add_project")
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

object SamGoogleProjectPolicyNames {
  val owner = SamResourcePolicyName("owner")
}

object SamBillingProjectPolicyNames {
  val owner = SamResourcePolicyName("owner")
  val workspaceCreator = SamResourcePolicyName("workspace-creator")
  val canComputeUser = SamResourcePolicyName("can-compute-user")
}

object SamWorkflowCollectionPolicyNames {
  val workflowCollectionOwnerPolicyName = SamResourcePolicyName("workflow-collection-owner")
  val workflowCollectionWriterPolicyName = SamResourcePolicyName("workflow-collection-writer")
  val workflowCollectionReaderPolicyName = SamResourcePolicyName("workflow-collection-reader")
}

case class SamPolicy(memberEmails: Set[WorkbenchEmail], actions: Set[SamResourceAction], roles: Set[SamResourceRole])
case class SamPolicyWithNameAndEmail(policyName: SamResourcePolicyName, policy: SamPolicy, email: WorkbenchEmail)
case class SamResourceWithPolicies(resourceId: String,
                                   policies: Map[SamResourcePolicyName, SamPolicy],
                                   authDomain: Set[String],
                                   returnResource: Boolean = false,
                                   parent: Option[SamFullyQualifiedResourceId] = None
)
case class SamResourceIdWithPolicyName(resourceId: String,
                                       accessPolicyName: SamResourcePolicyName,
                                       authDomainGroups: Set[WorkbenchGroupName],
                                       missingAuthDomainGroups: Set[WorkbenchGroupName],
                                       public: Boolean
)
case class SamPolicySyncStatus(lastSyncDate: String, email: WorkbenchEmail)

case class SamCreateResourceResponse(resourceTypeName: String,
                                     resourceId: String,
                                     authDomain: Set[String],
                                     accessPolicies: Set[SamCreateResourcePolicyResponse]
)
case class SamCreateResourcePolicyResponse(id: SamCreateResourceAccessPolicyIdResponse, email: String)
case class SamCreateResourceAccessPolicyIdResponse(accessPolicyName: String, resource: SamFullyQualifiedResourceId)
case class SamFullyQualifiedResourceId(resourceId: String, resourceTypeName: String)

case class SamRolesAndActions(roles: Set[SamResourceRole], actions: Set[SamResourceAction]) {
  def union(other: SamRolesAndActions) = SamRolesAndActions(roles.union(other.roles), actions.union(other.actions))
}
case class SamUserResource(resourceId: String,
                           direct: SamRolesAndActions,
                           inherited: SamRolesAndActions,
                           public: SamRolesAndActions,
                           authDomainGroups: Set[WorkbenchGroupName],
                           missingAuthDomainGroups: Set[WorkbenchGroupName]
) {
  def hasRole(role: SamResourceRole): Boolean =
    direct.roles.contains(role) || inherited.roles.contains(role) || public.roles.contains(role)

  def allRoles: Set[SamResourceRole] = direct.roles ++ inherited.roles ++ public.roles
}

case class SamUserStatusResponse(userSubjectId: String, userEmail: String, enabled: Boolean)

object SamModelJsonSupport extends JsonSupport {
  implicit val SamFullyQualifiesResourceIdFormat = jsonFormat2(SamFullyQualifiedResourceId)
  implicit val SamResourcePolicyNameFormat = ValueObjectFormat(SamResourcePolicyName)
  implicit val SamResourceActionFormat = ValueObjectFormat(SamResourceAction)
  implicit val SamResourceRoleFormat = ValueObjectFormat(SamResourceRole)

  implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
  implicit val SamPolicyWithNameAndEmailFormat = jsonFormat3(SamPolicyWithNameAndEmail)
  implicit val SamResourceWithPoliciesFormat = jsonFormat5(SamResourceWithPolicies)
  implicit val SamResourceIdWithPolicyNameFormat = jsonFormat5(SamResourceIdWithPolicyName)
  implicit val SamPolicySyncStatusFormat = jsonFormat2(SamPolicySyncStatus)

  implicit val SamCreateResourceAccessPolicyIdResponseFormat = jsonFormat2(SamCreateResourceAccessPolicyIdResponse)
  implicit val samCreateResourcePolicyResponseFormat = jsonFormat2(SamCreateResourcePolicyResponse)
  implicit val SamCreateResourceResponseFormat = jsonFormat4(SamCreateResourceResponse)

  implicit val SamRolesAndActionsFormat = jsonFormat2(SamRolesAndActions)
  implicit val SamUserResourceFormat = jsonFormat6(SamUserResource)
  implicit val SamUserStatusResponseFormat = jsonFormat3(SamUserStatusResponse)
}
