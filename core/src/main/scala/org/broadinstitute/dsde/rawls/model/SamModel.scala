package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model._
import spray.json.DefaultJsonProtocol._

/*
  Resource type names
 */

case class SamResourceTypeName(value: String)

object SamResourceTypeNames {
  val billingProject = SamResourceTypeName("billing-project")
  val managedGroup = SamResourceTypeName("managed-group")
  val workspace = SamResourceTypeName("workspace")
  val workflowCollection = SamResourceTypeName("workflow-collection")
  val servicePerimeter = SamResourceTypeName("service-perimeter")
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
}

object SamProjectRoles {
  val workspaceCreator = SamResourceRole("workspace-creator")
  val batchComputeUser = SamResourceRole("batch-compute-user")
  val notebookUser = SamResourceRole("notebook-user")
  val owner = SamResourceRole("owner")
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
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
}

object SamBillingProjectActions {
  val createWorkspace = SamResourceAction("create_workspace")
  val launchBatchCompute = SamResourceAction("launch_batch_compute")
  val alterPolicies = SamResourceAction("alter_policies")
  val readPolicies = SamResourceAction("read_policies")
  val alterGoogleRole = SamResourceAction("alter_google_role")
  val addToServicePerimeter = SamResourceAction("add_to_service_perimeter")
  def sharePolicy(policy: String) = SamResourceAction(s"share_policy::$policy")
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
case class SamResourceWithPolicies(resourceId: String, policies: Map[SamResourcePolicyName, SamPolicy], authDomain: Set[String])
case class SamResourceIdWithPolicyName(resourceId: String, accessPolicyName: SamResourcePolicyName, authDomainGroups: Set[WorkbenchGroupName], missingAuthDomainGroups: Set[WorkbenchGroupName], public: Boolean)
case class SamPolicySyncStatus(lastSyncDate: String, email: WorkbenchEmail)

object SamModelJsonSupport extends JsonSupport {
  implicit val SamResourcePolicyNameFormat = ValueObjectFormat(SamResourcePolicyName)
  implicit val SamResourceActionFormat = ValueObjectFormat(SamResourceAction)
  implicit val SamResourceRoleFormat = ValueObjectFormat(SamResourceRole)

  implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
  implicit val SamPolicyWithNameAndEmailFormat = jsonFormat3(SamPolicyWithNameAndEmail)
  implicit val SamResourceWithPoliciesFormat = jsonFormat3(SamResourceWithPolicies)
  implicit val SamResourceIdWithPolicyNameFormat = jsonFormat5(SamResourceIdWithPolicyName)
  implicit val SamPolicySyncStatusFormat = jsonFormat2(SamPolicySyncStatus)
}
