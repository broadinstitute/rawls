package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.SamResourceActions.SamResourceAction
import org.broadinstitute.dsde.rawls.dataaccess.SamResourceTypeNames.SamResourceTypeName
import org.broadinstitute.dsde.rawls.model.{ErrorReportSource, ErrorReportable, JsonSupport, RawlsGroupEmail, RawlsUserEmail, SubsystemStatus, SyncReportItem, UserInfo, UserStatus}
import spray.json.DefaultJsonProtocol._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.Future

/**
  * Created by mbemis on 9/11/17.
  */
trait SamDAO extends ErrorReportable {
  val errorReportSource = ErrorReportSource("sam")
  def registerUser(userInfo: UserInfo): Future[Option[UserStatus]]
  def getUserStatus(userInfo: UserInfo): Future[Option[UserStatus]]
  def getProxyGroup(userInfo: UserInfo, targetUserEmail: WorkbenchEmail): Future[WorkbenchEmail]
  def createResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit]
  def deleteResource(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Unit]
  def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean]
  def getPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, userInfo: UserInfo): Future[SamPolicy]
  def overwritePolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, policy: SamPolicy, userInfo: UserInfo): Future[Unit]
  def addUserToPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit]
  def removeUserFromPolicy(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String, memberEmail: String, userInfo: UserInfo): Future[Unit]
  def syncPolicyToGoogle(resourceTypeName: SamResourceTypeName, resourceId: String, policyName: String): Future[Map[RawlsGroupEmail, Seq[SyncReportItem]]]
  def getPoliciesForType(resourceTypeName: SamResourceTypeName, userInfo: UserInfo): Future[Set[SamResourceIdWithPolicyName]]
  def getResourcePolicies(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithName]]

  /**
    * @return a json blob
    */
  def getPetServiceAccountKeyForUser(googleProject: String, userEmail: RawlsUserEmail): Future[String]

  def getStatus(): Future[SubsystemStatus]
}

object SamResourceActions {
  case class SamResourceAction(value: String)

  val createWorkspace = SamResourceAction("create_workspace")
  val launchBatchCompute = SamResourceAction("launch_batch_compute")
  val alterPolicies = SamResourceAction("alter_policies")
  val readPolicies = SamResourceAction("read_policies")
  val alterGoogleRole = SamResourceAction("alter_google_role")
}

object SamResourceTypeNames {
  case class SamResourceTypeName(value: String)

  val billingProject = SamResourceTypeName("billing-project")
}

trait SamResourceRoles

object SamProjectRoles extends SamResourceRoles {
  val workspaceCreator = "workspace-creator"
  val batchComputeUser = "batch-compute-user"
  val notebookUser = "notebook-user"
  val owner = "owner"
}

case class SamPolicy(memberEmails: Seq[String], actions: Seq[String], roles: Seq[String])
case class SamPolicyWithName(policyName: String, policy: SamPolicy)
case class SamResourceIdWithPolicyName(resourceId: String, accessPolicyName: String)

object SamModelJsonSupport extends JsonSupport {
  implicit val SamPolicyFormat = jsonFormat3(SamPolicy)
  implicit val SamPolicyWithNameFormat = jsonFormat2(SamPolicyWithName)
  implicit val SamResourceIdWithPolicyNameFormat = jsonFormat2(SamResourceIdWithPolicyName)
}
