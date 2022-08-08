package org.broadinstitute.dsde.rawls.workspace

import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.{ActiveSubmission, AgoraEntity, AttributeName, AttributeValue, BucketUsageResponse, GoogleProjectId, GoogleProjectNumber, ManagedGroupAccessInstructions, MetadataParams, MethodConfiguration, MethodConfigurationName, MethodConfigurationNamePair, MethodConfigurationShort, MethodInputsOutputs, MethodRepoConfigurationExport, MethodRepoConfigurationImport, MethodRepoMethod, PendingCloneWorkspaceFileTransfer, RawlsBillingAccountName, RawlsBillingProject, SamResourceAction, SamResourcePolicyName, Submission, SubmissionListResponse, SubmissionReport, SubmissionRequest, SubmissionValidationReport, UserCommentUpdateOperation, UserInfo, ValidatedMethodConfiguration, WorkflowCost, WorkflowOutputs, WorkflowQueueStatusByUserResponse, WorkflowQueueStatusResponse, Workspace, WorkspaceACL, WorkspaceACLUpdate, WorkspaceACLUpdateResponseList, WorkspaceBucketOptions, WorkspaceCatalog, WorkspaceCatalogUpdateResponseList, WorkspaceDetails, WorkspaceFeatureFlag, WorkspaceFieldSpecs, WorkspaceName, WorkspaceRequest, WorkspaceTag}
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationMetadata
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import spray.json.{JsObject, JsValue}

import scala.concurrent.Future

trait WorkspaceServiceTrait {
  // WORKSPACES ORCHESTRATION
  def createWorkspace(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Workspace]
  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Workspace]
  def deleteWorkspace(workspaceName: WorkspaceName, parentSpan: Span = null): Future[Option[String]]
  def getWorkspace(workspaceName: WorkspaceName, params: WorkspaceFieldSpecs, parentSpan: Span = null): Future[JsObject]
  def getWorkspaceById(workspaceId: String, params: WorkspaceFieldSpecs, parentSpan: Span = null): Future[JsObject]
  def listWorkspaces(params: WorkspaceFieldSpecs, parentSpan: Span): Future[JsValue]
  def listAllWorkspaces(): Future[Seq[WorkspaceDetails]]
  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[WorkspaceDetails]
  def adminListWorkspaceFeatureFlags(workspaceName: WorkspaceName): Future[Seq[WorkspaceFeatureFlag]]
  def adminOverwriteWorkspaceFeatureFlags(workspaceName: WorkspaceName, flagNames: List[String]): Future[Seq[WorkspaceFeatureFlag]]
  def getACL(workspaceName: WorkspaceName): Future[WorkspaceACL]
  def updateACL(workspaceName: WorkspaceName, aclUpdates: Set[WorkspaceACLUpdate], inviteUsersNotFound: Boolean): Future[WorkspaceACLUpdateResponseList]
  def lockWorkspace(workspaceName: WorkspaceName): Future[Boolean]
  def unlockWorkspace(workspaceName: WorkspaceName): Future[Boolean]
  def listPendingFileTransfersForWorkspace(workspaceName: WorkspaceName): Future[Seq[PendingCloneWorkspaceFileTransfer]]

  // WORKSPACE TAGS
  def adminListWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue): Future[Seq[WorkspaceDetails]]

  // WORKFLOW SUBMISSIONS
  def countSubmissions(workspaceName: WorkspaceName): Future[Map[String, Int]]
  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[Int]
  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[Int]
  def adminListAllActiveSubmissions(): Future[Seq[ActiveSubmission]]
  def adminWorkflowQueueStatusByUser(): Future[WorkflowQueueStatusByUserResponse]
  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[SubmissionReport]
  def getSubmissionMethodConfiguration(workspaceName: WorkspaceName, submissionId: String): Future[MethodConfiguration]
  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String): Future[Submission]
  def listSubmissions(workspaceName: WorkspaceName): Future[Seq[SubmissionListResponse]]
  def updateSubmissionUserComment(workspaceName: WorkspaceName, submissionId: String, newComment: UserCommentUpdateOperation): Future[Int]
  def validateSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[SubmissionValidationReport]

  // METHODS
  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[ValidatedMethodConfiguration]
  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[ValidatedMethodConfiguration]
  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[AgoraEntity]
  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration]
  def createMethodConfigurationTemplate(methodRepoMethod: MethodRepoMethod): Future[MethodConfiguration]
  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[Boolean]
  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[ValidatedMethodConfiguration]
  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[MethodConfiguration]
  def getMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod): Future[MethodInputsOutputs]
  def listMethodConfigurations(workspaceName: WorkspaceName): Future[List[MethodConfigurationShort]]
  def listAgoraMethodConfigurations(workspaceName: WorkspaceName): Future[List[MethodConfigurationShort]]
  def overwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration]
  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName): Future[MethodConfiguration]
  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration]

  // PPW MIGRATION
  def createGoogleProject(billingProject: RawlsBillingProject, rbsHandoutRequestId: String, span: Span = null): Future[(GoogleProjectId, GoogleProjectNumber)]
  def getWorkspaceMigrationAttempts(workspaceName: WorkspaceName): Future[List[WorkspaceMigrationMetadata]]
  def migrateAll(workspaceNames: Iterable[WorkspaceName]): Future[Iterable[WorkspaceMigrationMetadata]]
  def migrateWorkspace(workspaceName: WorkspaceName): Future[WorkspaceMigrationMetadata]
  def setupGoogleProject(googleProjectId: GoogleProjectId, billingProject: RawlsBillingProject, billingAccount: RawlsBillingAccountName, workspaceId: String, workspaceName: WorkspaceName, span: Span = null): Future[Unit]
  def setupGoogleProjectIam(googleProjectId: GoogleProjectId, policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail], billingProjectOwnerPolicyEmail: WorkbenchEmail, span: Span = null): Future[Unit]

  // WORKFLOWS
  def workflowCost(workspaceName: WorkspaceName, submissionId: String, workflowId: String): Future[WorkflowCost]
  def workflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String, metadataParams: MetadataParams): Future[JsObject]
  def workflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String): Future[WorkflowOutputs]
  def workflowQueueStatus(): Future[WorkflowQueueStatusResponse]

  def checkBucketReadAccess(workspaceName: WorkspaceName): Future[Unit]

  def checkSamActionWithLock(workspaceName: WorkspaceName, samAction: SamResourceAction): Future[Boolean]

  def disableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[Unit]

  def enableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[Unit]

  def getAccessInstructions(workspaceName: WorkspaceName): Future[Seq[ManagedGroupAccessInstructions]]

  def getBucketOptions(workspaceName: WorkspaceName): Future[WorkspaceBucketOptions]

  def getBucketUsage(workspaceName: WorkspaceName): Future[BucketUsageResponse]

  def getCatalog(workspaceName: WorkspaceName): Future[Set[WorkspaceCatalog]]

  def getGenomicsOperationV2(workflowId: String, operationId: List[String]): Future[Option[JsObject]]

  def getTags(query: Option[String], limit: Option[Int] = None): Future[Seq[WorkspaceTag]]


  def sendChangeNotifications(workspaceName: WorkspaceName): Future[String]

  def updateCatalog(workspaceName: WorkspaceName, input: Seq[WorkspaceCatalog]): Future[WorkspaceCatalogUpdateResponseList]

  def updateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[WorkspaceDetails]

}

