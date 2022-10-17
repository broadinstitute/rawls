package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, WorkbenchEmail}

import java.util.UUID

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): WorkspaceDescription
  def createWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): CreatedWorkspace
  def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                      displayName: String,
                                      spendProfileId: String,
                                      ctx: RawlsRequestContext
  ): CreatedWorkspace
  def createAzureWorkspaceCloudContext(workspaceId: UUID,
                                       azureTenantId: String,
                                       azureResourceGroupId: String,
                                       azureSubscriptionId: String,
                                       ctx: RawlsRequestContext
  ): CreateCloudContextResult
  def getWorkspaceCreateCloudContextResult(workspaceId: UUID,
                                           jobControlId: String,
                                           ctx: RawlsRequestContext
  ): CreateCloudContextResult
  def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit
  def createDataRepoSnapshotReference(workspaceId: UUID,
                                      snapshotId: UUID,
                                      name: DataReferenceName,
                                      description: Option[DataReferenceDescriptionField],
                                      instanceName: String,
                                      cloningInstructions: CloningInstructionsEnum,
                                      ctx: RawlsRequestContext
  ): DataRepoSnapshotResource
  def updateDataRepoSnapshotReference(workspaceId: UUID,
                                      referenceId: UUID,
                                      updateInfo: UpdateDataRepoSnapshotReferenceRequestBody,
                                      ctx: RawlsRequestContext
  ): Unit
  def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, ctx: RawlsRequestContext): Unit
  def getDataRepoSnapshotReference(workspaceId: UUID,
                                   referenceId: UUID,
                                   ctx: RawlsRequestContext
  ): DataRepoSnapshotResource
  def getDataRepoSnapshotReferenceByName(workspaceId: UUID,
                                         refName: DataReferenceName,
                                         ctx: RawlsRequestContext
  ): DataRepoSnapshotResource
  def enumerateDataRepoSnapshotReferences(workspaceId: UUID,
                                          offset: Int,
                                          limit: Int,
                                          ctx: RawlsRequestContext
  ): ResourceList
  def enableApplication(workspaceId: UUID,
                        applicationId: String,
                        ctx: RawlsRequestContext
  ): WorkspaceApplicationDescription
  def createAzureRelay(workspaceId: UUID,
                       region: String,
                       ctx: RawlsRequestContext
  ): CreateControlledAzureRelayNamespaceResult
  def getCreateAzureRelayResult(workspaceId: UUID,
                                jobControlId: String,
                                ctx: RawlsRequestContext
  ): CreateControlledAzureRelayNamespaceResult
  def createAzureStorageAccount(workspaceId: UUID,
                                region: String,
                                ctx: RawlsRequestContext
  ): CreatedControlledAzureStorage
  def createAzureStorageContainer(workspaceId: UUID,
                                  storageAccountId: UUID,
                                  ctx: RawlsRequestContext
  ): CreatedControlledAzureStorageContainer

  def getRoles(workspaceId: UUID, ctx: RawlsRequestContext): RoleBindingList

  def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  def cloneWorkspace(sourceWorkspaceId: UUID, destinationWorkspaceContext: Workspace, ctx: RawlsRequestContext): CloneWorkspaceResult
}
