package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, RawlsRequestContext}
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

  def cloneWorkspace(sourceWorkspaceId: UUID,
                     workspaceId: UUID,
                     displayName: String,
                     spendProfileId: UUID,
                     location: String,
                     ctx: RawlsRequestContext
  ): CloneWorkspaceResult

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

  /**
    * Creates an Azure storage container in the workspace.
    *
    * @param workspaceId the UUID of the workspace
    * @param storageAccountId optional UUID of a storage account resource. If not specified, the storage
    *                         account from the workspace's landing zone will be used
    * @param ctx Raws context
    * @return the response from workspace manager
    */
  def createAzureStorageContainer(workspaceId: UUID,
                                  storageAccountId: Option[UUID],
                                  ctx: RawlsRequestContext
  ): CreatedControlledAzureStorageContainer

  def getRoles(workspaceId: UUID, ctx: RawlsRequestContext): RoleBindingList

  def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  def createLandingZone(definition: String,
                        version: String,
                        billingProfileId: UUID,
                        ctx: RawlsRequestContext
  ): CreateLandingZoneResult

  def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult

  def deleteLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): DeleteAzureLandingZoneResult

  @throws(classOf[ApiException])
  def throwWhenUnavailable(): Unit
}
