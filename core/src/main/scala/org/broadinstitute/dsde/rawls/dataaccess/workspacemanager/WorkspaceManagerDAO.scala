package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.profile.model.ProfileModel
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
                     spendProfile: ProfileModel,
                     ctx: RawlsRequestContext,
                     location: Option[String] = None
  ): CloneWorkspaceResult

  def getJob(jobControlId: String, ctx: RawlsRequestContext): JobReport

  def getCloneWorkspaceResult(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): CloneWorkspaceResult

  def createAzureWorkspaceCloudContext(workspaceId: UUID, ctx: RawlsRequestContext): CreateCloudContextResult

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
  def disableApplication(workspaceId: UUID,
                         applicationId: String,
                         ctx: RawlsRequestContext
  ): WorkspaceApplicationDescription
  def createAzureStorageAccount(workspaceId: UUID,
                                region: String,
                                ctx: RawlsRequestContext
  ): CreatedControlledAzureStorage

  /**
    * Creates an Azure storage container in the workspace.
    *
    * @param workspaceId the UUID of the workspace
    * @param storageContainerName the name of the new container
    * @param storageAccountId optional UUID of a storage account resource. If not specified, the storage
    *                         account from the workspace's landing zone will be used
    * @param ctx Rawls context
    * @return the response from workspace manager
    */
  def createAzureStorageContainer(workspaceId: UUID,
                                  storageContainerName: String,
                                  storageAccountId: Option[UUID],
                                  ctx: RawlsRequestContext
  ): CreatedControlledAzureStorageContainer

  /**
    * Clone the storage container from one workspace to another.
    *
    * @param sourceWorkspaceId the UUID of the source workspace
    * @param destinationWorkspaceId the UUID of the destination workspace
    * @param sourceContainerId the UUID of the source container to clone
    * @param destinationContainerName the name for the created container in the destination workspace
    * @param cloningInstructions the cloning instructions to use. Note that this will override the cloning
    *                            instructions of the source container for the purposes of this cloning operation;
    *                            however, the cloned container's cloning instructions will be the same as the
    *                            original source container's.
    * @param Rawls context
    * @return the response from workspace manager
    */
  def cloneAzureStorageContainer(sourceWorkspaceId: UUID,
                                 destinationWorkspaceId: UUID,
                                 sourceContainerId: UUID,
                                 destinationContainerName: String,
                                 cloningInstructions: CloningInstructionsEnum,
                                 ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult

  /**
    * Get the job result from a storage container clone operation.
    *
    * @param workspaceId the UUID of the workspace that is being cloned into
    * @param jobId the jobID of the container clone operation
    * @param Rawls context
    * @return the response from workspace manager
    */
  def getCloneAzureStorageContainerResult(workspaceId: UUID,
                                          jobId: String,
                                          ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult

  /**
    * Get the storage containers for the specified workspace.
    *
    * @param workspaceId the UUID of the workspace
    * @param offset starting index
    * @param limit number to return
    * @param Rawls context
    * @return the response from workspace manager
    */
  def enumerateStorageContainers(workspaceId: UUID, offset: Int, limit: Int, ctx: RawlsRequestContext): ResourceList

  def getRoles(workspaceId: UUID, ctx: RawlsRequestContext): RoleBindingList

  def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  def createLandingZone(definition: String,
                        version: String,
                        landingZoneParameters: Map[String, String],
                        billingProfileId: UUID,
                        ctx: RawlsRequestContext,
                        landingZoneId: Option[UUID] = None
                       ): CreateLandingZoneResult

  def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult

  def deleteLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): DeleteAzureLandingZoneResult

  def getDeleteLandingZoneResult(jobId: String,
                                 landingZoneId: UUID,
                                 ctx: RawlsRequestContext
  ): DeleteAzureLandingZoneJobResult

  @throws(classOf[ApiException])
  def throwWhenUnavailable(): Unit
}
