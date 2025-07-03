package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import com.google.common.annotations.VisibleForTesting
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, WorkbenchEmail}

import java.util.UUID
import scala.annotation.unused

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  @unused
  def getWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): WorkspaceDescription
  @unused
  def createWorkspace(workspaceId: UUID,
                      workspaceType: WorkspaceType,
                      policyInputs: Option[WsmPolicyInputs],
                      ctx: RawlsRequestContext
  ): CreatedWorkspace
  @VisibleForTesting
  def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                      displayName: String,
                                      spendProfileId: String,
                                      billingProjectNamespace: String,
                                      applicationIds: Seq[String],
                                      cloudPlatform: CloudPlatform,
                                      policyInputs: Option[WsmPolicyInputs],
                                      ctx: RawlsRequestContext
  ): CreateWorkspaceV2Result

  @unused
  def getCreateWorkspaceResult(jobControlId: String, ctx: RawlsRequestContext): CreateWorkspaceV2Result

  @VisibleForTesting
  def cloneWorkspace(sourceWorkspaceId: UUID,
                     workspaceId: UUID,
                     displayName: String,
                     spendProfile: Option[ProfileModel],
                     billingProjectNamespace: String,
                     ctx: RawlsRequestContext,
                     additionalPolicyInputs: Option[WsmPolicyInputs] = None
  ): CloneWorkspaceResult

  @unused
  def getJob(jobControlId: String, ctx: RawlsRequestContext): JobReport

  @unused
  def getCloneWorkspaceResult(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): CloneWorkspaceResult

  @unused
  def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit

  @VisibleForTesting
  def deleteWorkspaceV2(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): JobResult

  @VisibleForTesting
  def getDeleteWorkspaceV2Result(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): JobResult

  @unused
  def updateWorkspacePolicies(workspaceId: UUID,
                              policyInputs: WsmPolicyInputs,
                              ctx: RawlsRequestContext
  ): WsmPolicyUpdateResult

  @VisibleForTesting
  def createDataRepoSnapshotReference(workspaceId: UUID,
                                      snapshotId: UUID,
                                      name: DataReferenceName,
                                      description: Option[DataReferenceDescriptionField],
                                      instanceName: String,
                                      cloningInstructions: CloningInstructionsEnum,
                                      properties: Option[Map[String, String]],
                                      ctx: RawlsRequestContext
  ): DataRepoSnapshotResource
  @unused
  def updateDataRepoSnapshotReference(workspaceId: UUID,
                                      referenceId: UUID,
                                      updateInfo: UpdateDataRepoSnapshotReferenceRequestBody,
                                      ctx: RawlsRequestContext
  ): Unit
  @unused
  def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, ctx: RawlsRequestContext): Unit
  @unused
  def getDataRepoSnapshotReference(workspaceId: UUID,
                                   referenceId: UUID,
                                   ctx: RawlsRequestContext
  ): DataRepoSnapshotResource
  @unused
  def getDataRepoSnapshotReferenceByName(workspaceId: UUID,
                                         refName: DataReferenceName,
                                         ctx: RawlsRequestContext
  ): DataRepoSnapshotResource
  @unused
  def enumerateDataRepoSnapshotReferences(workspaceId: UUID,
                                          offset: Int,
                                          limit: Int,
                                          ctx: RawlsRequestContext
  ): ResourceList

  /**
    * Creates an Azure storage container in the workspace. This container will be created in the workspace's
    * parent landing zone.
    *
    * @param workspaceId the UUID of the workspace
    * @param storageContainerName the name of the new container
    * @param ctx Rawls context
    * @return the response from workspace manager
    */
  @VisibleForTesting
  def createAzureStorageContainer(workspaceId: UUID,
                                  storageContainerName: String,
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
  @VisibleForTesting
  def cloneAzureStorageContainer(sourceWorkspaceId: UUID,
                                 destinationWorkspaceId: UUID,
                                 sourceContainerId: UUID,
                                 destinationContainerName: String,
                                 cloningInstructions: CloningInstructionsEnum,
                                 prefixToClone: Option[String],
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
  @unused
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
  @VisibleForTesting
  def enumerateStorageContainers(workspaceId: UUID, offset: Int, limit: Int, ctx: RawlsRequestContext): ResourceList

  @VisibleForTesting
  def getRoles(workspaceId: UUID, ctx: RawlsRequestContext): RoleBindingList

  @VisibleForTesting
  def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  @VisibleForTesting
  def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit

  // TODO CORE-501: in use
  def createLandingZone(definition: String,
                        version: String,
                        landingZoneParameters: Map[String, String],
                        billingProfileId: UUID,
                        ctx: RawlsRequestContext,
                        landingZoneId: Option[UUID] = None
  ): CreateLandingZoneResult

  @unused
  def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult

  // TODO CORE-501: in use
  def getLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): AzureLandingZone

  /**
   * Initiate deletion of a landing zone.
    * This will either return the delete result, which will contain a job ID that can be used to check the status of the deletion,
    * or None if there is no job to wait for (such as the landing zone already being deleted, if WSM returns a 404).
   */
  // TODO CORE-501: in use
  def deleteLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): Option[DeleteAzureLandingZoneResult]

  @unused
  def getDeleteLandingZoneResult(jobId: String,
                                 landingZoneId: UUID,
                                 ctx: RawlsRequestContext
  ): DeleteAzureLandingZoneJobResult

  @throws(classOf[ApiException])
  def throwWhenUnavailable(): Unit
}
