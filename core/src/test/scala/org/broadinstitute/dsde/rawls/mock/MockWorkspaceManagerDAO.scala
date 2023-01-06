package org.broadinstitute.dsde.rawls.mock

import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import bio.terra.stairway.ShortUUID
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  DataReferenceDescriptionField,
  DataReferenceName,
  ErrorReport,
  RawlsRequestContext
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._

class MockWorkspaceManagerDAO(
  val createCloudContextResult: CreateCloudContextResult =
    MockWorkspaceManagerDAO.getCreateCloudContextResult(StatusEnum.SUCCEEDED)
) extends WorkspaceManagerDAO {

  val references: TrieMap[(UUID, UUID), DataRepoSnapshotResource] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId)
  def mockReferenceResponse(workspaceId: UUID, referenceId: UUID) = references.getOrElse(
    (workspaceId, referenceId),
    throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
  )
  def mockEnumerateReferenceResponse(workspaceId: UUID) = references.collect {
    case ((wsId, _), refDescription) if wsId == workspaceId => refDescription
  }
  def mockInitialCreateAzureCloudContextResult() =
    MockWorkspaceManagerDAO.getCreateCloudContextResult(StatusEnum.RUNNING)
  def mockCreateAzureCloudContextResult() = createCloudContextResult
  def mockCloneWorkspaceResult() = MockWorkspaceManagerDAO.getCloneWorkspaceResult(StatusEnum.SUCCEEDED)
  def mockCreateAzureStorageAccountResult() =
    new CreatedControlledAzureStorage().resourceId(UUID.randomUUID()).azureStorage(new AzureStorageResource())
  def mockCreateAzureStorageContainerResult() = new CreatedControlledAzureStorageContainer()

  override def getWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): WorkspaceDescription =
    mockGetWorkspaceResponse(workspaceId)

  override def createWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): CreatedWorkspace =
    mockCreateWorkspaceResponse(workspaceId)

  override def cloneWorkspace(sourceWorkspaceId: UUID,
                              workspaceId: UUID,
                              displayName: String,
                              spendProfile: ProfileModel,
                              ctx: RawlsRequestContext,
                              location: Option[String]
  ): CloneWorkspaceResult = {
    val clonedWorkspace = new ClonedWorkspace()
      .sourceWorkspaceId(sourceWorkspaceId)
      .destinationWorkspaceId(workspaceId)
      .destinationUserFacingId(UUID.randomUUID().toString)

    // Currently have no way of specifying a job id to wsm for this route and
    // a base64 url-encoded "short" UUID is generated instead.
    val jobReport = new JobReport()
      .id(ShortUUID.get())
      .status(StatusEnum.RUNNING)

    new CloneWorkspaceResult()
      .workspace(clonedWorkspace)
      .jobReport(jobReport)
  }

  override def getCloneWorkspaceResult(workspaceId: UUID,
                                       jobControlId: String,
                                       ctx: RawlsRequestContext
  ): CloneWorkspaceResult = mockCloneWorkspaceResult()

  override def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit = ()

  override def createDataRepoSnapshotReference(workspaceId: UUID,
                                               snapshotId: UUID,
                                               name: DataReferenceName,
                                               description: Option[DataReferenceDescriptionField],
                                               instanceName: String,
                                               cloningInstructions: CloningInstructionsEnum,
                                               ctx: RawlsRequestContext
  ): DataRepoSnapshotResource =
    if (name.value.contains("fakesnapshot"))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      val attributes = new DataRepoSnapshotAttributes().instanceName(instanceName).snapshot(snapshotId.toString)
      val metadata = new ResourceMetadata()
        .name(name.value)
        .resourceId(newId)
        .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
        .stewardshipType(StewardshipType.REFERENCED)
        .workspaceId(workspaceId)
        .cloningInstructions(CloningInstructionsEnum.NOTHING)
      description.map(d => metadata.description(d.value))
      val snapshot = new DataRepoSnapshotResource().metadata(metadata).attributes(attributes)
      references.put((workspaceId, newId), snapshot)
      mockReferenceResponse(workspaceId, newId)
    }

  override def getDataRepoSnapshotReference(workspaceId: UUID,
                                            referenceId: UUID,
                                            ctx: RawlsRequestContext
  ): DataRepoSnapshotResource =
    mockReferenceResponse(workspaceId, referenceId)

  override def getDataRepoSnapshotReferenceByName(workspaceId: UUID,
                                                  refName: DataReferenceName,
                                                  ctx: RawlsRequestContext
  ): DataRepoSnapshotResource =
    this.references
      .find { case ((workspaceUUID, _), ref) =>
        workspaceUUID == workspaceId && ref.getMetadata.getName == refName.value && ref.getMetadata.getResourceType == ResourceType.DATA_REPO_SNAPSHOT
      }
      .getOrElse(
        throw new ApiException(StatusCodes.NotFound.intValue, s"Snapshot $refName not found in workspace $workspaceId")
      )
      ._2

  override def enumerateDataRepoSnapshotReferences(workspaceId: UUID,
                                                   offset: Int,
                                                   limit: Int,
                                                   ctx: RawlsRequestContext
  ): ResourceList = {
    val resources = mockEnumerateReferenceResponse(workspaceId)
      .map { resp =>
        val attributesUnion = new ResourceAttributesUnion().gcpDataRepoSnapshot(resp.getAttributes)
        new ResourceDescription().metadata(resp.getMetadata).resourceAttributes(attributesUnion)
      }
      .toList
      .asJava
    new ResourceList().resources(resources)
  }

  override def updateDataRepoSnapshotReference(workspaceId: UUID,
                                               referenceId: UUID,
                                               updateInfo: UpdateDataRepoSnapshotReferenceRequestBody,
                                               ctx: RawlsRequestContext
  ): Unit =
    if (references.contains(workspaceId, referenceId)) {
      val existingRef = references.get(workspaceId, referenceId).get
      val newMetadata = existingRef.getMetadata
        .name(
          if (updateInfo.getName != null) updateInfo.getName else existingRef.getMetadata.getName
        )
        .description(
          if (updateInfo.getDescription != null) updateInfo.getDescription else existingRef.getMetadata.getDescription
        )
      references.update((workspaceId, referenceId), existingRef.metadata(newMetadata))
    }

  override def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, ctx: RawlsRequestContext): Unit =
    if (references.contains(workspaceId, referenceId))
      references -= ((workspaceId, referenceId))

  override def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                               displayName: String,
                                               spendProfileId: String,
                                               ctx: RawlsRequestContext
  ): CreatedWorkspace =
    mockCreateWorkspaceResponse(workspaceId)

  override def createAzureWorkspaceCloudContext(workspaceId: UUID,
                                                azureTenantId: String,
                                                azureResourceGroupId: String,
                                                azureSubscriptionId: String,
                                                ctx: RawlsRequestContext
  ): CreateCloudContextResult = mockInitialCreateAzureCloudContextResult()

  override def getWorkspaceCreateCloudContextResult(workspaceId: UUID,
                                                    jobControlId: String,
                                                    ctx: RawlsRequestContext
  ): CreateCloudContextResult = mockCreateAzureCloudContextResult()

  override def enableApplication(workspaceId: UUID,
                                 applicationId: String,
                                 ctx: RawlsRequestContext
  ): WorkspaceApplicationDescription =
    new WorkspaceApplicationDescription().workspaceId(workspaceId).applicationId(applicationId)

  override def createAzureStorageAccount(workspaceId: UUID,
                                         region: String,
                                         ctx: RawlsRequestContext
  ): CreatedControlledAzureStorage =
    mockCreateAzureStorageAccountResult()

  override def createAzureStorageContainer(workspaceId: UUID,
                                           storageAccountId: Option[UUID],
                                           ctx: RawlsRequestContext
  ): CreatedControlledAzureStorageContainer =
    mockCreateAzureStorageContainerResult()

  def createLandingZone(definition: String,
                        version: String,
                        billingProfileId: UUID,
                        ctx: RawlsRequestContext
  ): CreateLandingZoneResult = ???

  override def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult = ???

  def deleteLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): DeleteAzureLandingZoneResult = ???

  def getRoles(workspaceId: UUID, ctx: RawlsRequestContext): RoleBindingList = ???

  def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit = ???

  def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit = ???

  override def throwWhenUnavailable(): Unit = ()
}

object MockWorkspaceManagerDAO {
  def getCreateCloudContextResult(status: StatusEnum): CreateCloudContextResult =
    new CreateCloudContextResult().jobReport(new JobReport().id("fake_id").status(status))

  def getCloneWorkspaceResult(status: StatusEnum): CloneWorkspaceResult =
    new CloneWorkspaceResult().jobReport(new JobReport().id("fake_id").status(status))

  def buildWithAsyncCloudContextResult(createCloudContextStatus: StatusEnum) =
    new MockWorkspaceManagerDAO(
      getCreateCloudContextResult(createCloudContextStatus)
    )
}
