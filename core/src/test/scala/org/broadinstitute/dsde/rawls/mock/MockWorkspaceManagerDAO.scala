package org.broadinstitute.dsde.rawls.mock

import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import bio.terra.stairway.ShortUUID
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
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
  val createWorkspaceResult: CreateWorkspaceV2Result =
    MockWorkspaceManagerDAO.getCreateWorkspaceResult(StatusEnum.SUCCEEDED)
) extends WorkspaceManagerDAO {

  val references: TrieMap[(UUID, UUID), DataRepoSnapshotResource] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) =
    new WorkspaceDescription().id(workspaceId).stage(WorkspaceStageModel.RAWLS_WORKSPACE)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId)

  def mockInitialCreateWorkspaceV2Result() =
    MockWorkspaceManagerDAO.getCreateWorkspaceResult(StatusEnum.RUNNING)
  def mockCreateWorkspaceV2Result() = createWorkspaceResult

  def mockReferenceResponse(workspaceId: UUID, referenceId: UUID) = references.getOrElse(
    (workspaceId, referenceId),
    throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
  )
  def mockEnumerateReferenceResponse(workspaceId: UUID) = references.collect {
    case ((wsId, _), refDescription) if wsId == workspaceId => refDescription
  }
  def mockCreateAzureStorageContainerResult() = new CreatedControlledAzureStorageContainer()

  override def getWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): WorkspaceDescription =
    mockGetWorkspaceResponse(workspaceId)

  override def listWorkspaces(ctx: RawlsRequestContext, batchSize: Int = 100): List[WorkspaceDescription] = List()

  override def createWorkspace(workspaceId: UUID,
                               workspaceType: WorkspaceType, // currently ignored by the mock
                               policyInputs: Option[WsmPolicyInputs], // currently ignored by the mock
                               ctx: RawlsRequestContext
  ): CreatedWorkspace =
    mockCreateWorkspaceResponse(workspaceId)

  @throws[ApiException]
  override def cloneWorkspace(sourceWorkspaceId: UUID,
                              workspaceId: UUID,
                              displayName: String,
                              spendProfile: Option[ProfileModel],
                              billingProjectNamespace: String,
                              ctx: RawlsRequestContext,
                              additionalPolicyInputs: Option[WsmPolicyInputs]
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

  override def cloneAzureStorageContainer(sourceWorkspaceId: UUID,
                                          destinationWorkspaceId: UUID,
                                          sourceContainerId: UUID,
                                          destinationContainerName: String,
                                          cloningInstructions: CloningInstructionsEnum,
                                          prefixToClone: Option[String],
                                          ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult = {

    val clonedStorageContainer = new ClonedControlledAzureStorageContainer()
      .sourceWorkspaceId(sourceWorkspaceId)
      .sourceResourceId(sourceContainerId)
      .effectiveCloningInstructions(cloningInstructions)
      .storageContainer(mockCreateAzureStorageContainerResult())

    val jobReport = new JobReport()
      .id(UUID.randomUUID().toString)
      .status(StatusEnum.RUNNING)

    new CloneControlledAzureStorageContainerResult()
      .container(clonedStorageContainer)
      .jobReport(jobReport)
  }

  def getCloneAzureStorageContainerResult(workspaceId: UUID,
                                          jobId: String,
                                          ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult = {

    val jobReport = new JobReport()
      .id(jobId)
      .status(StatusEnum.RUNNING)
    new CloneControlledAzureStorageContainerResult()
      .jobReport(jobReport)
  }

  def enumerateStorageContainers(workspaceId: UUID, offset: Int, limit: Int, ctx: RawlsRequestContext): ResourceList =
    new ResourceList()

  override def getCloneWorkspaceResult(workspaceId: UUID,
                                       jobControlId: String,
                                       ctx: RawlsRequestContext
  ): CloneWorkspaceResult = MockWorkspaceManagerDAO.getCloneWorkspaceResult(StatusEnum.SUCCEEDED)

  override def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit = ()

  override def updateWorkspacePolicies(workspaceId: UUID,
                                       policyInputs: WsmPolicyInputs,
                                       ctx: RawlsRequestContext
  ): WsmPolicyUpdateResult = ???

  override def createDataRepoSnapshotReference(workspaceId: UUID,
                                               snapshotId: UUID,
                                               name: DataReferenceName,
                                               description: Option[DataReferenceDescriptionField],
                                               instanceName: String,
                                               cloningInstructions: CloningInstructionsEnum,
                                               properties: Option[Map[String, String]],
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
                                               billingProjectNamespace: String,
                                               applicationIds: Seq[String],
                                               cloudPlatform: CloudPlatform,
                                               policyInputs: Option[WsmPolicyInputs],
                                               ctx: RawlsRequestContext
  ): CreateWorkspaceV2Result =
    mockInitialCreateWorkspaceV2Result()

  override def getCreateWorkspaceResult(workspaceId: String, ctx: RawlsRequestContext): CreateWorkspaceV2Result =
    mockCreateWorkspaceV2Result()

  override def createAzureStorageContainer(workspaceId: UUID,
                                           storageContainerName: String,
                                           ctx: RawlsRequestContext
  ): CreatedControlledAzureStorageContainer =
    mockCreateAzureStorageContainerResult()

  def createLandingZone(definition: String,
                        version: String,
                        landingZoneParameters: Map[String, String],
                        billingProfileId: UUID,
                        ctx: RawlsRequestContext,
                        landingZoneId: Option[UUID] = None
  ): CreateLandingZoneResult = ???

  override def getJob(jobControlId: String, ctx: RawlsRequestContext): JobReport = ???

  override def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult = ???

  override def getLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): AzureLandingZone = ???

  override def deleteLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): Some[DeleteAzureLandingZoneResult] =
    ???

  override def getDeleteLandingZoneResult(jobId: String,
                                          landingZoneId: UUID,
                                          ctx: RawlsRequestContext
  ): DeleteAzureLandingZoneJobResult = ???

  def getRoles(workspaceId: UUID, ctx: RawlsRequestContext): RoleBindingList = ???

  def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit = ???

  def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit = ???

  override def throwWhenUnavailable(): Unit = ()

  override def deleteWorkspaceV2(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): JobResult =
    new JobResult().jobReport(new JobReport().id(UUID.randomUUID.toString))

  override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                          jobControlId: String,
                                          ctx: RawlsRequestContext
  ): JobResult =
    new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.SUCCEEDED))
}

object MockWorkspaceManagerDAO {
  def getCreateWorkspaceResult(status: StatusEnum): CreateWorkspaceV2Result =
    new CreateWorkspaceV2Result().jobReport(new JobReport().id("fake_id").status(status))

  def getCloneWorkspaceResult(status: StatusEnum): CloneWorkspaceResult =
    new CloneWorkspaceResult().jobReport(new JobReport().id("fake_id").status(status))

  def buildWithAsyncCreateWorkspaceResult(createWorkspaceStatus: StatusEnum) =
    new MockWorkspaceManagerDAO(
      getCreateWorkspaceResult(createWorkspaceStatus)
    )
}
