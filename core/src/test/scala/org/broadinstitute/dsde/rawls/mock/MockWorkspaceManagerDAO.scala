package org.broadinstitute.dsde.rawls.mock

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, ErrorReport}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._


class MockWorkspaceManagerDAO(val createCloudContextResult: CreateCloudContextResult = MockWorkspaceManagerDAO.getCreateCloudContextResult(StatusEnum.SUCCEEDED),
                              val createAzureRelayResult: CreateControlledAzureRelayNamespaceResult = MockWorkspaceManagerDAO.getCreateControlledAzureRelayNamespaceResult(StatusEnum.SUCCEEDED)) extends WorkspaceManagerDAO {

  val references: TrieMap[(UUID, UUID), DataRepoSnapshotResource] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId)
  def mockReferenceResponse(workspaceId: UUID, referenceId: UUID) = references.getOrElse((workspaceId, referenceId), throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
  def mockEnumerateReferenceResponse(workspaceId: UUID) = references.collect {
    case ((wsId, _), refDescription) if wsId == workspaceId => refDescription
  }
  def mockInitialCreateAzureCloudContextResult() = MockWorkspaceManagerDAO.getCreateCloudContextResult(StatusEnum.RUNNING)
  def mockCreateAzureCloudContextResult() = createCloudContextResult
  def mockInitialAzureRelayNamespaceResult() = MockWorkspaceManagerDAO.getCreateControlledAzureRelayNamespaceResult(StatusEnum.RUNNING)
  def mockAzureRelayNamespaceResult() = createAzureRelayResult

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = mockGetWorkspaceResponse(workspaceId)

  override def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace = mockCreateWorkspaceResponse(workspaceId)

  override def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit = ()

  override def createDataRepoSnapshotReference(workspaceId: UUID, snapshotId: UUID, name: DataReferenceName, description: Option[DataReferenceDescriptionField], instanceName: String, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    if(name.value.contains("fakesnapshot"))
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
  }

  override def getDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    mockReferenceResponse(workspaceId, referenceId)
  }

  override def getDataRepoSnapshotReferenceByName(workspaceId: UUID, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    this.references.find {
      case ((workspaceUUID, _), ref) => workspaceUUID == workspaceId && ref.getMetadata.getName == refName.value && ref.getMetadata.getResourceType == ResourceType.DATA_REPO_SNAPSHOT
    }.getOrElse(throw new ApiException(StatusCodes.NotFound.intValue, s"Snapshot $refName not found in workspace $workspaceId"))._2
  }

  override def enumerateDataRepoSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): ResourceList = {
    val resources = mockEnumerateReferenceResponse(workspaceId).map { resp =>
      val attributesUnion = new ResourceAttributesUnion().gcpDataRepoSnapshot(resp.getAttributes)
      new ResourceDescription().metadata(resp.getMetadata).resourceAttributes(attributesUnion)
    }.toList.asJava
    new ResourceList().resources(resources)
  }

  override def updateDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataRepoSnapshotReferenceRequestBody, accessToken: OAuth2BearerToken): Unit = {
    if (references.contains(workspaceId, referenceId)) {
      val existingRef = references.get(workspaceId, referenceId).get
      val newMetadata = existingRef.getMetadata.name(
        if (updateInfo.getName != null) updateInfo.getName else existingRef.getMetadata.getName
      ).description(
        if (updateInfo.getDescription != null) updateInfo.getDescription else existingRef.getMetadata.getDescription
      )
      references.update((workspaceId, referenceId), existingRef.metadata(newMetadata))
    }
  }

  override def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    if (references.contains(workspaceId, referenceId))
      references -= ((workspaceId, referenceId))
  }

  override def createWorkspaceWithSpendProfile(workspaceId: UUID, displayName: String, spendProfileId: String, accessToken: OAuth2BearerToken): CreatedWorkspace =
    mockCreateWorkspaceResponse(workspaceId)

  override def createAzureWorkspaceCloudContext(workspaceId: UUID,
                                                azureTenantId: String,
                                                azureResourceGroupId: String,
                                                azureSubscriptionId: String,
                                                accessToken: OAuth2BearerToken): CreateCloudContextResult = mockInitialCreateAzureCloudContextResult()

  override def getWorkspaceCreateCloudContextResult(workspaceId: UUID,
                                                    jobControlId: String,
                                                    accessToken: OAuth2BearerToken): CreateCloudContextResult = mockCreateAzureCloudContextResult()

  override def enableApplication(workspaceId: UUID, applicationId: String, accessToken: OAuth2BearerToken): WorkspaceApplicationDescription = {
    new WorkspaceApplicationDescription().workspaceId(workspaceId).applicationId(applicationId)
  }

  override def createAzureRelay(workspaceId: UUID, region: String, accessToken: OAuth2BearerToken): CreateControlledAzureRelayNamespaceResult = {
    mockInitialAzureRelayNamespaceResult()
  }

  override def getCreateAzureRelayResult(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken): CreateControlledAzureRelayNamespaceResult = {
    mockAzureRelayNamespaceResult()
  }
}


object MockWorkspaceManagerDAO {
  def getCreateCloudContextResult(status: StatusEnum): CreateCloudContextResult = {
    new CreateCloudContextResult().jobReport(new JobReport().id("fake_id").status(status))
  }

  def getCreateControlledAzureRelayNamespaceResult(status: StatusEnum): CreateControlledAzureRelayNamespaceResult = {
    new CreateControlledAzureRelayNamespaceResult().jobReport(new JobReport().id("relay_fake_id").status(status))
  }

  def buildWithAsyncResults(createCloudContestStatus: StatusEnum, createAzureRelayStatus: StatusEnum) = {
    new MockWorkspaceManagerDAO(
      getCreateCloudContextResult(createCloudContestStatus),
      getCreateControlledAzureRelayNamespaceResult(createAzureRelayStatus)
    )
  }
}
