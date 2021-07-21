package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.api.{ReferencedGcpResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName}

import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAO(baseWorkspaceManagerUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends WorkspaceManagerDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseWorkspaceManagerUrl)
    client.setAccessToken(accessToken)

    client
  }

  private def getWorkspaceApi(accessToken: OAuth2BearerToken): WorkspaceApi = {
    new WorkspaceApi(getApiClient(accessToken.token))
  }

  private def getReferencedGcpResourceApi(accessToken: OAuth2BearerToken): ReferencedGcpResourceApi = {
    new ReferencedGcpResourceApi(getApiClient(accessToken.token))
  }

  private def getResourceApi(accessToken: OAuth2BearerToken): ResourceApi = {
    new ResourceApi(getApiClient(accessToken.token))
  }

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = {
    getWorkspaceApi(accessToken).getWorkspace(workspaceId)
  }

  override def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace = {
    getWorkspaceApi(accessToken).createWorkspace(new CreateWorkspaceRequestBody().id(workspaceId))
  }

  override def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(accessToken).deleteWorkspace(workspaceId)
  }

  override def createDataRepoSnapshotReference(workspaceId: UUID, snapshotId: UUID, name: DataReferenceName, description: Option[DataReferenceDescriptionField], instanceName: String, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    val snapshot = new DataRepoSnapshotAttributes().instanceName(instanceName).snapshot(snapshotId.toString)
    val commonFields = new ReferenceResourceCommonFields().name(name.value).cloningInstructions(CloningInstructionsEnum.NOTHING)
    description.map(d => commonFields.description(d.value))
    val request = new CreateDataRepoSnapshotReferenceRequestBody().snapshot(snapshot).metadata(commonFields)
    getReferencedGcpResourceApi(accessToken).createDataRepoSnapshotReference(request, workspaceId)
  }

  override def updateDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataReferenceRequestBody, accessToken: OAuth2BearerToken): Unit = {
    getReferencedGcpResourceApi(accessToken).updateDataRepoSnapshotReference(updateInfo, workspaceId, referenceId)
  }

  override def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getReferencedGcpResourceApi(accessToken).deleteDataRepoSnapshotReference(workspaceId, referenceId)
  }

  override def getDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    getReferencedGcpResourceApi(accessToken).getDataRepoSnapshotReference(workspaceId, referenceId)
  }

  override def getDataRepoSnapshotReferenceByName(workspaceId: UUID, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    getReferencedGcpResourceApi(accessToken).getDataRepoSnapshotReferenceByName(workspaceId, refName.value)
  }

  override def enumerateDataRepoSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): ResourceList = {
    getResourceApi(accessToken).enumerateResources(workspaceId, offset, limit, ResourceType.DATA_REPO_SNAPSHOT, StewardshipType.REFERENCED)
  }

  override def createBigQueryDatasetReference(workspaceId: UUID, metadata: ReferenceResourceCommonFields, dataset: GcpBigQueryDatasetAttributes, accessToken: OAuth2BearerToken): GcpBigQueryDatasetResource = {
    val createBigQueryDatasetReference = new CreateGcpBigQueryDatasetReferenceRequestBody().dataset(dataset).metadata(metadata)
    getReferencedGcpResourceApi(accessToken).createBigQueryDatasetReference(createBigQueryDatasetReference, workspaceId)
  }

  override def deleteBigQueryDatasetReference(workspaceId: UUID, resourceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getReferencedGcpResourceApi(accessToken).deleteBigQueryDatasetReference(workspaceId, resourceId)
  }

  override def getBigQueryDatasetReferenceByName(workspaceId: UUID, name: String, accessToken: OAuth2BearerToken): GcpBigQueryDatasetResource = {
    getReferencedGcpResourceApi(accessToken).getBigQueryDatasetReferenceByName(workspaceId, name)
  }


}
