package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.api.WorkspaceApi
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

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = {
    getWorkspaceApi(accessToken).getWorkspace(workspaceId)
  }

  override def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace = {
    getWorkspaceApi(accessToken).createWorkspace(new CreateWorkspaceRequestBody().id(workspaceId))
  }

  override def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(accessToken).deleteWorkspace(workspaceId)
  }

  override def createDataReference(workspaceId: UUID, name: DataReferenceName, description: DataReferenceDescriptionField, referenceType: ReferenceTypeEnum, reference: DataRepoSnapshot, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).createDataReference(new CreateDataReferenceRequestBody().name(name.value).description(description.value).referenceType(referenceType).reference(reference).cloningInstructions(cloningInstructions), workspaceId)
  }

  override def updateDataReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataReferenceRequestBody, accessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(accessToken).updateDataReference(updateInfo, workspaceId, referenceId)
  }

  override def deleteDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(accessToken).deleteDataReference(workspaceId, referenceId)
  }

  override def getDataReference(workspaceId: UUID, snapshotId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).getDataReference(workspaceId, snapshotId)
  }

  override def getDataReferenceByName(workspaceId: UUID, refType: ReferenceTypeEnum, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).getDataReferenceByName(workspaceId, refType, refName.value)
  }

  override def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList = {
    getWorkspaceApi(accessToken).enumerateReferences(workspaceId, offset, limit)
  }

}
