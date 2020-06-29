package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.api.WorkspaceApi
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model.{CreateDataReferenceRequestBody, CreateWorkspaceRequestBody, CreatedWorkspace, DataReferenceDescription, DataReferenceList, DeleteWorkspaceRequestBody, WorkspaceDescription}
import org.broadinstitute.dsde.rawls.model.DataReferenceName

import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAO(baseWorkspaceManagerUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends WorkspaceManagerDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    // ignore any ServerConfigurations that pre-exist in the client by setting serverIndex to null
    client.setServerIndex(null)
    // and instead, rely solely on the basePath, which we read from env-specific Rawls config
    client.setBasePath(baseWorkspaceManagerUrl)
    client.setBearerToken(accessToken)

    client
  }

  private def getWorkspaceApi(accessToken: OAuth2BearerToken): WorkspaceApi = {
    new WorkspaceApi(getApiClient(accessToken.token))
  }

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = {
    getWorkspaceApi(accessToken).getWorkspace(workspaceId.toString)
  }

  override def createWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): CreatedWorkspace = {
    getWorkspaceApi(folderManagerAccessToken).createWorkspace(new CreateWorkspaceRequestBody().id(workspaceId).authToken(bodyAccessToken.token))
  }

  override def deleteWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(folderManagerAccessToken).deleteWorkspace(workspaceId.toString, new DeleteWorkspaceRequestBody().authToken(bodyAccessToken.token))
  }

  override def createDataReference(workspaceId: UUID, name: DataReferenceName, referenceType: String, reference: String, cloningInstructions: String, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).createDataReference(workspaceId.toString, new CreateDataReferenceRequestBody().name(name.value).referenceType(referenceType).reference(reference).cloningInstructions(cloningInstructions))
  }

  override def deleteDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(accessToken).deleteDataReference(workspaceId.toString, referenceId.toString)
  }

  override def getDataReference(workspaceId: UUID, snapshotId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).getDataReference(workspaceId.toString, snapshotId.toString)
  }

  override def getDataReferenceByName(workspaceId: UUID, refType: String, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).getDataReferenceByName(workspaceId.toString, refType, refName.value)
  }

  override def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList = {
    getWorkspaceApi(accessToken).enumerateReferences(workspaceId.toString, offset, limit)
  }

}
