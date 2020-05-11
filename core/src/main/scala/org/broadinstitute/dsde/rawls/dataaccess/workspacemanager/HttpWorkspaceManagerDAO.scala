package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.api.WorkspaceApi
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model.{CreateDataReferenceRequestBody, CreateWorkspaceRequestBody, CreatedWorkspace, DataReferenceDescription, WorkspaceDescription}
import spray.json.JsObject

import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAO(baseWorkspaceManagerUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends WorkspaceManagerDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
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

  override def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: JsObject, cloningInstructions: String, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).createDataReference(workspaceId.toString, new CreateDataReferenceRequestBody().name(name).referenceType(referenceType).reference(reference).cloningInstructions(cloningInstructions))
  }

  override def getDataReference(workspaceId: UUID, snapshotId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    getWorkspaceApi(accessToken).getDataReference(workspaceId.toString, snapshotId.toString)
  }

  override def enumerateDataReferences(workspaceId: UUID, limit: int, offset: int, accessToken: OAuth2BearerToken): DataReferenceList = {
    getWorkspaceApi(accessToken).enumerateDataReference(workspaceId.toString, offset, limit)
  }

}
