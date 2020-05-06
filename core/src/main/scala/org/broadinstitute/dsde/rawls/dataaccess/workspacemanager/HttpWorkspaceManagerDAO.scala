package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import bio.terra.workspace.api.WorkspaceApi
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model.{CreateDataReferenceRequestBody, CreateWorkspaceRequestBody, CreatedWorkspace, DataReferenceDescription, WorkspaceDescription}
import org.broadinstitute.dsde.rawls.model.UserInfo
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

class HttpWorkspaceManagerDAO(baseWorkspaceManagerUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends WorkspaceManagerDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseWorkspaceManagerUrl)
    client.setBearerToken(accessToken)

    client
  }

  private def getWorkspaceApi(userInfo: UserInfo): WorkspaceApi = {
    new WorkspaceApi(getApiClient(userInfo.accessToken.token))
  }

  override def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WorkspaceDescription] = {
    Future {
      getWorkspaceApi(userInfo).getWorkspace(workspaceId.toString)
    }
  }

  override def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[CreatedWorkspace] = {
    Future {
      getWorkspaceApi(userInfo).createWorkspace(new CreateWorkspaceRequestBody().id(workspaceId).authToken(userInfo.accessToken.token))
    }
  }

  override def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: JsObject, cloningInstructions: String, userInfo: UserInfo): Future[DataReferenceDescription] = {
    Future {
      getWorkspaceApi(userInfo).createDataReference(workspaceId.toString, new CreateDataReferenceRequestBody().name(name).referenceType(referenceType).reference(reference).cloningInstructions(cloningInstructions))
    }
  }

  override def getDataReference(workspaceId: UUID, snapshotId: UUID, userInfo: UserInfo): Future[DataReferenceDescription] = {
    Future {
      getWorkspaceApi(userInfo).getDataReference(workspaceId.toString, snapshotId.toString)
    }
  }

}
