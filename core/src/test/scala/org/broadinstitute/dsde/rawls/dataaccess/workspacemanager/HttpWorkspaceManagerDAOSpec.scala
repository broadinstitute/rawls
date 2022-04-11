package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.api.{ControlledAzureResourceApi, WorkspaceApplicationApi}
import bio.terra.workspace.client.ApiClient
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.mockito.Mockito.verify
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAOSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  implicit val actorSystem: ActorSystem = ActorSystem("HttpWorkspaceManagerDAOSpec")
  implicit val executionContext: ExecutionContext = new TestExecutionContext()


  behavior of "enableApplication"

  it should "call the WSM app API" in {
    val workspaceApplicationApi = mock[WorkspaceApplicationApi]
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]

    val provider = new WorkspaceManagerApiClientProvider {
      override def getApiClient(accessToken: String): ApiClient = ???

      override def getWorkspaceApplicationApi(accessToken: String): WorkspaceApplicationApi = {
        workspaceApplicationApi
      }

      override def getControlledAzureResourceApi(accessToken: String): ControlledAzureResourceApi = {
        controlledAzureResourceApi
      }
    }
    val wsmDao = new HttpWorkspaceManagerDAO(provider)
    val workspaceId = UUID.randomUUID()

    wsmDao.enableApplication(workspaceId, "leo", OAuth2BearerToken("fake_token"))

    verify(workspaceApplicationApi).enableWorkspaceApplication(workspaceId, "leo")
  }
}
