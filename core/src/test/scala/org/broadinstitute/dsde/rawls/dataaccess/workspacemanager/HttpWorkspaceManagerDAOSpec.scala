package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiClient
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.mockito.Mockito.when
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
    val provider = new WorkspaceManagerApiClientProvider {
      override def getApiClient(accessToken: String): ApiClient = {
        mock[ApiClient]
      }
    }
    val wsmDao = new HttpWorkspaceManagerDAO(provider)
    val workspaceId = UUID.randomUUID()

    wsmDao.enableApplication(workspaceId, "leo", OAuth2BearerToken("fake_token"))
  }

}
