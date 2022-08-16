package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.api.{ControlledAzureResourceApi, WorkspaceApplicationApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAOSpec extends AnyFlatSpec with Matchers with MockitoSugar with MockitoTestUtils {
  implicit val actorSystem: ActorSystem = ActorSystem("HttpWorkspaceManagerDAOSpec")
  implicit val executionContext: ExecutionContext = new TestExecutionContext()

  behavior of "enableApplication"

  it should "call the WSM app API" in {
    val workspaceApplicationApi = mock[WorkspaceApplicationApi]
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]

    val provider = new WorkspaceManagerApiClientProvider {
      override def getApiClient(accessToken: String): ApiClient = ???

      override def getWorkspaceApplicationApi(accessToken: String): WorkspaceApplicationApi =
        workspaceApplicationApi

      override def getControlledAzureResourceApi(accessToken: String): ControlledAzureResourceApi =
        controlledAzureResourceApi
    }
    val wsmDao = new HttpWorkspaceManagerDAO(provider)
    val workspaceId = UUID.randomUUID()

    def assertCommonFields(commonFields: ControlledResourceCommonFields): Unit = {
      commonFields.getName should endWith(workspaceId.toString)
      commonFields.getCloningInstructions shouldBe CloningInstructionsEnum.NOTHING
      commonFields.getAccessScope shouldBe AccessScope.SHARED_ACCESS
      commonFields.getManagedBy shouldBe ManagedBy.USER
    }

    wsmDao.enableApplication(workspaceId, "leo", OAuth2BearerToken("fake_token"))
    verify(workspaceApplicationApi).enableWorkspaceApplication(workspaceId, "leo")

    val relayArgumentCaptor = captor[CreateControlledAzureRelayNamespaceRequestBody]
    wsmDao.createAzureRelay(workspaceId, "arlington", OAuth2BearerToken("fake_token"))
    verify(controlledAzureResourceApi).createAzureRelayNamespace(relayArgumentCaptor.capture, any[UUID])
    relayArgumentCaptor.getValue.getAzureRelayNamespace.getRegion shouldBe "arlington"
    relayArgumentCaptor.getValue.getAzureRelayNamespace.getNamespaceName should endWith(workspaceId.toString)
    assertCommonFields(relayArgumentCaptor.getValue.getCommon)

    val saArgumentCaptor = captor[CreateControlledAzureStorageRequestBody]
    wsmDao.createAzureStorageAccount(workspaceId, "arlington", OAuth2BearerToken("fake_token"))
    verify(controlledAzureResourceApi).createAzureStorage(saArgumentCaptor.capture, any[UUID])
    saArgumentCaptor.getValue.getAzureStorage.getRegion shouldBe "arlington"
    saArgumentCaptor.getValue.getAzureStorage.getStorageAccountName should startWith("sa")
    assertCommonFields(saArgumentCaptor.getValue.getCommon)

    val scArgumentCaptor = captor[CreateControlledAzureStorageContainerRequestBody]
    val storageAccountId = UUID.randomUUID()
    wsmDao.createAzureStorageContainer(workspaceId, storageAccountId, OAuth2BearerToken("fake_token"))
    verify(controlledAzureResourceApi).createAzureStorageContainer(scArgumentCaptor.capture, any[UUID])
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageContainerName shouldBe "sc-" + workspaceId
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageAccountId shouldBe storageAccountId
    assertCommonFields(scArgumentCaptor.getValue.getCommon)
  }
}
