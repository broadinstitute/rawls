package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.api.{
  ControlledAzureResourceApi,
  LandingZonesApi,
  UnauthenticatedApi,
  WorkspaceApi,
  WorkspaceApplicationApi
}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
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

  val workspaceId = UUID.randomUUID()
  val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                          OAuth2BearerToken("token"),
                          123,
                          RawlsUserSubjectId("123456789876543212345")
  )
  val testContext = RawlsRequestContext(userInfo)

  def getApiClientProvider(workspaceApplicationApi: WorkspaceApplicationApi = mock[WorkspaceApplicationApi],
                           controlledAzureResourceApi: ControlledAzureResourceApi = mock[ControlledAzureResourceApi],
                           workspaceApi: WorkspaceApi = mock[WorkspaceApi],
                           landingZonesApi: LandingZonesApi = mock[LandingZonesApi]
  ): WorkspaceManagerApiClientProvider = new WorkspaceManagerApiClientProvider {
    override def getApiClient(ctx: RawlsRequestContext): ApiClient = ???

    override def getWorkspaceApplicationApi(ctx: RawlsRequestContext): WorkspaceApplicationApi =
      workspaceApplicationApi

    override def getControlledAzureResourceApi(ctx: RawlsRequestContext): ControlledAzureResourceApi =
      controlledAzureResourceApi

    override def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi =
      workspaceApi

    override def getLandingZonesApi(ctx: RawlsRequestContext): LandingZonesApi = landingZonesApi

    override def getUnauthenticatedApi(): UnauthenticatedApi = ???
  }

  behavior of "enableApplication"

  it should "call the WSM app API" in {
    val workspaceApplicationApi = mock[WorkspaceApplicationApi]
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]

    val wsmDao = new HttpWorkspaceManagerDAO(
      getApiClientProvider(workspaceApplicationApi = workspaceApplicationApi,
                           controlledAzureResourceApi = controlledAzureResourceApi
      )
    )

    wsmDao.enableApplication(workspaceId, "leo", testContext)
    verify(workspaceApplicationApi).enableWorkspaceApplication(workspaceId, "leo")
  }

  def assertControlledResourceCommonFields(commonFields: ControlledResourceCommonFields,
                                           expectedCloningInstructions: CloningInstructionsEnum =
                                             CloningInstructionsEnum.NOTHING
  ): Unit = {
    commonFields.getName should endWith(workspaceId.toString)
    commonFields.getCloningInstructions shouldBe expectedCloningInstructions
    commonFields.getAccessScope shouldBe AccessScope.SHARED_ACCESS
    commonFields.getManagedBy shouldBe ManagedBy.USER
  }

  behavior of "createAzureRelay"

  it should "call the WSM controlled azure resource API" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val relayArgumentCaptor = captor[CreateControlledAzureRelayNamespaceRequestBody]
    wsmDao.createAzureRelay(workspaceId, "arlington", testContext)
    verify(controlledAzureResourceApi).createAzureRelayNamespace(relayArgumentCaptor.capture, any[UUID])
    relayArgumentCaptor.getValue.getAzureRelayNamespace.getRegion shouldBe "arlington"
    relayArgumentCaptor.getValue.getAzureRelayNamespace.getNamespaceName should endWith(workspaceId.toString)
    assertControlledResourceCommonFields(relayArgumentCaptor.getValue.getCommon)
  }

  behavior of "createAzureStorageAccount"

  it should "call the WSM controlled azure resource API" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val saArgumentCaptor = captor[CreateControlledAzureStorageRequestBody]
    wsmDao.createAzureStorageAccount(workspaceId, "arlington", testContext)
    verify(controlledAzureResourceApi).createAzureStorage(saArgumentCaptor.capture, any[UUID])
    saArgumentCaptor.getValue.getAzureStorage.getRegion shouldBe "arlington"
    saArgumentCaptor.getValue.getAzureStorage.getStorageAccountName should startWith("sa")
    assertControlledResourceCommonFields(saArgumentCaptor.getValue.getCommon)
  }

  behavior of "createAzureStorageContainer"

  it should "call the WSM controlled azure resource API" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val scArgumentCaptor = captor[CreateControlledAzureStorageContainerRequestBody]
    val storageAccountId = UUID.randomUUID()
    wsmDao.createAzureStorageContainer(workspaceId, storageAccountId, testContext)
    verify(controlledAzureResourceApi).createAzureStorageContainer(scArgumentCaptor.capture, any[UUID])
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageContainerName shouldBe "sc-" + workspaceId
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageAccountId shouldBe storageAccountId
    assertControlledResourceCommonFields(scArgumentCaptor.getValue.getCommon, CloningInstructionsEnum.DEFINITION)
  }

  behavior of "getRoles"

  it should "call the WSM workspace API" in {
    val workspaceApi = mock[WorkspaceApi]

    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.getRoles(workspaceId, testContext)
    verify(workspaceApi).getRoles(workspaceId)
  }

  behavior of "grantRole"

  it should "call the WSM workspace API" in {
    val workspaceApi = mock[WorkspaceApi]

    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))
    val email = WorkbenchEmail("test@example.com")
    val iamRole = IamRole.OWNER

    wsmDao.grantRole(workspaceId, email, iamRole, testContext)
    verify(workspaceApi).grantRole(new GrantRoleRequestBody().memberEmail(email.value), workspaceId, iamRole)
  }

  behavior of "removeRole"

  it should "call the WSM workspace API" in {
    val workspaceApi = mock[WorkspaceApi]

    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))
    val email = WorkbenchEmail("test@example.com")
    val iamRole = IamRole.OWNER

    wsmDao.removeRole(workspaceId, email, iamRole, testContext)
    verify(workspaceApi).removeRole(workspaceId, iamRole, email.value)
  }

  behavior of "create and delete landing zone"

  it should "call the WSM landing zone resource API" in {
    val landingZonesApi = mock[LandingZonesApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(landingZonesApi = landingZonesApi))

    val creationArgumentCaptor = captor[CreateAzureLandingZoneRequestBody]
    val billingProfileId = UUID.randomUUID()
    wsmDao.createLandingZone("fake-definition", "fake-version", billingProfileId, testContext)
    verify(landingZonesApi).createAzureLandingZone(creationArgumentCaptor.capture)
    creationArgumentCaptor.getValue.getBillingProfileId shouldBe billingProfileId
    creationArgumentCaptor.getValue.getDefinition shouldBe "fake-definition"
    creationArgumentCaptor.getValue.getVersion shouldBe "fake-version"

    val landingZoneId = UUID.randomUUID()
    wsmDao.deleteLandingZone(landingZoneId, testContext)
    verify(landingZonesApi).deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody],
                                                   ArgumentMatchers.eq(landingZoneId)
    )
  }
}
