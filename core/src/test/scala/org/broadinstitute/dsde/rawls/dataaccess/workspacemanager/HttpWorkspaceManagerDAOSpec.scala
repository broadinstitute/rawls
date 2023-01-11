package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import bio.terra.stairway.ShortUUID
import bio.terra.workspace.api._
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID

class HttpWorkspaceManagerDAOSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with MockitoTestUtils
    with TestDriverComponent {

  implicit val actorSystem: ActorSystem = ActorSystem("HttpWorkspaceManagerDAOSpec")

  val workspaceId = UUID.randomUUID()

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

  it should "call the WSM controlled azure resource API with a SA id" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val scArgumentCaptor = captor[CreateControlledAzureStorageContainerRequestBody]
    val storageAccountId = UUID.randomUUID()
    wsmDao.createAzureStorageContainer(workspaceId, Some(storageAccountId), testContext)
    verify(controlledAzureResourceApi).createAzureStorageContainer(scArgumentCaptor.capture, any[UUID])
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageContainerName shouldBe "sc-" + workspaceId
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageAccountId shouldBe storageAccountId
    assertControlledResourceCommonFields(scArgumentCaptor.getValue.getCommon, CloningInstructionsEnum.NOTHING)
  }

  it should "call the WSM controlled azure resource API without a SA id" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val scArgumentCaptor = captor[CreateControlledAzureStorageContainerRequestBody]
    wsmDao.createAzureStorageContainer(workspaceId, None, testContext)
    verify(controlledAzureResourceApi).createAzureStorageContainer(scArgumentCaptor.capture, any[UUID])
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageContainerName shouldBe "sc-" + workspaceId
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageAccountId shouldBe null
    assertControlledResourceCommonFields(scArgumentCaptor.getValue.getCommon, CloningInstructionsEnum.NOTHING)
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

  behavior of "clone"
  it should "call the WSM workspace API" in {
    val workspaceApi = mock[WorkspaceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    val expectedRequest = new CloneWorkspaceRequest()
      .displayName("my-workspace-clone")
      .destinationWorkspaceId(workspaceId)
      .spendProfile(testData.azureBillingProfile.getId.toString)
      .location("the-moon")
      .azureContext(
        new AzureContext()
          .tenantId(testData.azureBillingProfile.getTenantId.toString)
          .subscriptionId(testData.azureBillingProfile.getSubscriptionId.toString)
          .resourceGroupId(testData.azureBillingProfile.getManagedResourceGroupId)
      )

    wsmDao.cloneWorkspace(
      testData.azureWorkspace.workspaceIdAsUUID,
      workspaceId,
      "my-workspace-clone",
      testData.azureBillingProfile,
      testContext,
      Some("the-moon")
    )

    verify(workspaceApi).cloneWorkspace(expectedRequest, testData.azureWorkspace.workspaceIdAsUUID)
  }

  behavior of "encode/decode ShortUuid"
  it should "encode and decode a UUID to the same value" in {
    val uuid = UUID.randomUUID()
    val encoded = WorkspaceManagerDAO.encodeShortUUID(uuid)
    val decoded = WorkspaceManagerDAO.decodeShortUuid(encoded)
    decoded shouldBe Some(uuid)
  }

  it should "encode and decode a Short UUID to the same value" in {
    val shortUuid = ShortUUID.get()
    val decoded = WorkspaceManagerDAO.decodeShortUuid(shortUuid)
    decoded shouldBe defined
    val encoded = WorkspaceManagerDAO.encodeShortUUID(decoded.get)
    encoded shouldBe shortUuid
  }
}
