package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import bio.terra.workspace.api._
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.jdk.CollectionConverters._

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
                           landingZonesApi: LandingZonesApi = mock[LandingZonesApi],
                           resourceApi: ResourceApi = mock[ResourceApi]
  ): WorkspaceManagerApiClientProvider = new WorkspaceManagerApiClientProvider {
    override def getApiClient(ctx: RawlsRequestContext): ApiClient = ???

    override def getWorkspaceApplicationApi(ctx: RawlsRequestContext): WorkspaceApplicationApi =
      workspaceApplicationApi

    override def getControlledAzureResourceApi(ctx: RawlsRequestContext): ControlledAzureResourceApi =
      controlledAzureResourceApi

    override def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi =
      workspaceApi

    override def getLandingZonesApi(ctx: RawlsRequestContext): LandingZonesApi = landingZonesApi

    override def getResourceApi(ctx: RawlsRequestContext): ResourceApi = resourceApi

    override def getJobsApi(ctx: RawlsRequestContext): JobsApi = ???

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
                                             CloningInstructionsEnum.NOTHING,
                                           expectedNameSuffix: String = workspaceId.toString
  ): Unit = {
    commonFields.getName should endWith(expectedNameSuffix)
    commonFields.getCloningInstructions shouldBe expectedCloningInstructions
    commonFields.getAccessScope shouldBe AccessScope.SHARED_ACCESS
    commonFields.getManagedBy shouldBe ManagedBy.USER
  }

  behavior of "createAzureStorageContainer"

  it should "call the WSM controlled azure resource API with a SA id" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val scArgumentCaptor = captor[CreateControlledAzureStorageContainerRequestBody]
    val containerName = "containerName"
    wsmDao.createAzureStorageContainer(workspaceId, containerName, testContext)
    verify(controlledAzureResourceApi).createAzureStorageContainer(scArgumentCaptor.capture, any[UUID])
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageContainerName shouldBe containerName
    assertControlledResourceCommonFields(scArgumentCaptor.getValue.getCommon,
                                         CloningInstructionsEnum.NOTHING,
                                         containerName
    )
  }

  it should "call the WSM controlled azure resource API without a SA id" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))
    val containerName = "containerName"
    val scArgumentCaptor = captor[CreateControlledAzureStorageContainerRequestBody]
    wsmDao.createAzureStorageContainer(workspaceId, containerName, testContext)
    verify(controlledAzureResourceApi).createAzureStorageContainer(scArgumentCaptor.capture, any[UUID])
    scArgumentCaptor.getValue.getAzureStorageContainer.getStorageContainerName shouldBe containerName
    assertControlledResourceCommonFields(scArgumentCaptor.getValue.getCommon,
                                         CloningInstructionsEnum.NOTHING,
                                         containerName
    )
  }

  behavior of "cloneAzureStorageContainer"

  it should "call the WSM controlled azure resource api" in {
    val controlledAzureResourceApi = mock[ControlledAzureResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(controlledAzureResourceApi = controlledAzureResourceApi))

    val cloneArgumentCaptor = captor[CloneControlledAzureStorageContainerRequest]

    val destinationWorkspaceUUID = UUID.randomUUID()
    val sourceContainerID = UUID.randomUUID()
    val destinationContainerName = "containerName"
    val cloningInstructions = CloningInstructionsEnum.DEFINITION
    val prefixToClone = "prefix/"

    // Call first with a prefix to limit cloning
    wsmDao.cloneAzureStorageContainer(workspaceId,
                                      destinationWorkspaceUUID,
                                      sourceContainerID,
                                      destinationContainerName,
                                      cloningInstructions,
                                      Some(prefixToClone),
                                      testContext
    )

    // Call second with no prefix
    wsmDao.cloneAzureStorageContainer(workspaceId,
                                      destinationWorkspaceUUID,
                                      sourceContainerID,
                                      destinationContainerName,
                                      cloningInstructions,
                                      None,
                                      testContext
    )
    verify(controlledAzureResourceApi, Mockito.times(2)).cloneAzureStorageContainer(
      cloneArgumentCaptor.capture,
      ArgumentMatchers.eq(workspaceId),
      ArgumentMatchers.eq(sourceContainerID)
    )

    cloneArgumentCaptor.getValue.getDestinationWorkspaceId shouldBe destinationWorkspaceUUID
    cloneArgumentCaptor.getValue.getName shouldBe destinationContainerName
    cloneArgumentCaptor.getValue.getCloningInstructions shouldBe cloningInstructions
    cloneArgumentCaptor.getAllValues.get(0).getPrefixesToClone shouldEqual (java.util.List.of(prefixToClone))
    cloneArgumentCaptor.getAllValues.get(1).getPrefixesToClone shouldEqual (java.util.List.of())
  }

  behavior of "enumerateStorageContainers"

  it should "call the WSM resource api" in {
    val resourceApi = mock[ResourceApi]
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(resourceApi = resourceApi))

    wsmDao.enumerateStorageContainers(workspaceId, 10, 200, testContext)
    verify(resourceApi).enumerateResources(workspaceId,
                                           10,
                                           200,
                                           ResourceType.AZURE_STORAGE_CONTAINER,
                                           StewardshipType.CONTROLLED
    )
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
    val landingZoneDefinition = "fake-definition"
    val landingZoneVersion = "fake-version"
    val landingZoneParameters = Map("fake_parameter" -> "fake_value")
    val expectedParameters = List(new AzureLandingZoneParameter().key("fake_parameter").value("fake_value"))

    wsmDao.createLandingZone(landingZoneDefinition,
                             landingZoneVersion,
                             landingZoneParameters,
                             billingProfileId,
                             testContext
    )
    verify(landingZonesApi).createAzureLandingZone(creationArgumentCaptor.capture)
    creationArgumentCaptor.getValue.getBillingProfileId shouldBe billingProfileId
    creationArgumentCaptor.getValue.getDefinition shouldBe landingZoneDefinition
    creationArgumentCaptor.getValue.getVersion shouldBe landingZoneVersion
    creationArgumentCaptor.getValue.getParameters.asScala should contain theSameElementsAs expectedParameters

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

    val billingProjectId = "billing-project-namespace";

    val expectedRequest = new CloneWorkspaceRequest()
      .displayName("my-workspace-clone")
      .destinationWorkspaceId(workspaceId)
      .spendProfile(testData.azureBillingProfile.getId.toString)
      .location("the-moon")
      .projectOwnerGroupId(billingProjectId);

    wsmDao.cloneWorkspace(
      testData.azureWorkspace.workspaceIdAsUUID,
      workspaceId,
      "my-workspace-clone",
      testData.azureBillingProfile,
      billingProjectId,
      testContext,
      Some("the-moon")
    )

    verify(workspaceApi).cloneWorkspace(expectedRequest, testData.azureWorkspace.workspaceIdAsUUID)
  }



  behavior of "deleteWorkspaceV2"

  it should "call the WSM workspace deletion V2 API" in {
    val workspaceApi = mock[WorkspaceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.deleteWorkspaceV2(testData.azureWorkspace.workspaceIdAsUUID, testContext)

    verify(workspaceApi).deleteWorkspaceV2(any[DeleteWorkspaceV2Request],
                                           ArgumentMatchers.eq(testData.azureWorkspace.workspaceIdAsUUID)
    )
  }

  it should "call the WSM workspace deletion result API" in {
    val workspaceApi = mock[WorkspaceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.getDeleteWorkspaceV2Result(testData.azureWorkspace.workspaceIdAsUUID, "test_job_id", testContext)

    verify(workspaceApi).getDeleteWorkspaceV2Result(ArgumentMatchers.eq(testData.azureWorkspace.workspaceIdAsUUID),
                                                    ArgumentMatchers.eq("test_job_id")
    )
  }
}
