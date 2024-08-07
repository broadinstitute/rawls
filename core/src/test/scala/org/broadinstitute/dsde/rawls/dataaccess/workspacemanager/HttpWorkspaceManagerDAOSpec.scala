package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import bio.terra.workspace.api._
import bio.terra.workspace.client.{ApiClient, ApiException}
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions, when}
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
                           resourceApi: ResourceApi = mock[ResourceApi],
                           referencedGcpResourceApi: ReferencedGcpResourceApi = mock[ReferencedGcpResourceApi]
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

    override def getReferencedGcpResourceApi(ctx: RawlsRequestContext): ReferencedGcpResourceApi =
      referencedGcpResourceApi

    override def getControlledGcpResourceApi(ctx: RawlsRequestContext): ControlledGcpResourceApi = ???
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

  it should "return None a 403 is returned to indicate the landing zone is not present when deleting" in {
    val landingZonesApi = mock[LandingZonesApi]
    when(landingZonesApi.deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody], any[UUID]))
      .thenThrow(new ApiException(403, "Forbidden"))
    val wsmDao =
      new HttpWorkspaceManagerDAO(getApiClientProvider(landingZonesApi = landingZonesApi))
    val landingZoneId = UUID.randomUUID()

    wsmDao.deleteLandingZone(landingZoneId, testContext) shouldBe None

    verify(landingZonesApi).deleteAzureLandingZone(any[DeleteAzureLandingZoneRequestBody],
                                                   ArgumentMatchers.eq(landingZoneId)
    )

  }

  behavior of "clone"
  it should "call the WSM workspace API" in {
    val workspaceApi = mock[WorkspaceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    val billingProjectId = "billing-project-namespace";
    val expectedPolicy =
      new WsmPolicyInputs()
        .inputs(
          Seq(
            new WsmPolicyInput()
              .name("dummy-policy")
              .namespace("terra")
              .additionalData(List().asJava)
          ).asJava
        );

    val expectedRequest = new CloneWorkspaceRequest()
      .displayName("my-workspace-clone")
      .destinationWorkspaceId(workspaceId)
      .spendProfile(testData.azureBillingProfile.getId.toString)
      .additionalPolicies(expectedPolicy)
      .projectOwnerGroupId(billingProjectId);

    wsmDao.cloneWorkspace(
      testData.azureWorkspace.workspaceIdAsUUID,
      workspaceId,
      "my-workspace-clone",
      Option(testData.azureBillingProfile),
      billingProjectId,
      testContext,
      Some(expectedPolicy)
    )

    verify(workspaceApi).cloneWorkspace(expectedRequest, testData.azureWorkspace.workspaceIdAsUUID)
  }

  behavior of "createWorkspaceWithSpendProfile"

  it should "call the WSM workspace API" in {
    val workspaceApi = mock[WorkspaceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))
    val policyInputs = new WsmPolicyInputs()
    val protectedPolicyInput = new WsmPolicyInput()
    protectedPolicyInput.name("protected-data")
    protectedPolicyInput.namespace("terra")
    protectedPolicyInput.additionalData(List().asJava)

    policyInputs.addInputsItem(protectedPolicyInput)

    val billingProjectId = "billing-project-namespace";

    wsmDao.createWorkspaceWithSpendProfile(
      testData.azureWorkspace.workspaceIdAsUUID,
      testData.azureWorkspace.name,
      testData.azureBillingProfile.getId.toString,
      billingProjectId,
      Seq("exampleApp"),
      CloudPlatform.AZURE,
      Some(policyInputs),
      testContext
    )

    val createWorkspaceArgumentCaptor = captor[CreateWorkspaceV2Request]
    verify(workspaceApi).createWorkspaceV2(createWorkspaceArgumentCaptor.capture)
    // V2-specific values
    createWorkspaceArgumentCaptor.getValue.getJobControl shouldNot be(null)
    createWorkspaceArgumentCaptor.getValue.getCloudPlatform shouldBe CloudPlatform.AZURE

    // original values
    createWorkspaceArgumentCaptor.getValue.getId shouldBe testData.azureWorkspace.workspaceIdAsUUID
    createWorkspaceArgumentCaptor.getValue.getDisplayName shouldBe testData.azureWorkspace.name
    createWorkspaceArgumentCaptor.getValue.getSpendProfile shouldBe testData.azureBillingProfile.getId.toString
    createWorkspaceArgumentCaptor.getValue.getStage shouldBe WorkspaceStageModel.MC_WORKSPACE
    createWorkspaceArgumentCaptor.getValue.getApplicationIds shouldBe Seq("exampleApp").asJava
    createWorkspaceArgumentCaptor.getValue.getPolicies shouldBe policyInputs
    createWorkspaceArgumentCaptor.getValue.getProjectOwnerGroupId shouldBe billingProjectId
  }

  behavior of "deleteWorkspaceV2"

  it should "call the WSM workspace deletion V2 API" in {
    val workspaceApi = mock[WorkspaceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.deleteWorkspaceV2(testData.azureWorkspace.workspaceIdAsUUID, UUID.randomUUID().toString, testContext)

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

  behavior of "createDataRepoSnapshotReference"

  it should "call the create data repo snapshot reference API" in {
    val referencedGcpResourceApi = mock[ReferencedGcpResourceApi]
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(referencedGcpResourceApi = referencedGcpResourceApi))
    val snapshotId = UUID.randomUUID()

    wsmDao.createDataRepoSnapshotReference(
      testData.workspace.workspaceIdAsUUID,
      snapshotId,
      DataReferenceName("name"),
      Some(DataReferenceDescriptionField("description")),
      "instance name",
      CloningInstructionsEnum.REFERENCE,
      Some(Map("k1" -> "v1")),
      testContext
    )

    val props = new Properties()
    props.add(new Property().key("k1").value("v1"))
    val snapshotRequest = new CreateDataRepoSnapshotReferenceRequestBody()
      .snapshot(new DataRepoSnapshotAttributes().instanceName("instance name").snapshot(snapshotId.toString))
      .metadata(
        new ReferenceResourceCommonFields()
          .name("name")
          .cloningInstructions(CloningInstructionsEnum.REFERENCE)
          .description("description")
          .properties(props)
      )
    verify(referencedGcpResourceApi).createDataRepoSnapshotReference(
      ArgumentMatchers.eq(snapshotRequest),
      ArgumentMatchers.eq(testData.workspace.workspaceIdAsUUID)
    )
  }

  behavior of "listWorkspaces"

  it should "should only make a single request when no workspaces are returned" in {
    val workspaceApi = mock[WorkspaceApi]
    val responseList = new WorkspaceDescriptionList().workspaces(List().asJava)
    when(workspaceApi.listWorkspaces(ArgumentMatchers.eq(0), anyInt(), any())).thenReturn(responseList)
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.listWorkspaces(testContext) shouldBe empty

    verify(workspaceApi).listWorkspaces(ArgumentMatchers.eq(0), anyInt(), any())
  }

  it should "should only make one request when fewer workspaces than the request batch size are returned" in {
    val workspaceApi = mock[WorkspaceApi]
    val ws = new WorkspaceDescription()
    when(workspaceApi.listWorkspaces(ArgumentMatchers.eq(0), ArgumentMatchers.eq(2), any()))
      .thenReturn(new WorkspaceDescriptionList().workspaces(List(ws).asJava))
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.listWorkspaces(testContext, 2) should have length 1

    verify(workspaceApi).listWorkspaces(ArgumentMatchers.eq(0), ArgumentMatchers.eq(2), any())

  }

  it should "should make a follow-up request when exactly the batch size of workspaces are returned" in {
    val workspaceApi = mock[WorkspaceApi]
    val ws = new WorkspaceDescription()
    when(workspaceApi.listWorkspaces(ArgumentMatchers.eq(0), ArgumentMatchers.eq(1), any()))
      .thenReturn(new WorkspaceDescriptionList().workspaces(List(ws).asJava))
    when(workspaceApi.listWorkspaces(ArgumentMatchers.eq(1), ArgumentMatchers.eq(1), any()))
      .thenReturn(new WorkspaceDescriptionList().workspaces(List().asJava))
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    wsmDao.listWorkspaces(testContext, 1) should have length 1

    verify(workspaceApi).listWorkspaces(ArgumentMatchers.eq(0), ArgumentMatchers.eq(1), any())
    verify(workspaceApi).listWorkspaces(ArgumentMatchers.eq(1), ArgumentMatchers.eq(1), any())
    verifyNoMoreInteractions(workspaceApi)
  }

  it should "should combine results from all batches in the result list" in {
    val workspaceApi = mock[WorkspaceApi]

    val workspaces = Array.tabulate[WorkspaceDescription](10) { i =>
      new WorkspaceDescription().displayName(i.toString)
    }
    val batchSize = 3
    val batches = workspaces.grouped(batchSize).toIndexedSeq
    for (i <- batches.indices)
      when(workspaceApi.listWorkspaces(ArgumentMatchers.eq(i * batchSize), ArgumentMatchers.eq(batchSize), any()))
        .thenReturn(new WorkspaceDescriptionList().workspaces(batches(i).toList.asJava))
    val wsmDao = new HttpWorkspaceManagerDAO(getApiClientProvider(workspaceApi = workspaceApi))

    val result = wsmDao.listWorkspaces(testContext, batchSize)

    result should have length 10
    result should contain theSameElementsAs workspaces
    verify(workspaceApi, times(batches.length)).listWorkspaces(any(), ArgumentMatchers.eq(batchSize), any())
  }

}
