package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, MockLeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.broadinstitute.dsde.rawls.model.WorkspaceType.McWorkspace
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeName, AttributeString, ErrorReport, GoogleProjectId, RawlsBillingProject, RawlsRequestContext, SamBillingProjectActions, SamResourceTypeNames, SamUserStatusResponse, SamWorkspaceActions, Workspace, WorkspaceCloudPlatform, WorkspaceDeletionResult, WorkspaceName, WorkspacePolicy, WorkspaceRequest, WorkspaceState, WorkspaceType, WorkspaceVersions}
import org.broadinstitute.dsde.rawls.workspace.MultiCloudWorkspaceService.getStorageContainerName
import org.broadinstitute.dsde.workbench.client.leonardo
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString, eq => equalTo}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with OptionValues
    with TestDriverComponent {
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")
  implicit val workbenchMetricBaseName: ShardId = "test"

  def activeMcWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    MultiCloudWorkspaceManagerConfig("fake_app_id", 60 seconds, 120 seconds),
    AzureConfig(
      "fake-landing-zone-definition",
      "fake-protected-landing-zone-definition",
      "fake-landing-zone-version",
      Map("fake_parameter" -> "fake_value"),
      Map("fake_parameter" -> "fake_value"),
      landingZoneAllowAttach = false
    )
  )

  behavior of "createMultiCloudOrRawlsWorkspace"

  it should "return forbidden if the user does not have the createWorkspace action for the billing project" in {
    val samDAO = mock[SamDAO]
    when(
      samDAO.userHasAction(SamResourceTypeNames.billingProject,
                           testData.azureBillingProject.projectName.value,
                           SamBillingProjectActions.createWorkspace,
                           testContext
      )
    ).thenReturn(Future.successful(false))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(testData.azureBillingProject)))
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(bpmDAO.getBillingProfile(any(), any())).thenReturn(Some(testData.azureBillingProfile))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      bpmDAO,
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      billingRepository
    )
    val workspaceRequest = WorkspaceRequest(
      testData.azureBillingProject.projectName.value,
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudOrRawlsWorkspace(
                     workspaceRequest,
                     mock[WorkspaceService]
                   ),
                   Duration.Inf
      )
    }

    actual.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if the billing profile is not found" in {
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getBillingProfile(any[UUID], any[RawlsRequestContext])).thenReturn(None)
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(testData.azureBillingProject)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      bpmDAO,
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      billingRepository
    )
    val request = WorkspaceRequest(
      "fake_mc_billing_project_name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudOrRawlsWorkspace(request, mock[WorkspaceService]),
        Duration.Inf
      )
    }

    assert(actual.errorReport.message.contains("Unable to find billing profile"))
  }

  behavior of "createMultiCloudWorkspace"

  it should "not delete the original workspace if a workspace with the same name already exists" in {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val samDAO = new MockSamDAO(slickDataSource)
    val namespace = "fake"
    val name = "fake-name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceRepository = mock[WorkspaceRepository]

    doAnswer(_ =>
      throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"Workspace '$name' already exists"))
    ).when(workspaceRepository)
      .createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(workspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any)
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )
    val request = WorkspaceRequest(namespace, name, Map.empty)

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())), Duration.Inf)
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
    // Make sure that the pre-existing workspace was not deleted.
    verify(workspaceManagerDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
    verify(workspaceRepository, times(0)).deleteWorkspace(workspaceName)
  }

  it should "throw an exception if the billing profile was created before 9/12/2023" in {
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )
    val request = WorkspaceRequest(
      "fake",
      s"fake-name-${UUID.randomUUID().toString}",
      Map.empty
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudWorkspace(
                     request,
                     new ProfileModel().id(UUID.randomUUID()).createdDate("2023-09-11T22:20:48.949Z")
                   ),
                   Duration.Inf
      )
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "not throw an exception for billing profiles created 9/12/2023 or later" in {
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    // calls assertBillingProfileCreationDate directly instead of createMultiCloudWorkspace
    // so we don't need to mock all the successes
    // we already know createMultiCloudWorkspace is calling assertBillingProfileCreationDate, because of the test above
    service.assertBillingProfileCreationDate(
      new ProfileModel().id(UUID.randomUUID()).createdDate("2023-09-12T22:20:48.949Z")
    ) shouldBe ()

  }

  it should "deploy a WDS instance during workspace creation" in {
    val namespace = "fake_ns"
    val name = "fake_name"
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID())

    val workspaceRepository = mock[WorkspaceRepository]
    val workspaceId = UUID.randomUUID()
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Ready
    )
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))

    val workspaceJobId = UUID.randomUUID()
    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(UUID.fromString(workspace.workspaceId))

    when(
      workspaceManagerDAO
        .createWorkspaceWithSpendProfile(
          any(),
          ArgumentMatchers.eq(name),
          ArgumentMatchers.eq(billingProfile.getId.toString),
          ArgumentMatchers.eq(namespace),
          any[Seq[String]],
          ArgumentMatchers.eq(CloudPlatform.AZURE),
          any[Option[WsmPolicyInputs]],
          ArgumentMatchers.eq(testContext)
        )
    ).thenReturn(
      jobReport
    )
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    val azureStorageContainerId = UUID.randomUUID()
    val containerResult = new CreatedControlledAzureStorageContainer().resourceId(azureStorageContainerId)
    when(
      workspaceManagerDAO.createAzureStorageContainer(
        workspaceId,
        MultiCloudWorkspaceService.getStorageContainerName(workspaceId),
        testContext
      )
    )
      .thenReturn(containerResult)

    val leonardoDAO: LeonardoDAO = mock[LeonardoDAO]
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val request = WorkspaceRequest(
      namespace,
      name,
      Map.empty
    )
    val result: Workspace =
      Await.result(service.createMultiCloudWorkspaceInt(request, workspaceId, billingProfile, testContext),
                   Duration.Inf
      )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(name),
        any(), // spend profile id
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        any[Option[WsmPolicyInputs]],
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(leonardoDAO)
      .createWDSInstance(
        ArgumentMatchers.eq("token"),
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(None)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId))),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "not deploy a WDS instance during workspace creation if test attribute is set as a boolean" in {
    val namespace = "fake_ns"
    val name = "fake_name"
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID())

    val workspaceRepository = mock[WorkspaceRepository]
    val workspaceId = UUID.randomUUID()
    val attributes = Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeBoolean(true))

    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = attributes,
      state = WorkspaceState.Ready
    )
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.eq(attributes),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))

    val workspaceJobId = UUID.randomUUID()
    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(UUID.fromString(workspace.workspaceId))

    when(
      workspaceManagerDAO
        .createWorkspaceWithSpendProfile(
          any(),
          ArgumentMatchers.eq(name),
          ArgumentMatchers.eq(billingProfile.getId.toString),
          ArgumentMatchers.eq(namespace),
          any[Seq[String]],
          ArgumentMatchers.eq(CloudPlatform.AZURE),
          any[Option[WsmPolicyInputs]],
          ArgumentMatchers.eq(testContext)
        )
    ).thenReturn(
      jobReport
    )
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    val azureStorageContainerId = UUID.randomUUID()
    val containerResult = new CreatedControlledAzureStorageContainer().resourceId(azureStorageContainerId)
    when(
      workspaceManagerDAO.createAzureStorageContainer(
        workspaceId,
        MultiCloudWorkspaceService.getStorageContainerName(workspaceId),
        testContext
      )
    )
      .thenReturn(containerResult)

    val leonardoDAO: LeonardoDAO = mock[LeonardoDAO]
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val request = WorkspaceRequest(
      namespace,
      name,
      attributes
    )
    val result: Workspace =
      Await.result(service.createMultiCloudWorkspaceInt(request, workspaceId, billingProfile, testContext),
                   Duration.Inf
      )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(name),
        any(), // spend profile id
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        any[Option[WsmPolicyInputs]],
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(leonardoDAO, never())
      .createWDSInstance(
        ArgumentMatchers.eq("token"),
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(None)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId))),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "fail and rollback workspace creation on initial WSM workspace creation failure" in {
    val namespace = "fake_ns"
    val name = "fake_name"
    val workspaceId = UUID.randomUUID()
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Ready
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    // The primary failure
    doAnswer(_ => throw new ApiException(404, "i've never seen that workspace in my life"))
      .when(wsmDAO)
      .createWorkspaceWithSpendProfile(any(), any(), any(), any(), any(), any(), any(), any())
    val deleteJobId = UUID.randomUUID()
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any()))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString).status(StatusEnum.SUCCEEDED)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()),
          testContext
        ),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any())
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext)
  }

  it should "fail and rollback workspace creation on async workspace creation job failure in WSM" in {
    val namespace = "fake_ns"
    val name = "fake_name"
    val workspaceId = UUID.randomUUID()
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Ready
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val createJobId = UUID.randomUUID()
    when(wsmDAO.createWorkspaceWithSpendProfile(any(), any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().id(createJobId.toString)))
    // This is the primary failure we're testing
    when(wsmDAO.getCreateWorkspaceResult(createJobId.toString, testContext))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().status(StatusEnum.FAILED)))
    val deleteJobId = UUID.randomUUID()
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any()))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString).status(StatusEnum.SUCCEEDED)))

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()),
          testContext
        ),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any())
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext)
  }

  it should "fail and rollback workspace creation on container creation failure in WSM " in {
    val namespace = "fake_ns"
    val name = "fake_name"
    val workspaceId = UUID.randomUUID()
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Ready
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val createJobId = UUID.randomUUID()
    when(wsmDAO.createWorkspaceWithSpendProfile(any(), any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().id(createJobId.toString)))
    when(wsmDAO.getCreateWorkspaceResult(createJobId.toString, testContext))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().status(StatusEnum.SUCCEEDED)))
    val deleteJobId = UUID.randomUUID()
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any()))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString).status(StatusEnum.SUCCEEDED)))
    // The primary failure
    doAnswer(_ => throw new ApiException(500, "what's a container?"))
      .when(wsmDAO)
      .createAzureStorageContainer(
        workspaceId,
        MultiCloudWorkspaceService.getStorageContainerName(workspaceId),
        testContext
      )

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()),
          testContext
        ),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any())
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext)
  }

  it should "still delete from the database when cleaning up the workspace in WSM fails" in {
    val namespace = "fake_ns"
    val name = "fake_name"
    val workspaceId = UUID.randomUUID()
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Ready
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    doAnswer(_ => throw new ApiException(404, "i've never seen that workspace in my life"))
      .when(wsmDAO)
      .createWorkspaceWithSpendProfile(any(), any(), any(), any(), any(), any(), any(), any())
    doAnswer(_ => throw new ApiException(404, "Workspace not found because it wasn't created successfully"))
      .when(wsmDAO)
      .deleteWorkspaceV2(any(), any(), any())
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()),
          testContext
        ),
        Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(wsmDAO).deleteWorkspaceV2(any(), any(), any())
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
  }

  // TODO replace with a test that just checks the result of the transformation is passed
  it should "create a workspace with the requested policies" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[MockLeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val policies = List(
      WorkspacePolicy("protected-data", "terra", List.empty),
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      protectedData = None,
      policies = Some(policies)
    )

    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_name"),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        ArgumentMatchers.eq(
          Some(
            new WsmPolicyInputs()
              .inputs(
                Seq(
                  new WsmPolicyInput()
                    .name("protected-data")
                    .namespace("terra"),
                  new WsmPolicyInput()
                    .name("group-constraint")
                    .namespace("terra")
                    .additionalData(List(new WsmPolicyPair().key("group").value("myFakeGroup")).asJava),
                  new WsmPolicyInput()
                    .name("region-constraint")
                    .namespace("other-namespace")
                    .additionalData(
                      List(new WsmPolicyPair().key("key1").value("value1"),
                           new WsmPolicyPair().key("key2").value("value2")
                      ).asJava
                    )
                ).asJava
              )
          )
        ),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO, Mockito.times(0))
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.any[UUID](),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any(),
        any(),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.any()
      )
  }

  behavior of "buildPolicyInputs"

  it should "transform the policy inputs from the request" in {
    val requestPolicies = List(
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(
      "namespace",
      "fake_name",
      Map.empty,
      policies = Some(requestPolicies)
    )
    val policies = MultiCloudWorkspaceService.buildPolicyInputs(request)
    policies shouldBe Some(
      new WsmPolicyInputs()
        .inputs(
          Seq(
            new WsmPolicyInput()
              .name("group-constraint")
              .namespace("terra")
              .additionalData(List(new WsmPolicyPair().key("group").value("myFakeGroup")).asJava),
            new WsmPolicyInput()
              .name("region-constraint")
              .namespace("other-namespace")
              .additionalData(
                List(new WsmPolicyPair().key("key1").value("value1"),
                     new WsmPolicyPair().key("key2").value("value2")
                ).asJava
              )
          ).asJava
        )
    )

  }

  it should "create a policy for the protected data flag" in {
    val request = WorkspaceRequest(
      "namespace",
      "fake_name",
      Map.empty,
      protectedData = Some(true)
    )

    val policies = MultiCloudWorkspaceService.buildPolicyInputs(request)
    policies shouldBe Some(
      new WsmPolicyInputs()
        .inputs(
          Seq(
            new WsmPolicyInput()
              .name("protected-data")
              .namespace("terra")
              .additionalData(List().asJava)
          ).asJava
        )
    )
  }

  it should "merge the protected data policy with other inputs in the request" in {
    val requestPolicies = List(
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(
      "namespace",
      "fake_name",
      Map.empty,
      policies = Some(requestPolicies),
      protectedData = Some(true)
    )
    val policies = MultiCloudWorkspaceService.buildPolicyInputs(request)
    policies shouldBe Some(
      new WsmPolicyInputs()
        .inputs(
          Seq(
            new WsmPolicyInput()
              .name("protected-data")
              .namespace("terra")
              .additionalData(List().asJava),
            new WsmPolicyInput()
              .name("group-constraint")
              .namespace("terra")
              .additionalData(List(new WsmPolicyPair().key("group").value("myFakeGroup")).asJava),
            new WsmPolicyInput()
              .name("region-constraint")
              .namespace("other-namespace")
              .additionalData(
                List(new WsmPolicyPair().key("key1").value("value1"),
                     new WsmPolicyPair().key("key2").value("value2")
                ).asJava
              )
          ).asJava
        )
    )
  }

  behavior of "cloneMultiCloudWorkspace"

  def withMockedMultiCloudWorkspaceService(runTest: MultiCloudWorkspaceService => Assertion,
                                           samDAO: SamDAO = new MockSamDAO(slickDataSource)
  ): Assertion = {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    val leonardoDAO: LeonardoDAO = spy(new MockLeonardoDAO())

    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpmDAO,
        new MockSamDAO(slickDataSource),
        config,
        leonardoDAO,
        workbenchMetricBaseName
      )(testContext)
    )

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.azureBillingProject.projectName), any())

    doReturn(Future.successful(testData.billingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.billingProject.projectName), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpmDAO)
      .getBillingProfile(equalTo(testData.azureBillingProfile.getId), any())

    // For testing of old Azure billing projects, can be removed if that code is removed.
    doReturn(Future.successful(testData.oldAzureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.oldAzureBillingProject.projectName), any())
    doReturn(Some(testData.oldAzureBillingProfile))
      .when(bpmDAO)
      .getBillingProfile(equalTo(testData.oldAzureBillingProfile.getId), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getV2WorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getV2WorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    runTest(mcWorkspaceService)
  }

  it should "fail if the destination workspace already exists" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.azureWorkspace)
              _ <- mcWorkspaceService.cloneMultiCloudWorkspace(
                mock[WorkspaceService](RETURNS_SMART_NULLS),
                testData.azureWorkspace.toWorkspaceName,
                WorkspaceRequest(
                  testData.azureWorkspace.namespace,
                  testData.azureWorkspace.name,
                  Map.empty
                )
              )
            } yield fail(
              "cloneMultiCloudWorkspace does not fail when a workspace " +
                "with the same name already exists."
            ),
            Duration.Inf
          )
        }

        result.errorReport.statusCode.value shouldBe StatusCodes.Conflict
      }
    }

  it should "not create a workspace record if the request to Workspace Manager fails" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.cloneWorkspace(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                any(),
                                                                equalTo("kifflom"),
                                                                any(),
                                                                equalTo(testData.azureWorkspace.namespace),
                                                                any(),
                                                                any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.ImATeapot, "short and stout"))
        )

        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        // preserve the error workspace manager returned
        result.errorReport.statusCode.value shouldBe StatusCodes.ImATeapot

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "throw an exception if the billing profile was created before 9/12/2023" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.oldAzureBillingProject.projectName.value, "unsupported")
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.azureWorkspace)
              _ <- mcWorkspaceService.cloneMultiCloudWorkspace(
                mock[WorkspaceService](RETURNS_SMART_NULLS),
                testData.azureWorkspace.toWorkspaceName,
                WorkspaceRequest(
                  cloneName.namespace,
                  cloneName.name,
                  Map.empty
                )
              )
            } yield fail(
              "cloneMultiCloudWorkspace does not fail when a workspace " +
                "with the same name already exists."
            ),
            Duration.Inf
          )
        }
        result.errorReport.statusCode.value shouldBe StatusCodes.Forbidden
      }
    }

  it should "not create a workspace record if the Workspace Manager clone operation fails" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.getCloneWorkspaceResult(
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) => MockWorkspaceManagerDAO.getCloneWorkspaceResult(StatusEnum.FAILED))

        intercept[WorkspaceManagerOperationFailureException] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if the workspace has no storage containers" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        // workspaceManagerDAO returns an empty ResourceList by default for enumerateStorageContainers

        val actual = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        assert(actual.errorReport.message.contains("does not have the expected storage container"))

        verify(mcWorkspaceService.workspaceManagerDAO, never())
          .cloneAzureStorageContainer(any(), any(), any(), any(), any(), any(), any())

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if there is no storage container with the correct name" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          new ResourceList().addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name("not the correct name")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
        )

        val actual = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        assert(actual.errorReport.message.contains("does not have the expected storage container"))

        verify(mcWorkspaceService.workspaceManagerDAO, never())
          .cloneAzureStorageContainer(any(), any(), any(), any, any(), any(), any())

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not allow users to clone azure workspaces into gcp billing projects" in
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              testData.billingProject.projectName.value,
              UUID.randomUUID().toString,
              Map.empty
            )
          ),
          Duration.Inf
        )
      }

      result.errorReport.statusCode.value shouldBe StatusCodes.BadRequest
    }

  it should "not allow users to clone gcp workspaces into azure billing projects" in
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.workspace.toWorkspaceName,
            WorkspaceRequest(
              testData.azureBillingProject.projectName.value,
              UUID.randomUUID().toString,
              Map.empty
            )
          ),
          Duration.Inf
        )
      }

      result.errorReport.statusCode.value shouldBe StatusCodes.BadRequest
    }

  it should "deploy a WDS instance during workspace clone" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
      when(
        mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
          equalTo(testData.azureWorkspace.workspaceIdAsUUID),
          any(),
          any(),
          any()
        )
      ).thenAnswer((_: InvocationOnMock) =>
        new ResourceList().addResourcesItem(
          new ResourceDescription().metadata(
            new ResourceMetadata()
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      Await.result(
        for {
          _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
          clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              cloneName.namespace,
              cloneName.name,
              Map.empty
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning deploys WDS and then a very simple
          // validation of the clone success.
          verify(mcWorkspaceService.leonardoDAO, times(1))
            .createWDSInstance(anyString(),
                               equalTo(clone.toWorkspace.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspace.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe Some(McWorkspace)
          clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
        },
        Duration.Inf
      )
    }
  }

  it should "not deploy a WDS instance during workspace clone if test attribute is set as a string" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
      when(
        mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
          equalTo(testData.azureWorkspace.workspaceIdAsUUID),
          any(),
          any(),
          any()
        )
      ).thenAnswer((_: InvocationOnMock) =>
        new ResourceList().addResourcesItem(
          new ResourceDescription().metadata(
            new ResourceMetadata()
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      Await.result(
        for {
          _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
          clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              cloneName.namespace,
              cloneName.name,
              Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeString("true"))
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning does not deploy WDS.
          verify(mcWorkspaceService.leonardoDAO, never())
            .createWDSInstance(anyString(),
                               equalTo(clone.toWorkspace.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspace.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe Some(McWorkspace)
          clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
        },
        Duration.Inf
      )
    }
  }

  it should "clone the workspace even if WDS instance creation fails" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
      when(
        mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
          equalTo(testData.azureWorkspace.workspaceIdAsUUID),
          any(),
          any(),
          any()
        )
      ).thenAnswer((_: InvocationOnMock) =>
        new ResourceList().addResourcesItem(
          new ResourceDescription().metadata(
            new ResourceMetadata()
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      // for this test, throw an error on WDS deployment
      when(mcWorkspaceService.leonardoDAO.createWDSInstance(anyString(), any[UUID](), any()))
        .thenAnswer(_ => throw new leonardo.ApiException(500, "intentional Leo exception for unit test"))

      Await.result(
        for {
          _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
          clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              cloneName.namespace,
              cloneName.name,
              Map.empty
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning deploys WDS and then a very simple
          // validation of the clone success.
          verify(mcWorkspaceService.leonardoDAO, times(1))
            .createWDSInstance(anyString(),
                               equalTo(clone.toWorkspace.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspace.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe Some(McWorkspace)
          clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
        },
        Duration.Inf
      )
    }
  }

  it should
    "clone an azure workspace" +
    " & create a new workspace record with merged attributes" +
    " & create a new job for the workspace manager resource monitor" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        val sourceContainerUUID = UUID.randomUUID()
        when(
          mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          new ResourceList().addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(sourceContainerUUID)
                .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
        )
        Await.result(
          for {
            _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
            clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map(
                  AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
                ),
                None,
                Some("analyses/"),
                policies = Some(
                  List(
                    WorkspacePolicy("dummy-policy-name", "terra", List.empty)
                  )
                )
              )
            )
            // Ensure test data has the attribute we expect to merge in.
            _ = testData.azureWorkspace.attributes shouldBe Map(
              AttributeName.withDefaultNS("description") -> AttributeString("source description")
            )
            _ = clone.toWorkspace.toWorkspaceName shouldBe cloneName
            _ = clone.workspaceType shouldBe Some(McWorkspace)
            _ = clone.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
            _ = clone.attributes.get shouldBe Map(
              AttributeName.withDefaultNS("description") -> AttributeString("source description"),
              AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
            )

            jobs <- slickDataSource.inTransaction { access =>
              for {
                // the newly cloned workspace should be persisted
                clone_ <- access.workspaceQuery.findByName(cloneName)
                _ = clone_.value.workspaceId shouldBe clone.workspaceId

                // a new resource monitor job should be created
                jobs <- access.WorkspaceManagerResourceMonitorRecordQuery
                  .selectByWorkspaceId(clone.toWorkspace.workspaceIdAsUUID)
              } yield jobs
            }
          } yield {
            verify(mcWorkspaceService.workspaceManagerDAO, times(1))
              .cloneWorkspace(
                equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                equalTo(clone.toWorkspace.workspaceIdAsUUID),
                equalTo(cloneName.name),
                any(),
                equalTo(cloneName.namespace),
                any(),
                equalTo(
                  Some(
                    new WsmPolicyInputs()
                      .inputs(
                        Seq(
                          new WsmPolicyInput()
                            .name("dummy-policy-name")
                            .namespace("terra")
                        ).asJava
                      )
                  )
                )
              )
            verify(mcWorkspaceService.workspaceManagerDAO, times(1))
              .cloneAzureStorageContainer(
                equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                equalTo(clone.toWorkspace.workspaceIdAsUUID),
                equalTo(sourceContainerUUID),
                equalTo(getStorageContainerName(clone.toWorkspace.workspaceIdAsUUID)),
                equalTo(CloningInstructionsEnum.RESOURCE),
                equalTo(Some("analyses/")),
                any()
              )
            clone.completedCloneWorkspaceFileTransfer shouldBe None
            jobs.size shouldBe 1
            jobs.head.jobType shouldBe JobType.CloneWorkspaceContainerResult
            jobs.head.workspaceId.value.toString shouldBe clone.workspaceId
          },
          Duration.Inf
        )
      }
    }

  behavior of "deleteMultiCloudOrRawlsWorkspaceV2"

  it should "call WorkspaceService to delete a rawls workspace synchronously" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString,SamWorkspaceActions.delete,testContext))
      .thenReturn(Future(true))
    val workspaceRepository = mock[WorkspaceRepository]
    val workspace = Workspace(
      namespace,
      name,
      workspaceId.toString,
      "test-bucket-name",
      None,
      DateTime.now(),
      DateTime.now(),
      "",
      Map.empty,
      false,
      WorkspaceVersions.V2,
      GoogleProjectId("project-id"),
      None,
      None,
      None,
      None,
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
    )
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val workspaceService = mock[WorkspaceService]
    val deleteResult = WorkspaceDeletionResult(None, None)
    when(workspaceService.deleteWorkspace(workspaceName)).thenReturn(Future(deleteResult))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    Await.result(
      service.deleteMultiCloudOrRawlsWorkspaceV2(workspaceName, workspaceService),
      Duration.Inf
    ) shouldBe deleteResult
    verify(workspaceService).deleteWorkspace(workspaceName)
  }

  it should "create a new job for the workspace manager resource monitor when deleting an azure workspace" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Ready
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    when(workspaceRepository.updateState(workspace.workspaceIdAsUUID, WorkspaceState.Deleting)).thenReturn(Future(1))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    val recordCaptor = ArgumentCaptor.forClass(classOf[WorkspaceManagerResourceMonitorRecord])
    when(monitorRecordDao.create(recordCaptor.capture())).thenReturn(Future())

    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      monitorRecordDao,
      workspaceRepository,
      mock[BillingRepository]
    )
    val deletionResult = Await.result(
      service.deleteMultiCloudOrRawlsWorkspaceV2(workspaceName, mock[WorkspaceService]),
      Duration.Inf
    )
    val record: WorkspaceManagerResourceMonitorRecord = recordCaptor.getValue
    record.workspaceId shouldBe Some(workspaceId)
    record.jobType shouldBe WorkspaceManagerResourceMonitorRecord.JobType.WorkspaceDeleteInit
    record.jobControlId shouldBe deletionResult.jobId.map(UUID.fromString).get
  }

  it should "fail with a conflict if the workspace is already deleting" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Deleting
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
       Await.result(service.deleteMultiCloudOrRawlsWorkspaceV2(workspaceName, mock[WorkspaceService]), Duration.Inf)
     }

    result.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  it should "fail with an error if the workspace is in an undeleteable state" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Creating
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.deleteMultiCloudOrRawlsWorkspaceV2(workspaceName, mock[WorkspaceService]), Duration.Inf)
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  behavior of "deleteMultiCloudOrRawlsWorkspace"

  it should "fail if user does not have delete permissions on the workspace" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(false))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.read, testContext))
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Creating
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
          service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService])
,        Duration.Inf
      )
    }

    result.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
  }

  it should "delete an MC workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.azureWorkspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield result shouldBe None,
          Duration.Inf
        )

        verify(mcWorkspaceService).deleteWorkspaceInWSM(equalTo(testData.azureWorkspace.workspaceIdAsUUID))
        assertWorkspaceGone(testData.azureWorkspace)
      }
    }
  }

  it should "delete a rawls workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.workspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
        when(workspaceService.deleteWorkspace(ArgumentMatchers.eq(wsName)))
          .thenReturn(
            Future.successful(WorkspaceDeletionResult.fromGcpBucketName(testData.workspace.bucketName))
          )

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.workspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield {
            verify(workspaceService).deleteWorkspace(equalTo(wsName))
            result shouldBe Some(testData.workspace.bucketName)
          },
          Duration.Inf
        )
      }
    }
  }

  it should "leave the rawls state in place if WSM returns an error during workspace deletion" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Creating
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ =>
      throw new ApiException(500, "error")
    )
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]), Duration.Inf)
    }

    verify(wsmDAO).getWorkspace(workspaceId, testContext)
    verify(wsmDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
    verify(workspaceRepository, times(0)).deleteWorkspace(workspace)

  }

  it should "delete the rawls record if the WSM record is not found" in {
    val namespace = "ws_namespace"
    val name = "ws_name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceId = UUID.randomUUID()
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = Workspace.buildMcWorkspace(
      namespace = namespace,
      name = name,
      workspaceId = workspaceId.toString,
      DateTime.now(),
      DateTime.now(),
      createdBy = testContext.userInfo.userEmail.value,
      attributes = Map.empty,
      state = WorkspaceState.Creating
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    when(workspaceRepository.deleteWorkspace(workspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ =>
      throw new ApiException(404, "not found")
    )
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = Await.result(service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]), Duration.Inf)
    result shouldBe None

    verify(wsmDAO).getWorkspace(workspaceId, testContext)
    verify(wsmDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
    verify(workspaceRepository).deleteWorkspace(workspace)
  }

  behavior of "deleteWorkspaceInWSM"

  it should "not attempt to delete a workspace and raise an exception if WSM returns a failure when getting the workspace" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(403, "forbidden")
    )

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.errorReport.statusCode shouldEqual Some(StatusCodes.InternalServerError)
    verify(svc.workspaceManagerDAO, times(0)).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                anyString(),
                                                                any[RawlsRequestContext]
    )
  }

  it should "not attempt to delete a workspace in WSM that does not exist in WSM" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(404, "workspace not found")
    )

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO, times(0)).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                anyString(),
                                                                any[RawlsRequestContext]
    )
  }

  it should "fail for for non 403 errors" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(
      _ => throw new ApiException(StatusCodes.BadRequest.intValue, "failure")
    )
    val samDAO = new MockSamDAO(slickDataSource)
    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[ApiException] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(1)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "fail for for non-ApiException errors" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(
      _ => throw new Exception("failure")
    )
    val samDAO = new MockSamDAO(slickDataSource)
    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[Exception] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(1)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "delete the rawls workspace when workspace manager returns 403 during polling" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0

      override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                              jobControlId: String,
                                              ctx: RawlsRequestContext
      ): JobResult = {
        times = times + 1
        if (times > 1) {
          throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden")
        } else {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.RUNNING))
        }
      }
    })

    val samDAO = new MockSamDAO(slickDataSource)

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(2)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "start deletion and poll for success when the workspace exists in workspace manager" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0
      override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                              jobControlId: String,
                                              ctx: RawlsRequestContext
      ): JobResult = {
        times = times + 1
        if (times > 1) {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.SUCCEEDED))
        } else {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.RUNNING))
        }
      }
    })

    val samDAO = new MockSamDAO(slickDataSource)

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(2)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  private def assertWorkspaceState(workspace: Workspace, expectedState: WorkspaceState) = {
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findByName(workspace.toWorkspaceName)),
      Duration.Inf
    )

    existing.get.state shouldBe expectedState
  }

  private def assertWorkspaceGone(workspace: Workspace) = {
    // fail if the workspace exists
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findByName(workspace.toWorkspaceName)),
      Duration.Inf
    )

    existing shouldBe empty
  }

  private def assertWorkspaceExists(workspace: Workspace) = {
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findById(workspace.workspaceId)),
      Duration.Inf
    )

    existing.get.workspaceId shouldBe workspace.workspaceId
  }

  private def insertWorkspaceWithBillingProject(billingProject: RawlsBillingProject, workspace: Workspace) =
    slickDataSource.inTransaction { access =>
      access.rawlsBillingProjectQuery.create(billingProject) >> access.workspaceQuery.createOrUpdate(workspace)
    }
}
