package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{
  CloudPlatform,
  CreateWorkspaceV2Result,
  CreatedControlledAzureStorageContainer,
  JobReport,
  JobResult,
  WsmPolicyInput,
  WsmPolicyInputs,
  WsmPolicyPair
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  CreationStatuses,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceTypeNames,
  UserInfo,
  Workspace,
  WorkspaceName,
  WorkspacePolicy,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.joda.time.DateTime
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{doAnswer, doNothing, never, times, verify, when}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.language.postfixOps

class MultiCloudWorkspaceServiceCreateSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with OptionValues {

  implicit val executionContext: TestExecutionContext = new TestExecutionContext()
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")

  val userInfo: UserInfo = UserInfo(RawlsUserEmail("owner-access"),
                                    OAuth2BearerToken("token"),
                                    123,
                                    RawlsUserSubjectId("123456789876543212345")
  )
  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)
  val namespace: String = "fake-namespace"
  val name: String = "fake-name"
  val workspaceName: WorkspaceName = WorkspaceName(namespace, name)
  val workspaceId: UUID = UUID.randomUUID()
  val defaultWorkspace: Workspace = Workspace.buildMcWorkspace(
    namespace = namespace,
    name = name,
    workspaceId = workspaceId.toString,
    DateTime.now(),
    DateTime.now(),
    createdBy = testContext.userInfo.userEmail.value,
    attributes = Map.empty,
    state = WorkspaceState.Ready
  )

  behavior of "createMultiCloudOrRawlsWorkspace"

  it should "return forbidden if the user does not have the createWorkspace action for the billing project" in {
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("azure-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )

    val samDAO = mock[SamDAO]
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(false))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      billingRepository
    )
    val workspaceRequest = WorkspaceRequest(
      billingProject.projectName.value,
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudOrRawlsWorkspace(workspaceRequest, mock[WorkspaceService]), Duration.Inf)
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if the billing profile is not found in BPM" in {
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("azure-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(bpmDAO.getBillingProfile(any[UUID], any[RawlsRequestContext])).thenReturn(None)
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
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
    val workspaceManagerDAO = mock[WorkspaceManagerDAO] // spy(new MockWorkspaceManagerDAO())
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
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
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID())
    val workspaceRepository = mock[WorkspaceRepository]
    val workspaceIdCaptor = ArgumentCaptor.forClass(classOf[UUID])
    when(
      workspaceRepository.createMCWorkspace(
        workspaceIdCaptor.capture(),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(defaultWorkspace))

    val workspaceJobId = UUID.randomUUID()
    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(workspaceId)
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
    ).thenReturn(jobReport)
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    val azureStorageContainerId = UUID.randomUUID()
    val containerResult = new CreatedControlledAzureStorageContainer().resourceId(azureStorageContainerId)
    when(workspaceManagerDAO.createAzureStorageContainer(any, any, any)).thenReturn(containerResult)
    val leonardoDAO: LeonardoDAO = mock[LeonardoDAO]
    doNothing().when(leonardoDAO).createWDSInstance(any(), any(), any())
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val request = WorkspaceRequest(namespace, name, Map.empty)
    val result: Workspace =
      Await.result(service.createMultiCloudWorkspaceInt(request, workspaceId, billingProfile, testContext),
                   Duration.Inf
      )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(name),
        ArgumentMatchers.eq(billingProfile.getId.toString),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        any[Option[WsmPolicyInputs]],
        ArgumentMatchers.eq(testContext)
      )

    verify(leonardoDAO).createWDSInstance(testContext.userInfo.accessToken.token, workspaceIdCaptor.getValue, None)

    verify(workspaceManagerDAO).createAzureStorageContainer(
      workspaceIdCaptor.getValue,
      MultiCloudWorkspaceService.getStorageContainerName(workspaceIdCaptor.getValue),
      testContext
    )
  }

  it should "not deploy a WDS instance during workspace creation if test attribute is set as a boolean" in {
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID())
    val workspaceRepository = mock[WorkspaceRepository]
    val attributes = Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeBoolean(true))
    val workspace = defaultWorkspace
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
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
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

    verify(workspaceManagerDAO).createWorkspaceWithSpendProfile(
      ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
      ArgumentMatchers.eq(name),
      any(), // spend profile id
      ArgumentMatchers.eq(namespace),
      any[Seq[String]],
      ArgumentMatchers.eq(CloudPlatform.AZURE),
      any[Option[WsmPolicyInputs]],
      ArgumentMatchers.eq(testContext)
    )
    verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        UUID.fromString(result.workspaceId),
        MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId)),
        testContext
      )
    verify(leonardoDAO, never()).createWDSInstance(any, any, any)

  }

  it should "fail and rollback workspace creation on initial WSM workspace creation failure" in {
    val workspace = defaultWorkspace
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
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
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
    val workspace = defaultWorkspace
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
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
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
    val workspace = defaultWorkspace
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
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
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
    val workspace = defaultWorkspace
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
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
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
}
