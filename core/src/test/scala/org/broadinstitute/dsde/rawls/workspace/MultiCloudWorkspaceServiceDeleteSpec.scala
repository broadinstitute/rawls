package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{JobReport, JobResult, WorkspaceDescription}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  GcpWorkspaceDeletionContext,
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  UserInfo,
  Workspace,
  WorkspaceDeletionResult,
  WorkspaceName,
  WorkspaceState,
  WorkspaceType,
  WorkspaceVersions
}
import org.joda.time.DateTime
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, never, spy, times, verify, when}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceDeleteSpec
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
  val mcWorkspaceConfig: MultiCloudWorkspaceConfig =
    MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig])
  val namespace: String = "fake-namespace"
  val name: String = "fake-name"
  val workspaceName: WorkspaceName = WorkspaceName(namespace, name)
  val workspaceId: UUID = UUID.randomUUID()
  val defaultMCWorkspace: Workspace = Workspace.buildMcWorkspace(
    namespace = namespace,
    name = name,
    workspaceId = workspaceId.toString,
    DateTime.now(),
    DateTime.now(),
    createdBy = testContext.userInfo.userEmail.value,
    attributes = Map.empty,
    state = WorkspaceState.Ready
  )
  val defaultRawlsWorkspace: Workspace = Workspace(
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

  behavior of "deleteMultiCloudOrRawlsWorkspaceV2"

  it should "call WorkspaceService to delete a rawls workspace synchronously" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspaceRepository = mock[WorkspaceRepository]
    val workspace = defaultRawlsWorkspace
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
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = defaultMCWorkspace
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
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = defaultMCWorkspace.copy(state = WorkspaceState.Deleting)
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
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = defaultMCWorkspace.copy(state = WorkspaceState.Creating)
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

  it should "fail if user does not have delete permissions on the workspace" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(false))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.read, testContext)
    )
      .thenReturn(Future(true))
    val workspace = defaultMCWorkspace
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
        service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]),
        Duration.Inf
      )
    }

    result.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
  }

  it should "delete an MC workspace" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = defaultMCWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    when(workspaceRepository.deleteWorkspace(workspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val service = spy(
      new MultiCloudWorkspaceService(
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
    )
    // MultiCloudWorkspaceService.deleteWorkspaceInWSM is tested independently
    doReturn(Future()).when(service).deleteWorkspaceInWSM(workspace.workspaceIdAsUUID)

    Await.result(
      service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]),
      Duration.Inf
    ) shouldBe None
    verify(workspaceRepository).deleteWorkspace(workspace)
    verify(service).deleteWorkspaceInWSM(workspaceId)
  }

  it should "delete a rawls workspace" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = defaultRawlsWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    when(workspaceRepository.deleteWorkspace(workspace)).thenReturn(Future(true))
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
    val workspaceService = mock[WorkspaceService]
    when(workspaceService.deleteWorkspace(workspaceName))
      .thenReturn(Future(WorkspaceDeletionResult(None, Some(GcpWorkspaceDeletionContext(workspace.bucketName)))))

    Await.result(
      service.deleteMultiCloudOrRawlsWorkspace(workspaceName, workspaceService),
      Duration.Inf
    ) shouldBe Some(workspace.bucketName)
    verify(workspaceService).deleteWorkspace(workspaceName)
  }

  it should "leave the rawls state in place if WSM returns an error during workspace deletion" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    )
      .thenReturn(Future(true))
    val workspace = defaultMCWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ => throw new ApiException(500, "error"))
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
    verify(wsmDAO, never).deleteWorkspaceV2(any, any, any)
    verify(workspaceRepository, never).deleteWorkspace(workspace)
  }

  it should "delete the rawls record if the WSM record is not found" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId.toString,
                           SamWorkspaceActions.delete,
                           testContext
      )
    ).thenReturn(Future(true))
    val workspace = defaultMCWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(workspaceName, None)).thenReturn(Future(Some(workspace)))
    when(workspaceRepository.deleteWorkspace(workspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ => throw new ApiException(404, "not found"))
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

    val result =
      Await.result(service.deleteMultiCloudOrRawlsWorkspace(workspaceName, mock[WorkspaceService]), Duration.Inf)

    result shouldBe None
    verify(wsmDAO).getWorkspace(workspaceId, testContext)
    verify(wsmDAO, never).deleteWorkspaceV2(any, any, any)
    verify(workspaceRepository).deleteWorkspace(workspace)
  }

  behavior of "deleteWorkspaceInWSM"

  it should "start deletion and poll for success when the workspace exists in workspace manager" in {
    val wsmDAO = mock[WorkspaceManagerDAO]
    val workspaceWSMDescription = new WorkspaceDescription()
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenReturn(workspaceWSMDescription)
    val jobId = UUID.randomUUID().toString
    val runningJob = new JobResult().jobReport(new JobReport().id(jobId).status(StatusEnum.RUNNING))
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), ArgumentMatchers.eq(testContext)))
      .thenReturn(runningJob)
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, jobId, testContext))
      .thenReturn(runningJob)
      .thenReturn(new JobResult().jobReport(new JobReport().id(jobId).status(StatusEnum.SUCCEEDED)))

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    Await.result(service.deleteWorkspaceInWSM(workspaceId), Duration.Inf)

    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any, any)
    verify(wsmDAO, times(2)).getDeleteWorkspaceV2Result(workspaceId, jobId, testContext)
  }

  it should "not attempt to delete a workspace if WSM returns a 403 when retrieving the workspace" in {
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ => throw new ApiException(403, "forbidden"))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )
    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.deleteWorkspaceInWSM(workspaceId), Duration.Inf)
    }
    result.errorReport.statusCode shouldEqual Some(StatusCodes.InternalServerError)
    verify(wsmDAO, never).deleteWorkspaceV2(any, any, any)
  }

  it should "not throw an exception or attempt to delete a workspace that does not exist in WSM" in {
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenAnswer(_ => throw new ApiException(404, "not found"))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )
    Await.result(service.deleteWorkspaceInWSM(workspaceId), Duration.Inf)
    verify(wsmDAO, never).deleteWorkspaceV2(any, any, any)
  }

  it should "fail for errors when polling the deletion result the workspace in WSM" in {
    val wsmDAO = mock[WorkspaceManagerDAO]
    val workspaceWSMDescription = new WorkspaceDescription()
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenReturn(workspaceWSMDescription)
    val jobId = UUID.randomUUID().toString
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), ArgumentMatchers.eq(testContext)))
      .thenReturn(new JobResult().jobReport(new JobReport().id(jobId).status(StatusEnum.RUNNING)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, jobId, testContext)).thenAnswer { _ =>
      throw new ApiException(StatusCodes.BadRequest.intValue, "failure")
    }
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    val actual = intercept[ApiException] {
      Await.result(service.deleteWorkspaceInWSM(workspaceId), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any, any)
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, jobId, testContext)
  }

  it should "fail for for non-ApiException errors" in {
    val wsmDAO = mock[WorkspaceManagerDAO]
    val workspaceWSMDescription = new WorkspaceDescription()
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenReturn(workspaceWSMDescription)
    val jobId = UUID.randomUUID().toString
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), ArgumentMatchers.eq(testContext)))
      .thenReturn(new JobResult().jobReport(new JobReport().id(jobId).status(StatusEnum.RUNNING)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, jobId, testContext)).thenAnswer { _ =>
      throw new Exception("non-api failure")
    }
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    val actual = intercept[Exception] {
      Await.result(service.deleteWorkspaceInWSM(workspaceId), Duration.Inf)
    }

    actual.getMessage shouldBe "non-api failure"
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any, any)
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, jobId, testContext)
  }

  it should "complete without an exception when workspace manager returns 403 polling the deletion result" in {
    val wsmDAO = mock[WorkspaceManagerDAO]
    val workspaceWSMDescription = new WorkspaceDescription()
    when(wsmDAO.getWorkspace(workspaceId, testContext)).thenReturn(workspaceWSMDescription)
    val jobId = UUID.randomUUID().toString
    val runningJob = new JobResult().jobReport(new JobReport().id(jobId).status(StatusEnum.RUNNING))
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), ArgumentMatchers.eq(testContext)))
      .thenReturn(runningJob)
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, jobId, testContext))
      .thenReturn(runningJob)
      .thenAnswer(_ => throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden"))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mcWorkspaceConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    Await.result(service.deleteWorkspaceInWSM(workspaceId), Duration.Inf)

    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any, any)
    verify(wsmDAO, times(2)).getDeleteWorkspaceV2Result(workspaceId, jobId, testContext)
  }

}
