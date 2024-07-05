package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsRequestContext, RawlsUserEmail, RawlsUserSubjectId, SamResourceTypeNames, SamUserStatusResponse, SamWorkspaceActions, UserInfo, Workspace, WorkspaceDeletionResult, WorkspaceName, WorkspaceState, WorkspaceType, WorkspaceVersions}
import org.joda.time.DateTime
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class MultiCloudWorkspaceServiceDeleteSpec extends AnyFlatSpecLike
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

  behavior of "deleteMultiCloudOrRawlsWorkspaceV2"

  it should "call WorkspaceService to delete a rawls workspace synchronously" in {
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
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
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(any())).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = defaultWorkspace
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
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = defaultWorkspace.copy(state = WorkspaceState.Deleting)
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
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(true))
    val workspace = defaultWorkspace.copy(state = WorkspaceState.Creating)
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
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.delete, testContext))
      .thenReturn(Future(false))
    when(samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId.toString, SamWorkspaceActions.read, testContext))
      .thenReturn(Future(true))
    val workspace = defaultWorkspace
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
        , Duration.Inf
      )
    }

    result.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
  }

}
