package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace,
  WorkspaceState
}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.WorkspaceDeletionRunnerSpec.{
  azureWorkspace,
  ctx,
  monitorRecord
}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.{
  LeonardoOperationFailureException,
  LeonardoResourceDeletionAction,
  WsmDeletionAction
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

object WorkspaceDeletionRunnerSpec {

  val azureWorkspace: Workspace = Workspace.buildReadyMcWorkspace(
    "fake_azure_bp",
    "fake_ws",
    UUID.randomUUID().toString,
    DateTime.now(),
    DateTime.now(),
    "example@example.com",
    Map.empty
  )

  val monitorRecord: WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord.forWorkspaceDeletion(
      UUID.randomUUID(),
      azureWorkspace.workspaceIdAsUUID,
      RawlsUserEmail("example@example.com")
    )
  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)

  val ctx = RawlsRequestContext(userInfo)
}
class WorkspaceDeletionRunnerSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "setup"

  it should "return a completed status if the workspace is None" in {
    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    )

    whenReady(runner(monitorRecord.copy(workspaceId = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }

  it should "return a completed status if the user email is None" in {
    val wsRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(wsRepo.getWorkspace(any[UUID])).thenAnswer(_ => Future(Some(azureWorkspace)))
    when(wsRepo.updateState(any[UUID], any[WorkspaceState])).thenAnswer(_ => Future.successful(1))

    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      wsRepo,
      mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    )

    whenReady(runner(monitorRecord.copy(userEmail = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }

  it should "throw an exception if called with a job type that is not WorkspaceDelete" in {
    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    )
    intercept[IllegalArgumentException](runner(monitorRecord.copy(jobType = JobType.BpmBillingProjectDelete)))
  }

  behavior of "runtime deletion orchestration"

  it should "start runtime deletion" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(any[WorkspaceManagerResourceMonitorRecord])).thenAnswer(_ => Future.successful())
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.deleteRuntimes(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext])
    ).thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        monitorRecordDao,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe Complete)
    verify(leoDeletion).deleteRuntimes(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(
      any[ExecutionContext]
    )
  }

  it should "poll runtime deletion and return incomplete if not finished" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.pollRuntimeDeletion(ArgumentMatchers.eq(azureWorkspace),
                                      any[WorkspaceManagerResourceMonitorRecord],
                                      any[RawlsRequestContext]
      )(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful(false))

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollLeoRuntimeDeletion)))(_ shouldBe Incomplete)
    verify(leoDeletion).pollRuntimeDeletion(ArgumentMatchers.eq(azureWorkspace),
                                            any[WorkspaceManagerResourceMonitorRecord],
                                            any[RawlsRequestContext]
    )(
      any[ExecutionContext]
    )
  }

  it should "poll runtime deletion and return complete if finished" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.pollRuntimeDeletion(ArgumentMatchers.eq(azureWorkspace),
                                      any[WorkspaceManagerResourceMonitorRecord],
                                      any[RawlsRequestContext]
      )(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful(true))

    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(any[WorkspaceManagerResourceMonitorRecord])).thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        monitorRecordDao,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollLeoRuntimeDeletion)))(_ shouldBe Complete)
    verify(leoDeletion).pollRuntimeDeletion(ArgumentMatchers.eq(azureWorkspace),
                                            any[WorkspaceManagerResourceMonitorRecord],
                                            any[RawlsRequestContext]
    )(
      any[ExecutionContext]
    )
  }

  behavior of "app deletion orchestration"

  it should "start app deletion" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(any[WorkspaceManagerResourceMonitorRecord])).thenAnswer(_ => Future.successful())
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.deleteApps(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext])
    ).thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        monitorRecordDao,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.StartLeoAppDeletion)))(_ shouldBe Complete)
    verify(leoDeletion).deleteApps(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext])
  }

  it should "poll app deletion and return incomplete if not finished" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                  any[WorkspaceManagerResourceMonitorRecord],
                                  any[RawlsRequestContext]
      )(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful(false))

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollLeoAppDeletion)))(_ shouldBe Incomplete)
    verify(leoDeletion).pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                        any[WorkspaceManagerResourceMonitorRecord],
                                        any[RawlsRequestContext]
    )(
      any[ExecutionContext]
    )
  }

  it should "poll app deletion and return complete if finished" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                  any[WorkspaceManagerResourceMonitorRecord],
                                  any[RawlsRequestContext]
      )(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful(true))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(any[WorkspaceManagerResourceMonitorRecord])).thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        monitorRecordDao,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollLeoAppDeletion)))(_ shouldBe Complete)
    verify(leoDeletion).pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                        any[WorkspaceManagerResourceMonitorRecord],
                                        any[RawlsRequestContext]
    )(
      any[ExecutionContext]
    )
  }

  behavior of "WSM deletion orchestration"

  it should "start WSM workspace deletion" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(any[WorkspaceManagerResourceMonitorRecord])).thenAnswer(_ => Future.successful())
    val wsmDeletionAction = mock[WsmDeletionAction](RETURNS_SMART_NULLS)
    when(
      wsmDeletionAction.startStep(ArgumentMatchers.eq(azureWorkspace), anyString(), ArgumentMatchers.eq(ctx))(
        any[ExecutionContext]
      )
    )
      .thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
        wsmDeletionAction,
        monitorRecordDao,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.StartWsmDeletion)))(_ shouldBe Complete)
    verify(wsmDeletionAction).startStep(ArgumentMatchers.eq(azureWorkspace), anyString(), ArgumentMatchers.eq(ctx))(
      any[ExecutionContext]
    )
  }

  it should "poll WSM workspace deletion and return incomplete if not finished" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))

    val wsmDeletionAction = mock[WsmDeletionAction](RETURNS_SMART_NULLS)
    when(
      wsmDeletionAction.isComplete(ArgumentMatchers.eq(azureWorkspace),
                                   any[WorkspaceManagerResourceMonitorRecord],
                                   ArgumentMatchers.eq(ctx)
      )
    )
      .thenAnswer(_ => Future.successful(false))

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
        wsmDeletionAction,
        mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollWsmDeletion)))(_ shouldBe Incomplete)
    verify(wsmDeletionAction).isComplete(ArgumentMatchers.eq(azureWorkspace),
                                         any[WorkspaceManagerResourceMonitorRecord],
                                         any[RawlsRequestContext]
    )
  }

  it should "poll app deletion and return complete if finished" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                  any[WorkspaceManagerResourceMonitorRecord],
                                  any[RawlsRequestContext]
      )(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful(true))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(any[WorkspaceManagerResourceMonitorRecord])).thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        monitorRecordDao,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(ctx))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollLeoAppDeletion)))(_ shouldBe Complete)
    verify(leoDeletion).pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                        any[WorkspaceManagerResourceMonitorRecord],
                                        any[RawlsRequestContext]
    )(
      any[ExecutionContext]
    )
  }

  behavior of "other"

  it should "fail if the workspace is not found" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(None))

    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      workspaceRepo,
      mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    )

    intercept[WorkspaceDeletionException] {
      Await.result(runner(monitorRecord), Duration.Inf)
    }
  }

  it should "mark the workspace as DeleteFailed when a step fails" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))
    when(workspaceRepo.updateState(ArgumentMatchers.eq(monitorRecord.workspaceId.get), any[WorkspaceState]))
      .thenAnswer(_ => Future.successful(1))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.deleteApps(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext]))
      .thenAnswer(_ => Future.successful())
    when(
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace),
                                  any[WorkspaceManagerResourceMonitorRecord],
                                  any[RawlsRequestContext]
      )(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.failed(new LeonardoOperationFailureException("failed", azureWorkspace.workspaceIdAsUUID)))

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS),
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(new RawlsRequestContext(null, null)))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord.copy(jobType = JobType.PollLeoAppDeletion)))(_ shouldBe Complete)
    verify(workspaceRepo).updateState(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                      ArgumentMatchers.eq(WorkspaceState.DeleteFailed)
    )
  }
}
