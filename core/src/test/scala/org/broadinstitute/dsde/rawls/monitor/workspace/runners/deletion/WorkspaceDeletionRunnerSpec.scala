package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

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
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail, Workspace, WorkspaceState}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.WorkspaceDeletionRunnerSpec.{
  azureWorkspace,
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

import java.sql.Timestamp
import java.time.Instant
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
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
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
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
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
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    )
    intercept[IllegalArgumentException](runner(monitorRecord.copy(jobType = JobType.BpmBillingProjectDelete)))
  }

  behavior of "deletion orchestration"

  it should "start deletion of leo apps and update job to LeoAppDeletionPoll on init" in {
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.deleteApps(any(), any())(any[ExecutionContext]())).thenReturn(Future.successful())
    val recordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)

    val jobUpdate = monitorRecord.copy(jobType = JobType.LeoAppDeletionPoll)
    when(recordDao.update(jobUpdate)).thenReturn(Future.successful(1))
    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      leoDeletion,
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      recordDao
    )
    whenReady(runner.runStep(monitorRecord, azureWorkspace, RawlsRequestContext(null, null)))(_ shouldBe Incomplete)
    verify(recordDao).update(jobUpdate)
    verify(leoDeletion).deleteApps(any(), any())(any[ExecutionContext]())
  }

  it should "return incomplete when leo apps are not deleted" in {
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.pollAppDeletion(any(), any())(any[ExecutionContext]())).thenReturn(Future.successful(false))

    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      leoDeletion,
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    )
    whenReady(
      runner.runStep(monitorRecord.copy(jobType = JobType.LeoAppDeletionPoll),
                     azureWorkspace,
                     RawlsRequestContext(null, null)
      )
    )(_ shouldBe Incomplete)
    verify(leoDeletion).pollAppDeletion(any(), any())(any[ExecutionContext]())
  }

  it should "delete leo runtimes and update the job after leo apps are deleted" in {
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.pollAppDeletion(any(), any())(any[ExecutionContext]())).thenReturn(Future.successful(true))
    when(leoDeletion.deleteRuntimes(any(), any())(any[ExecutionContext]())).thenReturn(Future.successful())
    val recordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    val jobUpdate = monitorRecord.copy(jobType = JobType.LeoRuntimeDeletionPoll)
    when(recordDao.update(jobUpdate)).thenReturn(Future.successful(1))
    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      leoDeletion,
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      recordDao
    )
    whenReady(
      runner.runStep(monitorRecord.copy(jobType = JobType.LeoAppDeletionPoll),
                     azureWorkspace,
                     RawlsRequestContext(null, null)
      )
    )(_ shouldBe Incomplete)
    verify(leoDeletion).pollAppDeletion(any(), any())(any[ExecutionContext]())
    verify(recordDao).update(jobUpdate)
    verify(leoDeletion).deleteRuntimes(any(), any())(any[ExecutionContext]())
  }

  it should "return incomplete when leo runtimes are not deleted" in {
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.pollRuntimeDeletion(any(), any())(any[ExecutionContext]())).thenReturn(Future.successful(false))

    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      leoDeletion,
      mock[WsmDeletionAction](RETURNS_SMART_NULLS),
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    )
    whenReady(
      runner.runStep(monitorRecord.copy(jobType = JobType.LeoRuntimeDeletionPoll),
                     azureWorkspace,
                     RawlsRequestContext(null, null)
      )
    )(_ shouldBe Incomplete)
    verify(leoDeletion).pollRuntimeDeletion(any(), any())(any[ExecutionContext]())
  }

  it should "delete the wsm workspace and update the job after leo runtimes are deleted" in {
    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.pollRuntimeDeletion(any(), any())(any[ExecutionContext]())).thenReturn(Future.successful(true))
    val wsmAction = mock[WsmDeletionAction](RETURNS_SMART_NULLS)
    when(wsmAction.startStep(any(), any(), any())(any[ExecutionContext]())).thenReturn(Future.successful())

    val recordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    val job = monitorRecord.copy(jobType = JobType.LeoRuntimeDeletionPoll)
    val jobUpdate = monitorRecord.copy(jobType = JobType.WSMWorkspaceDeletionPoll)
    when(recordDao.update(jobUpdate)).thenReturn(Future.successful(1))
    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      leoDeletion,
      wsmAction,
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      recordDao
    )
    whenReady(runner.runStep(job, azureWorkspace, RawlsRequestContext(null, null)))(_ shouldBe Incomplete)
    verify(leoDeletion).pollRuntimeDeletion(any(), any())(any[ExecutionContext]())
    verify(recordDao).update(jobUpdate)
    verify(wsmAction).startStep(any(), any(), any())(any[ExecutionContext]())
  }

  it should "return incomplete when wsm workspace is not not deleted" in {
    val wsmAction = mock[WsmDeletionAction](RETURNS_SMART_NULLS)
    when(wsmAction.pollForCompletion(any(), any(), any())(any[ExecutionContext]())).thenReturn(Future.successful(false))

    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceRepository](RETURNS_SMART_NULLS),
      mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
      wsmAction,
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    )
    whenReady(
      runner.runStep(monitorRecord.copy(jobType = JobType.WSMWorkspaceDeletionPoll),
                     azureWorkspace,
                     RawlsRequestContext(null, null)
      )
    )(_ shouldBe Incomplete)
    verify(wsmAction).pollForCompletion(any(), any(), any())(any[ExecutionContext]())
  }

  it should "delete the rawls record and return Complete after the wsm workspace is deleted" in {
    val wsmAction = mock[WsmDeletionAction](RETURNS_SMART_NULLS)
    when(wsmAction.pollForCompletion(any(), any(), any())(any[ExecutionContext]())).thenReturn(Future.successful(true))
    val repo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(repo.deleteWorkspaceRecord(azureWorkspace)).thenReturn(Future.successful(true))
    val runner = new WorkspaceDeletionRunner(
      mock[SamDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
      repo,
      mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS),
      wsmAction,
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    )
    whenReady(
      runner.runStep(monitorRecord.copy(jobType = JobType.WSMWorkspaceDeletionPoll),
                     azureWorkspace,
                     RawlsRequestContext(null, null)
      )
    )(_ shouldBe Complete)
    verify(wsmAction).pollForCompletion(any(), any(), any())(any[ExecutionContext]())
    verify(repo).deleteWorkspaceRecord(azureWorkspace)

  }

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
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
      mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
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
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(
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
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(RawlsRequestContext(null, null)))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe Complete)
    verify(workspaceRepo).updateState(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                      ArgumentMatchers.eq(WorkspaceState.DeleteFailed)
    )
  }

  it should "mark the workspace as DeleteFailed when a step times out" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))
    when(workspaceRepo.updateState(ArgumentMatchers.eq(monitorRecord.workspaceId.get), any[WorkspaceState]))
      .thenAnswer(_ => Future.successful(1))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful(false))
    val expiredtime =
      Instant.ofEpochMilli(Instant.now().toEpochMilli - 600001) // -10 minutes will make operation time out
    val job = monitorRecord.copy(jobType = JobType.LeoAppDeletionPoll, createdTime = Timestamp.from(expiredtime))
    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        mock[WsmDeletionAction](RETURNS_SMART_NULLS),
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(RawlsRequestContext(null, null)))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(job))(_ shouldBe Complete)
    verify(workspaceRepo).updateState(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                      ArgumentMatchers.eq(WorkspaceState.DeleteFailed)
    )
  }
}
