package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
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
      mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    )
    intercept[IllegalArgumentException](runner(monitorRecord.copy(jobType = JobType.BpmBillingProjectDelete)))
  }

  behavior of "deletion orchestration"

  it should "orchestrate the deletion of downstream resources" in {
    val workspaceRepo = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    when(workspaceRepo.getWorkspace(ArgumentMatchers.eq(monitorRecord.workspaceId.get)))
      .thenAnswer(_ => Future.successful(Some(azureWorkspace)))
    when(workspaceRepo.deleteWorkspaceRecord(ArgumentMatchers.eq(azureWorkspace)))
      .thenAnswer(_ => Future.successful(true))

    val leoDeletion = mock[LeonardoResourceDeletionAction](RETURNS_SMART_NULLS)
    when(leoDeletion.deleteApps(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext]))
      .thenAnswer(_ => Future.successful())
    when(
      leoDeletion.deleteRuntimes(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext])
    ).thenAnswer(_ => Future.successful())
    when(
      leoDeletion.pollAppDeletion(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful())
    when(
      leoDeletion.pollRuntimeDeletion(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful())

    val wsmDeletion = mock[WsmDeletionAction](RETURNS_SMART_NULLS)
    when(
      wsmDeletion.startStep(ArgumentMatchers.eq(azureWorkspace), anyString(), any[RawlsRequestContext])(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful())
    when(
      wsmDeletion.pollOperation(ArgumentMatchers.eq(azureWorkspace), anyString(), any[RawlsRequestContext])(
        any[ExecutionContext]
      )
    ).thenAnswer(_ => Future.successful())

    val runner = spy(
      new WorkspaceDeletionRunner(
        mock[SamDAO](RETURNS_SMART_NULLS),
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        workspaceRepo,
        leoDeletion,
        wsmDeletion,
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(new RawlsRequestContext(null, null)))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe Complete)
    verify(leoDeletion).deleteApps(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(any[ExecutionContext])
    verify(leoDeletion).deleteRuntimes(ArgumentMatchers.eq(azureWorkspace), any[RawlsRequestContext])(
      any[ExecutionContext]
    )
    verify(wsmDeletion).startStep(ArgumentMatchers.eq(azureWorkspace), anyString(), any[RawlsRequestContext])(
      any[ExecutionContext]
    )
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
        mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      )
    )
    doReturn(Future.successful(new RawlsRequestContext(null, null)))
      .when(runner)
      .getUserCtx(anyString())(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe Complete)
    verify(workspaceRepo).updateState(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                      ArgumentMatchers.eq(WorkspaceState.DeleteFailed)
    )
  }
}
