package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail, Workspace, WorkspaceState}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, spy, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class CloneWorkspaceAwaitStorageContainerStepSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with Matchers
    with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userEmail: String = "user@email.com"
  val workspaceId: UUID = UUID.randomUUID()
  val wsCreatedDate: DateTime = DateTime.parse("2023-01-18T10:08:48.541-05:00")

  val workspace: Workspace = Workspace(
    "test-ws-namespace",
    "test-ws-name",
    workspaceId.toString,
    "test-bucket",
    None,
    wsCreatedDate,
    wsCreatedDate,
    "a_user",
    Map()
  )

  behavior of "retrieving the report for the container cloning job"

  it should "report errors from api response and complete the job for jobs failed with a 500" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val apiMessage = "some failure message"
    val apiException = new ApiException(500, apiMessage)

    val monitorRecord: WorkspaceManagerResourceMonitorRecord =
      WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
        UUID.randomUUID(),
        workspaceId,
        RawlsUserEmail(userEmail),
        Some(Map.empty),
        JobType.CloneWorkspaceAwaitContainerResult
      )

    doAnswer(_ => throw apiException)
      .when(wsmDao)
      .getJob(ArgumentMatchers.eq(monitorRecord.jobControlId.toString), ArgumentMatchers.any())

    val runner = spy(
      new CloneWorkspaceAwaitStorageContainerStep(
        wsmDao,
        mock[WorkspaceRepository],
        mock[WorkspaceManagerResourceMonitorRecordDao],
        workspaceId,
        monitorRecord
      )
    )

    doAnswer { answer =>
      val errorMessage = answer.getArgument(1).asInstanceOf[String]
      errorMessage should include(apiMessage)
      Future.successful(1)
    }.when(runner)
      .fail(ArgumentMatchers.any(), ArgumentMatchers.any[String]())

    whenReady(runner.runStep(ctx))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)
    verify(runner).fail(ArgumentMatchers.any(), ArgumentMatchers.any[String]())
  }

  it should "report an errors and a complete job for jobs failed with a 404" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val apiMessage = "some failure message"
    val apiException = new ApiException(404, apiMessage)

    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map.empty),
      JobType.CloneWorkspaceAwaitContainerResult
    )

    doAnswer(_ => throw apiException)
      .when(wsmDao)
      .getJob(ArgumentMatchers.eq(monitorRecord.jobControlId.toString), ArgumentMatchers.any())

    val runner = spy(
      new CloneWorkspaceAwaitStorageContainerStep(
        wsmDao,
        mock[WorkspaceRepository],
        mock[WorkspaceManagerResourceMonitorRecordDao],
        workspaceId,
        monitorRecord
      )
    )

    doAnswer { answer =>
      val errorMessage = answer.getArgument(1).asInstanceOf[String]
      errorMessage should include("Unable to find")
      Future.successful(1)
    }.when(runner)
      .fail(ArgumentMatchers.any(), ArgumentMatchers.any[String]())

    whenReady(runner.runStep(ctx))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)
    verify(runner).fail(ArgumentMatchers.any(), ArgumentMatchers.any[String]())
  }

  behavior of "handling the clone container report"

  it should "update the workspace with the time from the job and sets the status to ready on success" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map.empty),
      JobType.CloneWorkspaceAwaitContainerResult
    )
    val completedTime = "2023-01-16T10:08:48.541-05:00"
    val expectedTime = DateTime.parse(completedTime)
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.updateState(workspaceId, WorkspaceState.Ready)).thenReturn(Future(1))
    when(workspaceRepository.updateCompletedCloneWorkspaceFileTransfer(workspaceId, expectedTime))
      .thenReturn(Future(1))
    val runner = new CloneWorkspaceAwaitStorageContainerStep(
      mock[WorkspaceManagerDAO],
      workspaceRepository,
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceId,
      monitorRecord
    )
    val report = new JobReport().status(JobReport.StatusEnum.SUCCEEDED).completed(completedTime)

    whenReady(runner.handleCloneResult(workspaceId, report))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(workspaceRepository).updateState(workspaceId, WorkspaceState.Ready)
    verify(workspaceRepository).updateCompletedCloneWorkspaceFileTransfer(workspaceId, expectedTime)
  }

  it should "return incomplete for running jobs" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map.empty),
      JobType.CloneWorkspaceAwaitContainerResult
    )
    val runner = new CloneWorkspaceAwaitStorageContainerStep(
      mock[WorkspaceManagerDAO],
      mock[WorkspaceRepository],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceId,
      monitorRecord
    )
    val report = new JobReport().status(JobReport.StatusEnum.RUNNING)

    whenReady(runner.handleCloneResult(workspaceId, report))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete
    )
  }

  it should "record the error and update the workspace state for a failed job" in {
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map.empty),
      JobType.CloneWorkspaceAwaitContainerResult
    )
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.setFailedState(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceState.CloningFailed),
        ArgumentMatchers.any[String]
      )
    ).thenReturn(Future(1))
    val runner = new CloneWorkspaceAwaitStorageContainerStep(
      mock[WorkspaceManagerDAO],
      workspaceRepository,
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceId,
      monitorRecord
    )(executionContext)
    val report = new JobReport().status(JobReport.StatusEnum.FAILED)

    whenReady(runner.handleCloneResult(workspaceId, report))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(workspaceRepository).setFailedState(
      ArgumentMatchers.eq(workspaceId),
      ArgumentMatchers.eq(WorkspaceState.CloningFailed),
      ArgumentMatchers.any[String]
    )
  }

}
