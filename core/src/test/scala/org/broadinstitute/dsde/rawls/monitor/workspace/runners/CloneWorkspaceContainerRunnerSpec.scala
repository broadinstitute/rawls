package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.CloneWorkspaceContainerRunnerSpec.{
  monitorRecord,
  userEmail,
  workspace,
  workspaceId
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, doReturn, doThrow, spy, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object CloneWorkspaceContainerRunnerSpec {
  val userEmail: String = "user@email.com"
  val workspaceId: UUID = UUID.randomUUID()
  val wsCreatedDate: DateTime = DateTime.parse("2023-01-18T10:08:48.541-05:00")
  val monitorRecord: WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord.forCloneWorkspaceContainer(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail)
    )

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
}

class CloneWorkspaceContainerRunnerSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "initial setup and basic requirements updating workspace container cloning status monitoring"

  it should "return a completed status if the workspace id is not set on the job" in {
    val runner = new CloneWorkspaceContainerRunner(
      mock[SamDAO],
      mock[WorkspaceManagerDAO],
      mock[SlickDataSource],
      mock[GoogleServicesDAO]
    )
    whenReady(runner(monitorRecord.copy(workspaceId = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }

  it should "return a completed status if no workspace is found for the id" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )
    val completedTime = "2023-01-16T10:08:48.541-05:00"
    val expectedTime = DateTime.parse(completedTime)
    val report = new JobReport().status(JobReport.StatusEnum.SUCCEEDED).completed(completedTime)

    doAnswer { answer =>
      val specifiedTime = answer.getArgument(1).asInstanceOf[DateTime]
      specifiedTime shouldBe expectedTime
      Future.successful(None)
    }.when(runner)
      .cloneSuccess(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.eq(expectedTime))(
        ArgumentMatchers.any[ExecutionContext]()
      )

    whenReady(runner.handleCloneResult(workspaceId, report))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )

    verify(runner)
      .cloneSuccess(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.eq(expectedTime))(
        ArgumentMatchers.any[ExecutionContext]()
      )
  }

  it should "return a completed status if no user email is set on the job" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )

    doAnswer(answer =>
      Future.successful(Some(workspace.copy(errorMessage = Some(answer.getArgument(1).asInstanceOf[String]))))
    ).when(runner)
      .cloneFail(ArgumentMatchers.any(), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())

    whenReady(runner(monitorRecord.copy(userEmail = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )

  }

  it should "return Incomplete when the if the user context cannot be created" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )
    doReturn(Future.failed(new org.broadinstitute.dsde.workbench.client.sam.ApiException()))
      .when(runner)
      .getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())
    doAnswer { answer =>
      val errorMessage = answer.getArgument(1).asInstanceOf[String]
      errorMessage should include(workspaceId.toString)
      errorMessage should include(userEmail)
      Future.successful(Some(workspace.copy(errorMessage = Some(errorMessage))))
    }.when(runner)
      .cloneFail(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
    verify(runner).cloneFail(ArgumentMatchers.any(), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())
  }

  it should "report errors from api response and complete the job for jobs failed with a 500" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val apiMessage = "some failure message"
    val apiException = new ApiException(500, apiMessage)

    doAnswer(_ => throw apiException)
      .when(wsmDao)
      .getJob(ArgumentMatchers.eq(monitorRecord.jobControlId.toString), ArgumentMatchers.any())

    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        wsmDao,
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )

    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    doAnswer { answer =>
      val errorMessage = answer.getArgument(1).asInstanceOf[String]
      errorMessage should include(apiMessage)
      Future.successful(Some(workspace.copy(errorMessage = Some(errorMessage))))
    }.when(runner)
      .cloneFail(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)
    verify(runner).cloneFail(ArgumentMatchers.any(), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())
  }

  it should "report an errors and a complete job for jobs failed with a 404" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val apiMessage = "some failure message"
    val apiException = new ApiException(404, apiMessage)

    doAnswer(_ => throw apiException)
      .when(wsmDao)
      .getJob(ArgumentMatchers.eq(monitorRecord.jobControlId.toString), ArgumentMatchers.any())

    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        wsmDao,
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )

    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    doAnswer { answer =>
      val errorMessage = answer.getArgument(1).asInstanceOf[String]
      errorMessage should include("Unable to find")
      errorMessage should include(monitorRecord.jobControlId.toString)
      Future.successful(Some(workspace.copy(errorMessage = Some(errorMessage))))
    }.when(runner)
      .cloneFail(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)
    verify(runner).cloneFail(ArgumentMatchers.any(), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())
  }

  behavior of "handling the clone container report"

  it should "set completedCloneWorkspaceFileTransfer on the workspace to the complete time in the report" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )
    val completedTime = "2023-01-16T10:08:48.541-05:00"
    val expectedTime = DateTime.parse(completedTime)
    val report = new JobReport().status(JobReport.StatusEnum.SUCCEEDED).completed(completedTime)
    doAnswer { answer =>
      val specifiedTime = answer.getArgument(1).asInstanceOf[DateTime]
      specifiedTime shouldBe expectedTime
      Future.successful(Some(workspace.copy(completedCloneWorkspaceFileTransfer = Some(expectedTime))))
    }.when(runner)
      .cloneSuccess(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.eq(expectedTime))(
        ArgumentMatchers.any[ExecutionContext]()
      )

    whenReady(runner.handleCloneResult(workspaceId, report))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
    verify(runner)
      .cloneSuccess(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.eq(expectedTime))(
        ArgumentMatchers.any[ExecutionContext]()
      )
  }

  it should "return Incomplete for running jobs" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )
    val completedTime = "2023-01-16T10:08:48.541-05:00"
    val report = new JobReport().status(JobReport.StatusEnum.RUNNING).completed(completedTime)

    whenReady(runner.handleCloneResult(workspaceId, report))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete
    )
  }

  it should "report an error for a successful request but a failed job" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )
    val completedTime = "2023-01-16T10:08:48.541-05:00"
    val report = new JobReport().status(JobReport.StatusEnum.FAILED).completed(completedTime)

    doAnswer { answer =>
      val errorMessage = answer.getArgument(1).asInstanceOf[String]
      errorMessage should include("Cloning")
      errorMessage should include("failed")
      Future.successful(Some(workspace.copy(errorMessage = Some(errorMessage))))
    }.when(runner)
      .cloneFail(ArgumentMatchers.eq(workspaceId), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())

    whenReady(runner.handleCloneResult(workspaceId, report))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
    verify(runner).cloneFail(ArgumentMatchers.any(), ArgumentMatchers.any())(ArgumentMatchers.any[ExecutionContext]())
  }

}
