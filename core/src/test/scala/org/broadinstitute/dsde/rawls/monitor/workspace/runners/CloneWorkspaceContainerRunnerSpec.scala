package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeName, RawlsUserEmail, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.CloneWorkspaceContainerRunnerSpec.{
  monitorRecord,
  userEmail,
  workspace,
  workspaceId
}
import org.joda.time.DateTime
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.{doAnswer, doReturn, spy, verify, when, RETURNS_SMART_NULLS}
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

  /*it should "return a completed status if no workspace is found for the id" in {
    val runner = spy(
      new CloneWorkspaceContainerRunner(
        mock[SamDAO],
        mock[WorkspaceManagerDAO],
        mock[SlickDataSource],
        mock[GoogleServicesDAO]
      )
    )
    // TODO: This needs an otherwise successful setup, up until the first use of the workspace
    doReturn(Future.successful(None)).when(runner).getWorkspace(monitorRecord.workspaceId.get)
    doReturn(Future.successful(new org.broadinstitute.dsde.workbench.client.sam.ApiException()))
      .when(runner)
      .getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())
    whenReady(runner(monitorRecord))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }*/

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

  behavior of "handling the clone container report"

  it should "set completedCloneWorkspaceFileTransfer on the workspace to the complete time in the report" in {}

  it should "not set completedCloneWorkspaceFileTransfer on failures" in {}

}
