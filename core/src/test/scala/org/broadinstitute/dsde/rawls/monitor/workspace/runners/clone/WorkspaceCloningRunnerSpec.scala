package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, Workspace}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.joda.time.DateTime
import org.scalatest.concurrent.ScalaFutures
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, doReturn, spy, verify}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class WorkspaceCloningRunnerSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext



  val userEmail: String = "user@email.com"
  val workspaceId: UUID = UUID.randomUUID()
  val wsCreatedDate: DateTime = DateTime.parse("2023-01-18T10:08:48.541-05:00")
  val monitorRecord: WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord.forCloneWorkspaceContainer(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail)
    )

  def cloneJobMonitorRecord(jobType: JobType, args: Map[String, String] = Map.empty): WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(args),
      jobType
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



  behavior of "getStep"

  it should "return CloneWorkspaceInitStep for JobType of CloneWorkspaceInit" in {
    val runner = new WorkspaceCloningRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[LeonardoDAO],
      mock[WorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository]
    )
    val step = runner.getStep(cloneJobMonitorRecord(JobType.CloneWorkspaceInit), workspaceId)
    step shouldBe a [CloneWorkspaceInitStep]
  }

  it should "return CloneWorkspaceCreateWDSAppStep for JobType of CreateWdsAppInClonedWorkspace" in {
    val runner = new WorkspaceCloningRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[LeonardoDAO],
      mock[WorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository]
    )
    val step = runner.getStep(cloneJobMonitorRecord(JobType.CreateWdsAppInClonedWorkspace), workspaceId)
    step shouldBe a[CloneWorkspaceCreateWDSAppStep]
  }

  it should "return CloneWorkspaceStorageContainerInitStep for JobType of CloneWorkspaceContainerInit" in {
    val runner = new WorkspaceCloningRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[LeonardoDAO],
      mock[WorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository]
    )
    val step = runner.getStep(cloneJobMonitorRecord(JobType.CloneWorkspaceContainerInit), workspaceId)
    step shouldBe a[CloneWorkspaceStorageContainerInitStep]
  }

  it should "return CloneWorkspaceAwaitStorageContainerStep for JobType of CloneWorkspaceContainerResult" in {
    val runner = new WorkspaceCloningRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[LeonardoDAO],
      mock[WorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository]
    )
    val step = runner.getStep(cloneJobMonitorRecord(JobType.CloneWorkspaceContainerResult), workspaceId)
    step shouldBe a[CloneWorkspaceAwaitStorageContainerStep]
  }

  it should "throw an exception for unmapped types" in {
    val runner = new WorkspaceCloningRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[LeonardoDAO],
      mock[WorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository]
    )
    // We have to construct this manually to avoid the check in `WorkspaceManagerResourceMonitorRecord.forCloneWorkspace`
    val job = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.AzureLandingZoneResult,
      workspaceId = Some(workspaceId),
      billingProjectId = None,
      userEmail = Some(userEmail),
      Timestamp.from(Instant.now()),
      Some(Map.empty)
    )
    an [IllegalArgumentException] should be thrownBy runner.getStep(job, workspaceId)
  }



}
