package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.{
  CloneResourceResult,
  CloneWorkspaceResult,
  ClonedWorkspace,
  JobReport,
  ResourceCloneDetails,
  ResourceType
}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CloningFailed
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doAnswer, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class CloneWorkspaceInitStepSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with ScalaFutures {

  val userEmail: String = "user@email.com"
  val workspaceId: UUID = UUID.randomUUID()
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "waiting for the initial cloning job to complete in CloneWorkspaceInitStepSpec"

  it should "return Incomplete when the job report status is RUNNING" in {
    val jobId = UUID.randomUUID()
    val cloneJobId = "not-a-real-uuid"
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      jobId,
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.WORKSPACE_INITIAL_CLONE_JOBID_KEY -> cloneJobId)),
      JobType.CloneWorkspaceInit
    )
    val ctx = mock[RawlsRequestContext]
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    when(workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, cloneJobId, ctx))
      .thenReturn(new CloneWorkspaceResult().jobReport(new JobReport().status(JobReport.StatusEnum.RUNNING)))
    val step = new CloneWorkspaceInitStep(
      workspaceManagerDAO,
      mock[WorkspaceRepository],
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceId,
      monitorRecord
    )

    whenReady(step.runStep(ctx))(_ shouldBe Incomplete)
  }

  it should "complete and update the workspace when the job report status is FAILED" in {
    val jobId = UUID.randomUUID()
    val cloneJobId = "not-a-real-uuid"
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      jobId,
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.WORKSPACE_INITIAL_CLONE_JOBID_KEY -> cloneJobId)),
      JobType.CloneWorkspaceInit
    )
    val ctx = mock[RawlsRequestContext]
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    when(workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, cloneJobId, ctx))
      .thenReturn(new CloneWorkspaceResult().jobReport(new JobReport().status(JobReport.StatusEnum.FAILED)))
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.setFailedState(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(CloningFailed),
        ArgumentMatchers.anyString()
      )
    ).thenReturn(Future(1))
    val step = new CloneWorkspaceInitStep(
      workspaceManagerDAO,
      workspaceRepository,
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceId,
      monitorRecord
    )

    whenReady(step.runStep(ctx)) {
      _ shouldBe Complete
    }
    verify(workspaceRepository).setFailedState(
      ArgumentMatchers.eq(workspaceId),
      ArgumentMatchers.eq(CloningFailed),
      ArgumentMatchers.anyString()
    )
  }

  it should "complete and update the workspace as CloningFailed when resources failed to clone" in {
    val jobId = UUID.randomUUID()
    val cloneJobId = "not-a-real-uuid"
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      jobId,
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.WORKSPACE_INITIAL_CLONE_JOBID_KEY -> cloneJobId)),
      JobType.CloneWorkspaceInit
    )
    val ctx = mock[RawlsRequestContext]
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    when(workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, cloneJobId, ctx))
      .thenReturn(
        new CloneWorkspaceResult()
          .workspace(
            new ClonedWorkspace().resources(
              Set(
                new ResourceCloneDetails()
                  .name("failedResource1")
                  .resourceType(ResourceType.AZURE_VM)
                  .result(CloneResourceResult.FAILED)
                  .errorMessage("error1"),
                new ResourceCloneDetails()
                  .name("succeededResource")
                  .resourceType(ResourceType.AZURE_VM)
                  .result(CloneResourceResult.SUCCEEDED),
                new ResourceCloneDetails()
                  .name("skippedResource")
                  .resourceType(ResourceType.AZURE_VM)
                  .result(CloneResourceResult.SKIPPED),
                new ResourceCloneDetails()
                  .name("failedResource2")
                  .resourceType(ResourceType.AZURE_DATABASE)
                  .result(CloneResourceResult.FAILED)
                  .errorMessage("error2")
              ).toList.asJava
            )
          )
          .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
      )
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.setFailedState(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(CloningFailed),
        ArgumentMatchers.anyString()
      )
    ).thenReturn(Future(1))
    val step = new CloneWorkspaceInitStep(
      workspaceManagerDAO,
      workspaceRepository,
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceId,
      monitorRecord
    )

    whenReady(step.runStep(ctx)) {
      _ shouldBe Complete
    }
    verify(workspaceRepository).setFailedState(
      ArgumentMatchers.eq(workspaceId),
      ArgumentMatchers.eq(CloningFailed),
      ArgumentMatchers.eq(
        s"Workspace Clone Operation [Initial Workspace Clone], source workspace: [Unknown], dest workspace [$workspaceId] failed for jobId [$jobId]: resource (failedResource1, AZURE_VM) failed to clone with error \"error1\", resource (failedResource2, AZURE_DATABASE) failed to clone with error \"error2\""
      )
    )
  }

  it should "complete and schedule the next job when the job report status is SUCCEEDED" in {
    val jobId = UUID.randomUUID()
    val cloneJobId = "not-a-real-uuid"
    val monitorRecord = WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
      jobId,
      workspaceId,
      RawlsUserEmail(userEmail),
      Some(Map(WorkspaceCloningRunner.WORKSPACE_INITIAL_CLONE_JOBID_KEY -> cloneJobId)),
      JobType.CloneWorkspaceInit
    )
    val ctx = mock[RawlsRequestContext]
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    when(workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, cloneJobId, ctx))
      .thenReturn(
        new CloneWorkspaceResult()
          .workspace(
            new ClonedWorkspace().resources(
              Set(
                new ResourceCloneDetails()
                  .name("succeededResource")
                  .resourceType(ResourceType.AZURE_VM)
                  .result(CloneResourceResult.SUCCEEDED),
                new ResourceCloneDetails()
                  .name("skippedResource")
                  .resourceType(ResourceType.AZURE_VM)
                  .result(CloneResourceResult.SKIPPED)
              ).toList.asJava
            )
          )
          .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
      )
    val workspaceRepository = mock[WorkspaceRepository]
    val recordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val record: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      record.workspaceId shouldBe Some(workspaceId)
      record.jobType shouldBe JobType.CreateWdsAppInClonedWorkspace
      Future.successful()
    }.when(recordDao).create(ArgumentMatchers.any())
    val step = new CloneWorkspaceInitStep(
      workspaceManagerDAO,
      workspaceRepository,
      recordDao,
      workspaceId,
      monitorRecord
    )

    whenReady(step.runStep(ctx)) {
      _ shouldBe Complete
    }
    verify(recordDao).create(ArgumentMatchers.any())
  }

}
