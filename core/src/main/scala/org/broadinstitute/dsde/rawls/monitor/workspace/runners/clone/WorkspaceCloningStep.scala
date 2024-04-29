package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.CloneWorkspaceResult
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{JobStatus, JobType}
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsUserEmail}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CloningFailed
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

abstract class WorkspaceCloningStep(
  val workspaceRepository: WorkspaceRepository,
  val monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
  val workspaceId: UUID,
  val job: WorkspaceManagerResourceMonitorRecord
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging {

  val jobType: JobType.JobType

  def runStep(userCtx: RawlsRequestContext): Future[JobStatus]

  def scheduleNextJob(nextJobId: UUID): Future[Unit] = {
    val nextJobType = job.jobType match {
      case JobType.CloneWorkspaceInit                 => Some(JobType.CreateWdsAppInClonedWorkspace)
      case JobType.CreateWdsAppInClonedWorkspace      => Some(JobType.CloneWorkspaceContainerInit)
      case JobType.CloneWorkspaceContainerInit        => Some(JobType.CloneWorkspaceAwaitContainerResult)
      case JobType.CloneWorkspaceAwaitContainerResult => None
      // This should be caught in the WorkspaceCloningRunner, but better to be explicit and fail fast
      case _ => throw new IllegalArgumentException(s"Invalid job type for clone job: ${job.jobType}")
    }

    val nextJobMessage = nextJobType match {
      case Some(next) => s"; scheduling next job of type [$next] with id: [$nextJobId]"
      case None       => ""
    }
    logger.debug(s"Clone Workspace Job [${job.jobControlId}], job type [${job.jobType}] Complete$nextJobMessage")

    nextJobType
      .map(jobType =>
        monitorRecordDao.create(
          WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
            nextJobId,
            job.workspaceId.get,
            job.userEmail.map(RawlsUserEmail).get,
            job.args,
            jobType
          )
        )
      )
      .getOrElse(Future())
  }

  def fail(operationName: String, result: CloneWorkspaceResult): Future[Int] = {
    val errorMessage = Option(result.getErrorReport)
      .map(report => report.getMessage)
      .getOrElse("Error not specified in job")
    val jobId = result.getJobReport.getId
    val message = s"${errorContextString(operationName)}, failed for jobId [$jobId]]: $errorMessage"
    logger.error(s"${job.jobType} failure: $message")
    workspaceRepository.setFailedState(workspaceId, CloningFailed, message)
  }

  def fail(operationName: String, errorMessage: String): Future[Int] = {
    val message = s"${errorContextString(operationName)} failed for jobId [${job.jobControlId}]: $errorMessage"
    logger.error(s"${job.jobType} failure: $message")
    workspaceRepository.setFailedState(workspaceId, CloningFailed, message)
  }

  private def errorContextString(operationName: String): String = {
    val sourceWorkspaceId = WorkspaceCloningRunner.getSourceWorkspaceId(job.args).map(_.toString).getOrElse("Unknown")
    s"Workspace Clone Operation [$operationName], source workspace: [$sourceWorkspaceId], dest workspace [$workspaceId]"
  }
}
