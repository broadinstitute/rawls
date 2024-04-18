package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.CloneWorkspaceResult
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{JobStatus, JobType}
import org.broadinstitute.dsde.rawls.model.RawlsUserEmail
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CreateFailed
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

abstract class WorkspaceCloningStep(
                                     val workspaceRepository: WorkspaceRepository,
                                     val monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
                                     val workspaceId: UUID,
                                     val job: WorkspaceManagerResourceMonitorRecord
                                   ) extends LazyLogging {

  val jobType: JobType.JobType


  def scheduleNextJob(nextJobId: UUID)(
    implicit executionContext: ExecutionContext): Future[Unit] = {
    val nextJobType = job.jobType match {
      case JobType.CloneWorkspaceInit =>
        Some(JobType.CreateWdsAppInClonedWorkspace)
      case JobType.CreateWdsAppInClonedWorkspace => Some(JobType.CloneWorkspaceContainerInit)
      case JobType.CloneWorkspaceContainerInit => Some(JobType.CloneWorkspaceContainerResult)
      case JobType.CloneWorkspaceContainerResult => None
      // This should be caught earlier, but better to be explicit and fail fast
      case _ => throw new IllegalArgumentException(s"Invalid job type for clone job: ${job.jobType}")
    }

    nextJobType.map(jobType =>
      monitorRecordDao.create(
        WorkspaceManagerResourceMonitorRecord.forCloneWorkspace(
          nextJobId,
          job.workspaceId.get,
          job.userEmail.map(RawlsUserEmail).get,
          job.args,
          jobType
        )
      )
    ).getOrElse(Future())
  }


  def fail(operationName: String, result: CloneWorkspaceResult)(implicit ex: ExecutionContext): Future[Int] = {
    val errorMessage = Option(result.getErrorReport)
      .map(report => report.getMessage)
      .getOrElse("Error not specified in job")
    val jobId = result.getJobReport.getId
    val message = s"Workspace Clone Operation $operationName failed for jobId $jobId: $errorMessage"
    // todo: log as well?
    workspaceRepository.setFailedState(workspaceId, CreateFailed, message)
  }

  def fail(operationName: String, errorMessage: String)(implicit executionContext: ExecutionContext): Future[Int] = {
    val message = s"Workspace Clone Operation $operationName failed: $errorMessage"
    // todo: log?
    workspaceRepository.setFailedState(workspaceId, CreateFailed, message)
  }

}
