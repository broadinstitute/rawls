package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.CloneWorkspaceResult
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CreateFailed
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait WorkspaceCloneStatusUpdate extends LazyLogging {
  val workspaceRepository: WorkspaceRepository
  val workspaceId: UUID

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
    // todo: log
    workspaceRepository.setFailedState(workspaceId, CreateFailed, message)
  }

}

