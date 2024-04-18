package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.CloneWorkspaceResult
import bio.terra.workspace.model.JobReport.StatusEnum
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, Incomplete, JobStatus}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.model.WorkspaceState.CreateFailed
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceManagerPollingOperationException, WorkspaceRepository}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class CloneWorkspaceInitStep(
                              val workspaceManagerDAO: WorkspaceManagerDAO,
                              val workspaceRepository: WorkspaceRepository,
                              val workspaceId: UUID,
                              val jobId: UUID
                            )
                            (implicit executionContext: ExecutionContext) extends WorkspaceCloneStatusUpdate {

  def runStep(userCtx: RawlsRequestContext): Future[JobStatus] = {
    val result = workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, jobId.toString, userCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED =>
        // no status update needed (this is all under the 'Cloning Resources' state)
        // the next job will be scheduled by the runner
        Future(Complete)
      case StatusEnum.RUNNING => Future(Incomplete)
      case StatusEnum.FAILED =>
          fail("Initial Workspace Creation", result).map(_ => Complete)
    }
  }


}


