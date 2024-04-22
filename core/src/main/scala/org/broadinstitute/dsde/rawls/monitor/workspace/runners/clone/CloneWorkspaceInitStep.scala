package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.JobReport.StatusEnum
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, Incomplete, JobStatus, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, WorkspaceState}
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceManagerPollingOperationException, WorkspaceRepository}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class CloneWorkspaceInitStep(
                               val workspaceManagerDAO: WorkspaceManagerDAO,
                               workspaceRepository: WorkspaceRepository,
                               monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
                               workspaceId: UUID,
                               job: WorkspaceManagerResourceMonitorRecord
                            )
                            (implicit executionContext: ExecutionContext)
  extends WorkspaceCloningStep(workspaceRepository, monitorRecordDao, workspaceId, job) {

  override val jobType: JobType = JobType.CloneWorkspaceInit

  override def runStep(userCtx: RawlsRequestContext): Future[JobStatus] = {
    val result = workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, job.jobControlId.toString, userCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED =>
       // workspaceRepository.updateState(workspaceId, WorkspaceState.Cloning).flatMap { _ =>
          scheduleNextJob(UUID.randomUUID()).map(_ => Complete)
        //}
      case StatusEnum.RUNNING => Future(Incomplete)
      case StatusEnum.FAILED =>
          fail("Initial Workspace Creation", result).map(_ => Complete)
    }
  }

}


