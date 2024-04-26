package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  JobStatus,
  JobType
}

import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class CloneWorkspaceCreateWDSAppStep(
  val leonardoDAO: LeonardoDAO,
  workspaceRepository: WorkspaceRepository,
  monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
  workspaceId: UUID,
  job: WorkspaceManagerResourceMonitorRecord
)(implicit executionContext: ExecutionContext)
    extends WorkspaceCloningStep(workspaceRepository, monitorRecordDao, workspaceId, job) {

  override val jobType: JobType = JobType.CreateWdsAppInClonedWorkspace

  override def runStep(userCtx: RawlsRequestContext): Future[JobStatus] =
    if (WorkspaceCloningRunner.isAutomaticAppCreationDisabled(job.args)) {
      scheduleNextJob(UUID.randomUUID()).map(_ => Complete)
    } else {
      Future(
        leonardoDAO.createWDSInstance(
          userCtx.userInfo.accessToken.token,
          workspaceId,
          WorkspaceCloningRunner.getSourceWorkspaceId(job.args)
        )
      ).map(_ => scheduleNextJob(UUID.randomUUID()))
        .recoverWith { case t: Throwable =>
          fail("Creating WDS Instance", t.getMessage)
        }
        .map(_ => Complete)
    }

}
