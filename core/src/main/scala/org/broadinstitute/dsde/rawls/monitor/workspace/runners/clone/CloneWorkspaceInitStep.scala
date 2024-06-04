package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.CloneResourceResult
import bio.terra.workspace.model.JobReport.StatusEnum
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class CloneWorkspaceInitStep(
  val workspaceManagerDAO: WorkspaceManagerDAO,
  workspaceRepository: WorkspaceRepository,
  monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
  workspaceId: UUID,
  job: WorkspaceManagerResourceMonitorRecord
)(implicit executionContext: ExecutionContext)
    extends WorkspaceCloningStep(workspaceRepository, monitorRecordDao, workspaceId, job) {

  override val jobType: JobType = JobType.CloneWorkspaceInit

  override def runStep(userCtx: RawlsRequestContext): Future[JobStatus] = {
    val jobId = WorkspaceCloningRunner.getInitialWSMJobId(job.args) match {
      case None =>
        return fail("Initial Workspace Clone", "No JobId for WSM cloning operation provided")
          .map(_ => Complete)
      case Some(id) => id
    }
    val result = workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, jobId, userCtx)
    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED =>
        val workspaceResult = result.getWorkspace();
        val errorMessages = new StringBuilder
        if (workspaceResult != null) {
          for (resource <- workspaceResult.getResources().asScala)
            if (resource.getResult() == CloneResourceResult.FAILED) {
              if (errorMessages.nonEmpty) {
                errorMessages.append(", ");
              }
              errorMessages.append(s"resource (${resource.getName()}, ${resource
                  .getResourceType()}) failed to clone with error \"${resource.getErrorMessage()}\"");
            }
        }
        if (errorMessages.nonEmpty) {
          fail("Initial Workspace Clone", errorMessages.result()).map(_ => Complete)
        } else {
          scheduleNextJob(UUID.randomUUID()).map(_ => Complete)
        }
      case StatusEnum.RUNNING => Future(Incomplete)
      case StatusEnum.FAILED =>
        fail("Initial Workspace Clone", result).map(_ => Complete)
    }
  }

}
