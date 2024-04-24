package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport
import bio.terra.workspace.model.JobReport.StatusEnum
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, Incomplete, JobStatus, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, WorkspaceState}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.joda.time.DateTime

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CloneWorkspaceAwaitStorageContainerStep(
  val workspaceManagerDAO: WorkspaceManagerDAO,
  workspaceRepository: WorkspaceRepository,
  monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
  workspaceId: UUID,
  job: WorkspaceManagerResourceMonitorRecord
)(implicit executionContext: ExecutionContext)
    extends WorkspaceCloningStep(workspaceRepository, monitorRecordDao, workspaceId, job) {

  override val jobType: JobType = JobType.CloneWorkspaceContainerInit


  override def runStep(userCtx: RawlsRequestContext): Future[JobStatus] = {
    val operationName = "Await Storage Container Clone"
    Try(workspaceManagerDAO.getJob(job.jobControlId.toString, userCtx)) match {
      case Success(result) => handleCloneResult(workspaceId, result)
      case Failure(e: ApiException) =>
        e.getMessage
        e.getCode match {
          case 500 =>
            fail(operationName, e.getMessage).map(_ => Complete)
          case 404 =>
            val msg = s"Unable to find job in WSM for clone container operation"
            fail(operationName, msg).map(_ => Complete)
          case 401 =>
            val msg = s"Unable to get job result, user is unauthed with jobId ${job.jobControlId}: ${e.getMessage}"
            fail(operationName, msg).map(_ => Complete)
          case code =>
            logger.error(s"API call to get clone result failed with status code $code: ${e.getMessage}")
            Future.successful(Incomplete)
        }
      case Failure(t) =>
        val msg = s"API call to get clone result from workspace manager failed with: ${t.getMessage}"
        fail(operationName, msg).map(_ => Incomplete)
    }
  }


  def handleCloneResult(workspaceId: UUID, result: JobReport)(implicit
                                                              executionContext: ExecutionContext
  ): Future[JobStatus] = result.getStatus match {
    case JobReport.StatusEnum.RUNNING => Future.successful(Incomplete)
    case JobReport.StatusEnum.SUCCEEDED =>
      val completeTime = DateTime.parse(result.getCompleted)
      cloneSuccess(workspaceId, completeTime).map(_ => Complete)
    // set the error, and indicate this runner is finished with the job
    case JobReport.StatusEnum.FAILED =>
      fail("Cloning Workspace Resource Container", "Cloning workspace resource container failed").map(_ => Complete)
  }

  /**
    * If the workspace still exists, updates the clone completed time on the workspace. Otherwise does nothing.
    *
    * @param wsId       the ID of the workspace
    * @param finishTime the time cloning completed
    * @return the number of records updated, wrapped in a Future
    */
  def cloneSuccess(wsId: UUID, finishTime: DateTime)(implicit
                                                     executionContext: ExecutionContext
  ): Future[Int] = {
    logger.debug(s"Cloning complete for workspace $wsId")
    workspaceRepository.updateState(wsId, WorkspaceState.Ready)
    workspaceRepository.updateCompletedCloneWorkspaceFileTransfer(wsId, finishTime)

  }
}
