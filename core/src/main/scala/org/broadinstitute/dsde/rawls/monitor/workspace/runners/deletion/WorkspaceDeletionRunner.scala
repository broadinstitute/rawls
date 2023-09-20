package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{
  LeoAppDeletionPoll,
  LeoRuntimeDeletionPoll,
  WSMWorkspaceDeletionPoll,
  WorkspaceDeleteInit
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace, WorkspaceState}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.UserCtxCreator
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.{
  LeonardoResourceDeletionAction,
  WsmDeletionAction
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Orchestrates the deletion of a workspace with downstream resources. The process looks like this:
  *
  * 1. Sanity check inputs and ensure the workspace is ready for deletion
  * 2. Fire a request to Leonardo to delete any applications
  * 3. Fire a request to Leonardo to delete any runtimes
  * 4. Fire a request to WSM to delete the WSM workspace (including any cloud resources like containers, batch pools, etc.)
  * 5. Delete the rawls record 
  * 
  * If a downstream operation fails or times out during deletion, we mark the workspace as `DeleteFailed` and finish the job
  */
class WorkspaceDeletionRunner(val samDAO: SamDAO,
                              workspaceManagerDAO: WorkspaceManagerDAO,
                              workspaceRepository: WorkspaceRepository,
                              leonardoResourceDeletionAction: LeonardoResourceDeletionAction,
                              wsmDeletionAction: WsmDeletionAction,
                              val gcsDAO: GoogleServicesDAO,
                              monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging
    with UserCtxCreator {

  override def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    if (!JobType.deleteJobTypes.contains(job.jobType)) {
      throw new IllegalArgumentException(s"${this.getClass.getSimpleName} called with invalid job type: ${job.jobType}")
    }
    val workspaceId = job.workspaceId match {
      case Some(id) => id
      case None =>
        logger.error(
          s"Job to monitor workspace deletion created with id ${job.jobControlId} but no workspace ID"
        )
        return Future.successful(Complete)
    }

    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        logger.error(
          s"Job to monitor workspace deletion for workspace id = ${workspaceId} created with id ${job.jobControlId} but no user email set"
        )
        return workspaceRepository.updateState(workspaceId, WorkspaceState.DeleteFailed).map(_ => Complete)
    }

    for {
      // get the associated workspace
      maybeWorkspace <- workspaceRepository.getWorkspace(workspaceId)
      workspace: Workspace <- maybeWorkspace match {
        case Some(ws) => Future.successful(ws)
        case None =>
          logger.error(
            s"Workspace not found for deletion [workspaceId=${workspaceId}, jobControlId = ${job.jobControlId}]"
          )
          Future.failed(
            new WorkspaceDeletionException(
              s"Workspace not found for deletion [workspaceId=${workspaceId}, jobControlId = ${job.jobControlId.toString}, jobType=${job.jobType}]"
            )
          )
      }
      ctx <- getUserCtx(userEmail)

      result <- runStep(job, workspace, ctx).recoverWith { case t: Throwable =>
        logger.error(
          s"Workspace deletion failed [workspaceId=${workspaceId}, jobControlId=${job.jobControlId}, jobType=${job.jobType}]",
          t
        )
        workspaceRepository.updateState(job.workspaceId.get, WorkspaceState.DeleteFailed).map(_ => Complete)
      }
    } yield {
      logger.info(
        s"Finished workspace deletion step [workspaceId=${workspaceId}, jobControlId=${job.jobControlId}, jobType=${job.jobType}]"
      )
      result
    }

  }

  def runStep(job: WorkspaceManagerResourceMonitorRecord, workspace: Workspace, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] =
    job.jobType match {
      case WorkspaceDeleteInit => completeStep(job, workspace, ctx)
      case LeoAppDeletionPoll =>
        leonardoResourceDeletionAction.pollAppDeletion(workspace, ctx).transformWith {
          case Failure(t) =>
            Future.failed(
              new WorkspaceDeletionException(
                s"Workspace deletion failed when deleting leo apps [jobControlId=${job.jobControlId}]",
                t
              )
            )
          case Success(false) => Future.successful(checkTimeout(job))
          case Success(true)  => completeStep(job, workspace, ctx)
        }
      case LeoRuntimeDeletionPoll =>
        leonardoResourceDeletionAction.pollRuntimeDeletion(workspace, ctx).transformWith {
          case Failure(t) =>
            Future.failed(
              new WorkspaceDeletionException(
                s"Workspace deletion failed when deleting leo resources [jobControlId=${job.jobControlId}]",
                t
              )
            )
          case Success(false) => Future.successful(checkTimeout(job))
          case Success(true)  => completeStep(job, workspace, ctx)
        }
      case WSMWorkspaceDeletionPoll =>
        wsmDeletionAction.pollForCompletion(workspace, job.jobControlId.toString, ctx) transformWith {
          case Success(true) =>
            completeStep(job, workspace, ctx)
          case Success(false) => Future.successful(checkTimeout(job))
          case Failure(e)     => Future.failed(e)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"${this.getClass.getSimpleName} called with invalid job type: ${job.jobType}"
        )

    }

  def checkTimeout(job: WorkspaceManagerResourceMonitorRecord): JobStatus = {
    val timeoutIntervalMinutes = job.jobType match {
      case LeoAppDeletionPoll       => 10
      case LeoRuntimeDeletionPoll   => 20
      case WSMWorkspaceDeletionPoll => 30
      case _                        => 0
    }
    val now = Timestamp.from(Instant.now())
    val expireTime = Timestamp.from(Instant.ofEpochMilli(job.createdTime.getTime + (timeoutIntervalMinutes * 60000)))
    if (Timestamp.from(Instant.now()).after(expireTime)) {
      workspaceRepository.updateState(job.workspaceId.get, WorkspaceState.DeleteFailed)
      Complete
    } else {
      Incomplete
    }

  }

  def completeStep(job: WorkspaceManagerResourceMonitorRecord, workspace: Workspace, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] =
    job.jobType match {
      case WorkspaceDeleteInit =>
        for {
          _ <- leonardoResourceDeletionAction.deleteApps(workspace, ctx)
          _ <- monitorRecordDao.update(job.copy(jobType = LeoAppDeletionPoll))
        } yield Incomplete
      case LeoAppDeletionPoll =>
        for {
          _ <- leonardoResourceDeletionAction.deleteRuntimes(workspace, ctx)
          _ <- monitorRecordDao.update(job.copy(jobType = LeoRuntimeDeletionPoll))
        } yield Incomplete
      case LeoRuntimeDeletionPoll =>
        wsmDeletionAction.startStep(workspace, job.jobControlId.toString, ctx)
        monitorRecordDao
          .update(job.copy(jobType = WSMWorkspaceDeletionPoll))
          .map(_ => Incomplete)
      case WSMWorkspaceDeletionPoll =>
        logger.info(
          s"Deleting rawls workspace record [workspaceId=${workspace.workspaceId}, jobControlId=${job.jobControlId}, jobType=${job.jobType}]"
        )
        workspaceRepository.deleteWorkspaceRecord(workspace).map(_ => Complete)
    }

}

class WorkspaceDeletionException(message: String = null, cause: Throwable = null) extends RawlsException(message, cause)
