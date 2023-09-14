package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{LeoAppDeletionPoll, LeoRuntimeDeletionPoll, WSMWorkspaceDeletionPoll}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, Incomplete, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkspaceManagerResourceJobRunner, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace, WorkspaceState}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.UserCtxCreator
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.{LeonardoResourceDeletionAction, WsmDeletionAction}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

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
    if (!job.jobType.equals(JobType.WorkspaceDelete))
      throw new IllegalArgumentException(s"${this.getClass.getSimpleName} called with invalid job type: ${job.jobType}")

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
            s"Workspace not found for deletion, id = ${workspaceId.toString}, jobControlId = ${job.jobControlId.toString}"
          )
          Future.failed(
            new WorkspaceDeletionException(
              s"Workspace not found for deletion, id = ${workspaceId.toString}, jobControlId = ${job.jobControlId.toString}"
            )
          )
      }
      ctx <- getUserCtx(userEmail)

      result <- runStep(job, workspace, ctx).recoverWith { case t: Throwable =>
        logger.error(
          s"Workspace deletion failed; workspaceId = ${workspaceId}, jobControlId = ${job.jobControlId.toString}",
          t
        )
        // TODO: add error message to workspace?
        workspaceRepository.updateState(job.workspaceId.get, WorkspaceState.DeleteFailed).map(_ => Complete)
      }
    } yield {
      logger.info(s"Finished workspace deletion for workspaceId = ${workspaceId}")
      result
    }

  }

  private def runWorkspaceDeletionSteps(workspace: Workspace, ctx: RawlsRequestContext, jobControlId: String)(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    logger.info(
      s"Starting downstream resource deletion for workspace ID ${workspace.workspaceId}, jobControlId = ${jobControlId}"
    )

    // run the leo operations in parallel
    val leoAppsDeletionFuture = leonardoResourceDeletionAction.deleteApps(workspace, ctx)
    val leoRuntimesDeletionFuture = leonardoResourceDeletionAction.deleteRuntimes(workspace, ctx)

    val result = for {
      _ <- leoAppsDeletionFuture
      _ <- leonardoResourceDeletionAction.pollAppDeletion(workspace, ctx)
      _ <- leoRuntimesDeletionFuture
      _ <- leonardoResourceDeletionAction.pollRuntimeDeletion(workspace, ctx)
      _ <- wsmDeletionAction.startStep(workspace, jobControlId, ctx)
      _ <- wsmDeletionAction.pollOperation(workspace, jobControlId, ctx)
      _ <- workspaceRepository.deleteWorkspaceRecord(workspace)
    } yield {
      logger.info(
        s"Finished downstream resource deletion for workspace ID ${workspace.workspaceId}, jobControlId = ${jobControlId}"
      )
      Complete
    }

    result
  }


  def runStep(job: WorkspaceManagerResourceMonitorRecord, workspace: Workspace, ctx: RawlsRequestContext)(implicit
                                                                                                          executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus]  =
    job.jobType match {
      case LeoAppDeletionPoll  =>
        leonardoResourceDeletionAction.pollAppDeletion(workspace, ctx).transformWith {
          case Failure(t) =>
            Future.failed(new WorkspaceDeletionException("Workspace deletion failed when deleting leo resources"))
          case Success(false) =>
            // TODO: time out if time elapsed since job start time is > some threshold
            Future.successful(Incomplete)
          case Success(true) => completeStep(job, workspace, ctx)
        }
      case LeoRuntimeDeletionPoll =>
        leonardoResourceDeletionAction.pollRuntimeDeletion(workspace, ctx).transformWith {
          case Failure(t) =>
            Future.failed(new WorkspaceDeletionException("Workspace deletion failed when deleting leo resources"))
          case Success(false) =>
            // TODO: time out if time elapsed since job start time is > some threshold
            Future.successful(Incomplete)
          case Success(true) => completeStep(job, workspace, ctx)
        }
      case WSMWorkspaceDeletionPoll => Try(wsmDeletionAction.pollDeletionComplete(workspace, job.jobControlId.toString, ctx)) match {
        case Success(true) =>
          completeStep(job, workspace, ctx)
        case Success(false) =>
        // TODO: time out if time elapsed since job start time is > some threshold
          Future.successful(Incomplete)
        case Failure(e) => Future.failed(e)
      }
      case _ =>
        // TODO
        Future.successful(Complete)
    }

  def completeStep(job: WorkspaceManagerResourceMonitorRecord, workspace: Workspace, ctx: RawlsRequestContext)(implicit
                                                               executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    job.jobType match {
      case LeoAppDeletionPoll => for {
        _ <- leonardoResourceDeletionAction.deleteRuntimes(workspace, ctx)
        _ <- monitorRecordDao.update(job.copy(jobType = LeoRuntimeDeletionPoll))
      } yield Incomplete
      case LeoRuntimeDeletionPoll =>
        // todo: start wsm deletion
        wsmDeletionAction.startStep(workspace, job.jobControlId.toString, ctx)
        monitorRecordDao.update(job.copy(jobType = WSMWorkspaceDeletionPoll))
          .map(_ => Incomplete)
      case WSMWorkspaceDeletionPoll =>
        workspaceRepository.deleteWorkspaceRecord(workspace).map(_ => Complete)
    }
  }

}

class WorkspaceDeletionException(message: String) extends RawlsException(message) {}
