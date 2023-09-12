package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, JobType}
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

import scala.concurrent.{ExecutionContext, Future}

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
                              val gcsDAO: GoogleServicesDAO
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

      result <- runWorkspaceDeletionSteps(workspace, ctx, job.jobControlId.toString).recoverWith { case t: Throwable =>
        logger.error(
          s"Workspace deletion failed; workspaceId = ${workspaceId}, jobControlId = ${job.jobControlId.toString}",
          t
        )
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
}
