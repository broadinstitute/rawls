package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{
  JobType,
  PollLeoRuntimeDeletion,
  PollWsmDeletion,
  StartWsmDeletion
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
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

import java.util.UUID
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
                              workspaceManagerResourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
                              val gcsDAO: GoogleServicesDAO
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging
    with UserCtxCreator {

  val DELETION_JOB_TYPES = Set(
    JobType.WorkspaceDelete,
    JobType.StartLeoAppDeletion,
    JobType.PollLeoAppDeletion,
    JobType.PollLeoRuntimeDeletion,
    JobType.StartWsmDeletion,
    JobType.PollWsmDeletion
  )

  override def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    if (!DELETION_JOB_TYPES.contains(job.jobType))
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

      result <- runWorkspaceDeletionStep(workspace, job, ctx, job.jobControlId.toString).recoverWith {
        case t: Throwable =>
          logger.error(
            s"Workspace deletion failed; workspaceId = ${workspaceId}, jobControlId = ${job.jobControlId.toString}",
            t
          )
          workspaceRepository.updateState(job.workspaceId.get, WorkspaceState.DeleteFailed).map(_ => Complete)
      }
    } yield {
      logger.info(s"Finished workspace deletion step for workspaceId = ${workspaceId}")
      result
    }

  }

  private def runWorkspaceDeletionStep(workspace: Workspace,
                                       job: WorkspaceManagerResourceMonitorRecord,
                                       ctx: RawlsRequestContext,
                                       jobControlId: String
  )(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    logger.info(
      s"Starting downstream resource deletion for workspace ID ${workspace.workspaceId}, jobControlId = ${jobControlId}"
    )

    val result = job.jobType match {
      case JobType.WorkspaceDelete =>
        leonardoResourceDeletionAction
          .deleteRuntimes(workspace, ctx)
          .flatMap(_ => enqueueRecord(workspace, PollLeoRuntimeDeletion, ctx))
          .map(_ => Complete)
      case JobType.PollLeoRuntimeDeletion =>
        leonardoResourceDeletionAction.pollRuntimeDeletion(workspace, job, ctx).flatMap {
          case false => Future.successful(Incomplete)
          case true =>
            enqueueRecord(workspace, JobType.StartLeoAppDeletion, ctx)
              .map(_ => Complete)
        }
      case JobType.StartLeoAppDeletion =>
        leonardoResourceDeletionAction
          .deleteApps(workspace, ctx)
          .flatMap(_ => enqueueRecord(workspace, JobType.PollLeoAppDeletion, ctx))
          .map(_ => Complete)
      case JobType.PollLeoAppDeletion =>
        leonardoResourceDeletionAction.pollAppDeletion(workspace, job, ctx).flatMap {
          case false => Future.successful(Incomplete)
          case true =>
            enqueueRecord(workspace, StartWsmDeletion, ctx)
              .map(_ => Complete)
        }
      case JobType.StartWsmDeletion =>
        wsmDeletionAction
          .startStep(workspace, jobControlId, ctx)
          .flatMap(_ => enqueueRecord(workspace, PollWsmDeletion, ctx))
          .map(_ => Complete)
      case JobType.PollWsmDeletion =>
        wsmDeletionAction.isComplete(workspace, job, ctx).flatMap {
          case false => Future.successful(Incomplete)
          case true =>
            logger.info(
              s"Workspace manager deletion complete, deleting rawls record. [workspaceId=${workspace.workspaceId}, jobControlId=${job.jobControlId}]"
            )
            workspaceRepository.deleteWorkspaceRecord(workspace).map(_ => Complete)
        }
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"Invalid job type for deletion [jobType = ${job.jobType}, jobControlId=${job.jobControlId}]"
          )
        )
    }
    result

  }

  private def enqueueRecord(workspace: Workspace, jobType: JobType, ctx: RawlsRequestContext): Future[Unit] =
    workspaceManagerResourceMonitorRecordDao
      .create(
        WorkspaceManagerResourceMonitorRecord.forJobType(
          UUID.randomUUID(),
          workspace.workspaceIdAsUUID,
          ctx.userInfo.userEmail,
          jobType
        )
      )
}

class WorkspaceDeletionException(message: String) extends RawlsException(message) {}
