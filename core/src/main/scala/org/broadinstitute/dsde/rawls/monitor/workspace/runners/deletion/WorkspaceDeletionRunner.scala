package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{JobResult, WorkspaceDescription}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceDeletionState.WorkspaceDeletionState
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceDeletionState,
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  Workspace,
  WorkspaceDeletionResult,
  WorkspaceState
}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.UserCtxCreator
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// TODO factor the GCS dao out of the interface
class WorkspaceDeletionRunner(val samDAO: SamDAO,
                              workspaceManagerDAO: WorkspaceManagerDAO,
                              workspaceRepository: WorkspaceRepository,
                              leonardoDAO: LeonardoDAO,
                              val gcsDAO: GoogleServicesDAO,
                              val system: ActorSystem
) extends WorkspaceManagerResourceJobRunner
    with Retry
    with LazyLogging
    with UserCtxCreator {

  override def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
    executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    // TODO counters/timers/etc.

    val jobType = job.jobType
    if (jobType != JobType.WorkspaceDelete) {
      // TODO fixup -- is this even necessary
      logger.error(s"Non workspace deletion record?")
      return Future.successful(Complete)
    }
    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        throw new Exception("whoops")
    }
    for {
      // get the associated workspace
      maybeWorkspace <- workspaceRepository.getWorkspace(job.workspaceId.get) // TODO no hard get
      workspace: Workspace <- maybeWorkspace match {
        case Some(ws) => Future.successful(ws)
        case None     => Future.failed(new Exception("whoops")) // TODO fixup
      }
      deletionState <- getWorkspaceDeletionState(workspace)
      _ <- deletionState match {
        case WorkspaceDeletionState.NotStarted              => startAppDeletion(workspace, userEmail)
        case WorkspaceDeletionState.AppDeletionStarted      => pollAppDeletion(workspace, userEmail)
        case WorkspaceDeletionState.AppDeletionFinished     => startRuntimeDeletion(workspace, userEmail)
        case WorkspaceDeletionState.RuntimeDeletionStarted  => pollRuntimeDeletion(workspace, userEmail)
        case WorkspaceDeletionState.RuntimeDeletionFinished => startWsmDeletion(workspace, job.jobControlId, userEmail)
        case WorkspaceDeletionState.WsmDeletionStarted      => pollWsmDeletion(workspace, job.jobControlId, userEmail)
        case WorkspaceDeletionState.WsmDeletionFinished     => finalizeWorkspaceDeletion(workspace)
      }

    } yield
    // todo cleanup
    Complete
  }

  protected def when500(throwable: Throwable): Boolean =
    throwable match {
      case t: ApiException => t.getCode / 100 == 5
      case _               => false
    }

  private def listApps(workspace: Workspace, token: String)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListAppResponse]] =
    retry(when500) { () =>
      Future {
        blocking {
          leonardoDAO.listApps(token, workspace.workspaceIdAsUUID.toString)
        }
      }
    }

  private def listAzureRuntimes(workspace: Workspace, token: String)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListRuntimeResponse]] =
    retry(when500) { () =>
      Future {
        blocking {
          leonardoDAO.listAzureRuntimes(token, workspace.workspaceIdAsUUID.toString)
        }
      }
    }

  private def deleteRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext) =
    retry(when500) { () =>
      Future {
        blocking {
          leonardoDAO.deleteApps(ctx.userInfo.accessToken.token, workspace.workspaceId, deleteDisk = true)
        }
      }
    }

  private def deleteApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext) =
    retry(when500) { () =>
      Future {
        blocking {
          leonardoDAO.deleteApps(ctx.userInfo.accessToken.token, workspace.workspaceId, deleteDisk = true)
        }
      }
    }

  private def startAppDeletion(workspace: Workspace, userEmail: String)(implicit
    ec: ExecutionContext
  ): Future[JobStatus] =
    for {
      ctx <- getUserCtx(userEmail)
      _ <- workspaceRepository.setAppDeletionStarted(workspace)
      apps: Seq[ListAppResponse] <- listApps(workspace, ctx.userInfo.accessToken.token).recoverWith {
        case e: ApiException if e.getCode == StatusCodes.NotFound.intValue =>
          Future.successful(Seq.empty)
      }
      _ <-
        if (apps.nonEmpty) {
          deleteApps(workspace, ctx)
        } else {
          workspaceRepository.setAppDeletionFinished(workspace)
        }
    } yield
    // TODO error handling
    Complete

  private def getWorkspaceDeletionState(
    workspace: Workspace
  )(implicit ec: ExecutionContext): Future[WorkspaceDeletionState] =
    for {
      deletion <- workspaceRepository.findDeletion(workspace)
    } yield deletion.getOrElse(throw new Exception("WHOOPS")).getState()
  private def pollAppDeletion(workspace: Workspace, userEmail: String)(implicit
    ec: ExecutionContext
  ): Future[JobStatus] =
    for {
      ctx <- getUserCtx(userEmail)
      apps: Seq[ListAppResponse] <- listApps(workspace, ctx.userInfo.accessToken.token).recover {
        case e: ApiException =>
          if (e.getCode == StatusCodes.NotFound.intValue) {
            Seq.empty
          } else {
            // TODO mark workspace delete failed
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError, "Unable to start workspace application deletion")
            )
          }
      }
      _ <-
        if (apps.isEmpty) {
          workspaceRepository.setAppDeletionFinished(workspace)
        } else { Future.successful() }
    } yield Complete

  private def startRuntimeDeletion(workspace: Workspace, userEmail: String)(implicit
    ec: ExecutionContext
  ): Future[JobStatus] =
    for {
      ctx <- getUserCtx(userEmail)
      _ <- workspaceRepository.setRuntimeDeletionStarted(workspace)
      runtimes: Seq[ListRuntimeResponse] <- listAzureRuntimes(workspace, ctx.userInfo.accessToken.token).recover {
        case e: ApiException =>
          if (e.getCode == StatusCodes.NotFound.intValue) {
            Seq.empty
          } else {
            // TODO mark workspace delete failed
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError, "Unable to start workspace runtime deletion")
            )
          }
      }
      _ <-
        if (runtimes.nonEmpty) {
          deleteRuntimes(workspace, ctx)
        } else {
          workspaceRepository.setRuntimeDeletionFinished(workspace)
        }
    } yield
    // TODO error handling
    Complete

  private def pollRuntimeDeletion(workspace: Workspace, userEmail: String)(implicit
    ec: ExecutionContext
  ): Future[JobStatus] =
    for {
      ctx <- getUserCtx(userEmail)
      runtimes: Seq[ListRuntimeResponse] <- listAzureRuntimes(workspace, ctx.userInfo.accessToken.token).recover {
        case e: ApiException =>
          if (e.getCode == StatusCodes.NotFound.intValue) {
            Seq.empty
          } else {
            // TODO mark workspace delete failed
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError, "Unable to get workspace runtime deletion status")
            )
          }
      }
      _ <-
        if (runtimes.isEmpty) {
          workspaceRepository.setRuntimeDeletionFinished(workspace)
        } else {
          Future.successful()
        }
    } yield Complete

  private def getWorkspaceFromWsm(workspaceId: UUID, ctx: RawlsRequestContext): Option[WorkspaceDescription] =
    Try(workspaceManagerDAO.getWorkspace(workspaceId, ctx)) match {
      case Success(w) => Some(w)
      case Failure(e: ApiException) if e.getCode == 404 =>
        logger.warn(s"Workspace not found in workspace manager for deletion [id=${workspaceId}]")
        None
      case Failure(e) =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.InternalServerError, e)
        )
    }
  private def startWsmDeletion(workspace: Workspace, jobId: UUID, userEmail: String)(implicit
    ec: ExecutionContext
  ): Future[JobStatus] =
    for {
      _ <- workspaceRepository.setWsmDeletionStarted(workspace)
      ctx <- getUserCtx(userEmail)
      existing <- Future {
        getWorkspaceFromWsm(workspace.workspaceIdAsUUID, ctx)
      }
      _ <-
        if (existing.isEmpty) {
          workspaceRepository.setWsmDeletionFinished(workspace)
        } else {
          Future(workspaceManagerDAO.deleteWorkspaceV2(workspace.workspaceIdAsUUID, jobId.toString, ctx))
        }
    } yield Complete

  private def pollWsmDeletion(workspace: Workspace, jobId: UUID, userEmail: String)(implicit ec: ExecutionContext) =
    for {
      ctx <- getUserCtx(userEmail)
      maybeStatus <- getWsmDeletionStatus(workspace, jobId, ctx)
      _ <- maybeStatus match {
        case Some(_) => workspaceRepository.setWsmDeletionFinished(workspace)
        case None    => workspaceRepository.setWsmDeletionFinished(workspace)
      }

    } yield Complete

  private def getWsmDeletionStatus(workspace: Workspace,
                                   jobId: UUID,
                                   ctx: RawlsRequestContext
  ): Future[Option[JobResult]] = {
    val result: JobResult = Try(
      workspaceManagerDAO.getDeleteWorkspaceV2Result(workspace.workspaceIdAsUUID, jobId.toString, ctx)
    ) match {
      case Success(w) => w
      case Failure(e: ApiException) =>
        if (e.getCode == StatusCodes.Forbidden.intValue) {
          // WSM will give back a 403 during polling if the workspace is not present or already deleted.
          logger.info(
            s"Workspace deletion result status = ${e.getCode} for workspace ID ${workspace.workspaceId}, WSM record is gone. Proceeding with rawls workspace deletion"
          )
          return Future.successful(None)
        } else {
          throw e
        }
      case Failure(e) => throw e
    }

    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(Some(result))
      case _ =>
        Future.failed(
          new Exception("WHOOPS")
        )
    }

  }

  private def finalizeWorkspaceDeletion(workspace: Workspace)(implicit
    ec: ExecutionContext
  ): Future[JobStatus] =
    for {
      _ <- workspaceRepository.deleteWorkspaceRecord(workspace)
      _ <- workspaceRepository.deleteWorkspaceDeletionStateRecord(workspace)
    } yield Complete

}
