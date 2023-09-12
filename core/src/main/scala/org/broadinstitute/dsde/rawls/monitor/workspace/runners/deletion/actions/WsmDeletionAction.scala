package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.JobResult
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.workspace.WorkspaceManagerOperationFailureException

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

class WsmDeletionAction(workspaceManagerDao: WorkspaceManagerDAO,
                        pollInterval: FiniteDuration,
                        timeout: FiniteDuration
)(implicit
  val system: ActorSystem
) extends Retry
    with LazyLogging {

  private def jobStatusPredicate(t: Throwable): Boolean =
    t match {
      case t: WorkspaceManagerPollingOperationException => t.status == StatusEnum.RUNNING
      case _                                            => false
    }

  def isComplete(workspace: Workspace, jobId: String, ctx: RawlsRequestContext): Future[Boolean] = {
    logger.info(
      s"Checking for WSM deletion for workspace ${workspace.workspaceIdAsUUID} [pollInterval = ${pollInterval}, timeout = ${timeout}]"
    )
    val result: JobResult = Try(
      workspaceManagerDao.getDeleteWorkspaceV2Result(workspace.workspaceIdAsUUID, jobId, ctx)
    ) match {
      case Success(w) => w
      case Failure(e: ApiException) =>
        if (e.getCode == StatusCodes.Forbidden.intValue) {
          // WSM will give back a 403 during polling if the workspace is not present or already deleted.
          logger.info(
            s"Workspace deletion result status = ${e.getCode} for workspace ID ${workspace.workspaceId}, WSM record is gone. Proceeding with rawls workspace deletion"
          )
          return Future.successful(false)
        } else {
          return Future.failed(e)
        }
      case Failure(e) => throw e
    }

    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(true)
      case _ =>
        Future.failed(
          new WorkspaceManagerPollingOperationException(
            s"Polling workspace deletion for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus} [workspaceId=${workspace.workspaceId}, jobControlId = ${jobId}] ",
            result.getJobReport.getStatus
          )
        )
    }
  }

  def pollOperation(workspace: Workspace, jobId: String, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    for {
      result <- retryUntilSuccessOrTimeout(pred = jobStatusPredicate)(pollInterval, timeout) { () =>
        isComplete(workspace, jobId, ctx)
      }
    } yield result match {
      case Left(_) =>
        throw new WorkspaceManagerOperationFailureException(
          s"Failed deleting workspace in WSM [workspaceId = ${workspace.workspaceId}, jobControlid=${jobId}]",
          workspace.workspaceIdAsUUID,
          jobId
        )
      case Right(_) => ()
    }

  def startStep(workspace: Workspace, jobId: String, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    retry(when500) { () =>
      startDeletion(workspace, jobId, ctx)
    }

  private def startDeletion(workspace: Workspace, jobControlId: String, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    Future {
      blocking {
        workspaceManagerDao.deleteWorkspaceV2(workspace.workspaceIdAsUUID, jobControlId, ctx)
      }
    }.recoverWith {
      case t: ApiException =>
        if (t.getCode == StatusCodes.NotFound.intValue) {
          logger.warn(
            s"404 when starting WSM workspace deletion, continuing [workspaceId=${workspace.workspaceId}, jobControlId=${jobControlId}]"
          )
          Future.successful()
        } else {
          Future.failed(t)
        }
      case t: Throwable => Future.failed(t)

    }.flatMap(_ => Future.successful())
}

class WorkspaceManagerPollingOperationException(message: String, val status: StatusEnum) extends Exception(message)
