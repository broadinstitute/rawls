package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.JobResult
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500
import org.broadinstitute.dsde.rawls.util.Retry
import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class WsmDeletionAction(workspaceManagerDao: WorkspaceManagerDAO, timeout: FiniteDuration)(implicit
  val system: ActorSystem
) extends Retry
    with LazyLogging {

  def isComplete(workspace: Workspace,
                 job: WorkspaceManagerResourceMonitorRecord,
                 ctx: RawlsRequestContext
  ): Future[Boolean] =
    isComplete(workspace, job.jobControlId.toString, new DateTime(job.createdTime), DateTime.now, ctx)

  def isComplete(workspace: Workspace,
                 jobId: String,
                 createdTime: DateTime,
                 checkTime: DateTime,
                 ctx: RawlsRequestContext
  ): Future[Boolean] = {
    logger.info(
      s"Checking WSM deletion status for workspace [workspaceId=${workspace.workspaceId}, jobControlId=${jobId}, createdTime=${createdTime}]"
    )
    if (checkTime.isAfter(createdTime.plus(timeout.toMillis))) {
      return Future.failed(
        new WorkspaceDeletionActionTimeoutException(
          s"Job timed out [workspaceId=${workspace.workspaceId}, jobControlId=${jobId}, createdTime=${createdTime}]"
        )
      )
    }

    val result: JobResult = Try(
      workspaceManagerDao.getDeleteWorkspaceV2Result(workspace.workspaceIdAsUUID, jobId, ctx)
    ) match {
      case Success(w) => w
      case Failure(e: ApiException) =>
        if (e.getCode == StatusCodes.Forbidden.intValue || e.getCode == StatusCodes.NotFound.intValue) {
          // WSM will give back a 403 during polling if the workspace is not present or already deleted.
          logger.info(
            s"Workspace deletion result status = ${e.getCode} for workspace ID ${workspace.workspaceId}, WSM record is gone. Proceeding with rawls workspace deletion"
          )
          return Future.successful(true)
        } else {
          return Future.failed(e)
        }
      case Failure(e) => return Future.failed(e)
    }

    result.getJobReport.getStatus match {
      case StatusEnum.SUCCEEDED => Future.successful(true)
      case StatusEnum.RUNNING   => Future.successful(false)
      case StatusEnum.FAILED =>
        Future.failed(
          new WorkspaceDeletionActionFailureException(
            s"Polling workspace deletion for status to be ${StatusEnum.SUCCEEDED}. Current status: ${result.getJobReport.getStatus} [workspaceId=${workspace.workspaceId}, jobControlId = ${jobId}] "
          )
        )
    }
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
