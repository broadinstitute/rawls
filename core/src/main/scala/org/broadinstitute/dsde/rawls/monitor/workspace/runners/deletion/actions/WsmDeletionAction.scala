package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class WsmDeletionAction(workspaceManagerDao: WorkspaceManagerDAO)(implicit val system: ActorSystem)
    extends Retry
    with LazyLogging {

  def pollForCompletion(workspace: Workspace, jobId: String, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Boolean] =
    retry(when500)(() => Future(isComplete(workspace, jobId, ctx)))

  private def isComplete(workspace: Workspace, jobId: String, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Boolean = Try(workspaceManagerDao.getDeleteWorkspaceV2Result(workspace.workspaceIdAsUUID, jobId, ctx)) match {
    case Success(result) =>
      Option(result.getJobReport).map(report => report.getStatus) match {
        case Some(StatusEnum.SUCCEEDED) => true
        case Some(StatusEnum.RUNNING)   => false
        case Some(StatusEnum.FAILED) =>
          throw WorkspaceDeletionActionFailureException(
            s"Workspace deletion returned status ${StatusEnum.FAILED} [workspaceId=${workspace.workspaceId}, jobControlId = ${jobId}]"
          )
        case None =>
          val message = Option(result.getErrorReport)
            .map { errorReport =>
              errorReport.getMessage
            }
            .getOrElse("No errors reported")
          throw WorkspaceDeletionActionFailureException(
            s"Workspace Deletion failed for [workspaceId=${workspace.workspaceId}, jobControlId = $jobId]: $message"
          )
      }
    // WSM will give back a 403 during polling if the workspace is not present or already deleted.
    case Failure(e: ApiException) if e.getCode == StatusCodes.Forbidden.intValue =>
      logger.info(
        s"Workspace deletion result status = ${e.getCode} for workspace ID ${workspace.workspaceId}, WSM record is gone. Proceeding with rawls workspace deletion"
      )
      true
    case Failure(e) => throw e
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
