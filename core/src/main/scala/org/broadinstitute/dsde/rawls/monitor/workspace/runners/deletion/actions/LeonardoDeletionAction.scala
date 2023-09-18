package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}
import org.joda.time.DateTime

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{blocking, ExecutionContext, Future}

class LeonardoResourceDeletionAction(leonardoDAO: LeonardoDAO, timeout: FiniteDuration)(implicit
  val system: ActorSystem
) extends Retry
    with LazyLogging {

  def isComplete[T](workspace: Workspace,
                    jobStartedAt: DateTime,
                    checkTime: DateTime,
                    ctx: RawlsRequestContext,
                    checker: (Workspace, RawlsRequestContext) => Future[Seq[T]]
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    if (checkTime.isAfter(jobStartedAt.plus(timeout.toMillis))) {
      return Future.failed(new WorkspaceDeletionActionTimeoutException("Job timed out"))
    }

    val result: Future[Seq[T]] =
      checker(workspace, ctx).recoverWith {
        case t: ApiException =>
          if (t.getCode == StatusCodes.Forbidden.intValue) {
            // leo gives back a 403 when the workspace is gone
            logger.warn(s"403 when fetching leo resources, continuing [workspaceId=${workspace.workspaceId}]")
            Future.successful(Seq.empty)
          } else if (t.getCode == StatusCodes.NotFound.intValue) {
            logger.warn(s"404 when fetching leo resources, continuing [workspaceId=${workspace.workspaceId}]")
            Future.successful(Seq.empty)
          } else {
            Future.failed(t)
          }
        case t: Throwable => Future.failed(t)
      }

    result.flatMap { resources =>
      logger.info(s"Number of resources present = ${resources.length} [workspaceId=${workspace.workspaceId}]")
      Future.successful(resources.isEmpty)
    }
  }

  def pollRuntimeDeletion(workspace: Workspace, job: WorkspaceManagerResourceMonitorRecord, ctx: RawlsRequestContext)(
    implicit ec: ExecutionContext
  ): Future[Boolean] = {
    logger.info(
      s"Checking state of leo runtime deletion [workspaceId=${workspace.workspaceId}, jobControlId=${job.jobControlId}, createdTime=${job.createdTime}]"
    )
    isComplete(workspace, new DateTime(job.createdTime), DateTime.now(), ctx, listAzureRuntimes)
  }

  def pollAppDeletion(workspace: Workspace, job: WorkspaceManagerResourceMonitorRecord, ctx: RawlsRequestContext)(
    implicit ec: ExecutionContext
  ): Future[Boolean] = {
    logger.info(
      s"Checking state of leo app deletion [workspaceId=${workspace.workspaceId}, jobControlId=${job.jobControlId}, createdTime=${job.createdTime}]"
    )
    isComplete(workspace, new DateTime(job.createdTime), DateTime.now(), ctx, listApps)
  }

  def listApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListAppResponse]] =
    retry(when500) { () =>
      Future {
        blocking {
          leonardoDAO.listApps(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID)
        }
      }
    }

  def listAzureRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListRuntimeResponse]] =
    retry(when500) { () =>
      Future {
        blocking {
          leonardoDAO.listAzureRuntimes(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID)
        }
      }
    }

  def deleteApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Unit] =
    retry(when500) { () =>
      Future {
        blocking {
          logger.info(s"Sending app deletion request [workspaceId=${workspace.workspaceIdAsUUID}]")
          leonardoDAO.deleteApps(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID, deleteDisk = true)
        }
      }
    }

  def deleteRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Unit] =
    retry(when500) { () =>
      Future {
        blocking {
          logger.info(s"Sending runtime deletion request [workspaceId=${workspace.workspaceIdAsUUID}]")
          leonardoDAO.deleteAzureRuntimes(ctx.userInfo.accessToken.token,
                                          workspace.workspaceIdAsUUID,
                                          deleteDisk = true
          )
        }
      }
    }

}

class LeonardoOperationFailureException(message: String, val workspaceId: UUID)
    extends WorkspaceDeletionActionFailureException(message)
