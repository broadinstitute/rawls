package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500OrProcessingException
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppStatus,
  ClusterStatus,
  ListAppResponse,
  ListRuntimeResponse
}

import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.{Failure, Success}

class LeonardoResourceDeletionAction(leonardoDAO: LeonardoDAO)(implicit
  val system: ActorSystem
) extends Retry
    with LazyLogging {

  def pollOperation[T](workspace: Workspace,
                       ctx: RawlsRequestContext,
                       checker: (Workspace, RawlsRequestContext) => Future[Seq[T]]
  )(implicit
    ec: ExecutionContext
  ): Future[Boolean] = checker(workspace, ctx).transformWith {
    case Failure(t: ApiException) =>
      if (t.getCode == StatusCodes.Forbidden.intValue) {
        // leo gives back a 403 when the workspace is gone
        logger.warn(s"403 when fetching leo resources, continuing [workspaceId=${workspace.workspaceId}]")
        Future.successful(true)
      } else if (t.getCode == StatusCodes.NotFound.intValue) {
        logger.warn(s"404 when fetching leo resources, continuing [workspaceId=${workspace.workspaceId}]")
        Future.successful(true)
      } else {
        Future.failed(t)
      }
    case Failure(t)                               => Future.failed(t)
    case Success(resources) if resources.nonEmpty => Future.successful(false)
    case Success(_)                               => Future.successful(true)
  }

  def pollRuntimeDeletion(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    logger.info(s"Polling runtime deletion [workspaceId=${workspace.workspaceId}]")
    pollOperation[ListRuntimeResponse](workspace, ctx, listNonErroredAzureRuntimes)
  }

  def pollAppDeletion(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    logger.info(s"Polling app deletion [workspaceId=${workspace.workspaceId}]")
    pollOperation[ListAppResponse](workspace, ctx, listNonErroredApps)
  }

  def listNonErroredApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListAppResponse]] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          val allApps = leonardoDAO.listApps(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID)
          val nonErroredApps = allApps.filter(_.getStatus != AppStatus.ERROR)
          val erroredAppCount = allApps.size - nonErroredApps.size
          if (erroredAppCount > 0) {
            logger.info(
              s"Filtering out ${erroredAppCount} errored apps for [workspaceId=${workspace.workspaceIdAsUUID}]"
            )
          }
          nonErroredApps
        }
      }
    }

  def listNonErroredAzureRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListRuntimeResponse]] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          val allRuntimes = leonardoDAO.listAzureRuntimes(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID)
          val nonErroredRuntimes = allRuntimes.filter(_.getStatus != ClusterStatus.ERROR)
          val erroredRuntimeCount = allRuntimes.size - nonErroredRuntimes.size
          if (erroredRuntimeCount > 0) {
            logger.info(
              s"Filtering out ${erroredRuntimeCount} errored runtimes for [workspaceId=${workspace.workspaceIdAsUUID}]"
            )
          }
          nonErroredRuntimes
        }
      }
    }

  def deleteApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Unit] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          logger.info(s"Sending app deletion request [workspaceId=${workspace.workspaceIdAsUUID}]")
          leonardoDAO.deleteApps(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID, deleteDisk = true)
        }
      }
    }

  def deleteRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Unit] =
    retry(when500OrProcessingException) { () =>
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
