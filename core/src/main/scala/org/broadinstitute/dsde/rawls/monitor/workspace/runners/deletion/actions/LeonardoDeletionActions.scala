package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

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
  ): Future[Boolean] =
    pollOperation[ListRuntimeResponse](workspace, ctx, listAzureRuntimes)

  def pollAppDeletion(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Boolean] =
    pollOperation[ListAppResponse](workspace, ctx, listApps)

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

class LeonardoPollingException(message: String) extends WorkspaceDeletionActionFailureException(message)
class LeonardoOperationFailureException(message: String, val workspaceId: UUID)
    extends WorkspaceDeletionActionFailureException(message)
