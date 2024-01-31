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
      logger.info("PollOperation (success): Got ApiException when polling Leonardo resources", t)
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
    case Failure(t) =>
      logger.info("PollOperation (failure): Got exception when polling Leonardo resources", t)
      Future.failed(t)
    case Success(resources) if resources.nonEmpty =>
      logger.info(
        s"PollOperation (failure): Found non-empty resources when polling Leonardo resources [workspaceId=${workspace.workspaceId}]",
        resources
      )
      Future.successful(false) // It's OK if they are nonEmpty as long as the status of all is ERROR.
    case Success(_) =>
      logger.info(
        s"PollOperation (success): Found empty resources when polling Leonardo resources [workspaceId=${workspace.workspaceId}]"
      )
      Future.successful(true)
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
          logger.info(
            s"Filtering out ${allApps.size - nonErroredApps.size} errored apps for [workspaceId=${workspace.workspaceIdAsUUID}]"
          )
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
          logger.info(
            s"Filtering out ${allRuntimes.size - nonErroredRuntimes.size} errored runtimes for [workspaceId=${workspace.workspaceIdAsUUID}]"
          )
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
