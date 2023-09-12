package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{blocking, ExecutionContext, Future}

class LeonardoResourceDeletionAction(leonardoDAO: LeonardoDAO, pollInterval: FiniteDuration, timeout: FiniteDuration)(
  implicit val system: ActorSystem
) extends Retry
    with LazyLogging {

  def pollOperation[T](workspace: Workspace,
                       ctx: RawlsRequestContext,
                       checker: (Workspace, RawlsRequestContext) => Future[Seq[T]]
  )(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    for {
      result <- retryUntilSuccessOrTimeout(pred = jobStatusPredicate)(pollInterval, timeout) { () =>
        isComplete(workspace, ctx, checker)
      }
    } yield result match {
      case Left(_) =>
        throw new LeonardoOperationFailureException(
          s"Leonardo resource deletion failed [workspaceId=${workspace.workspaceId}]",
          workspace.workspaceIdAsUUID
        )
      case Right(_) => ()
    }

  def isComplete[T](workspace: Workspace,
                    ctx: RawlsRequestContext,
                    checker: (Workspace, RawlsRequestContext) => Future[Seq[T]]
  )(implicit ec: ExecutionContext): Future[Unit] = {
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
      if (resources.nonEmpty) {
        Future.failed(
          new LeonardoPollingException(s"Leo resources still present [workspaceId=${workspace.workspaceId}]")
        )
      } else {
        Future.successful()
      }
    }
  }

  def jobStatusPredicate(t: Throwable): Boolean =
    t match {
      case _: LeonardoPollingException => true
      case _                           => false
    }

  def pollRuntimeDeletion(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Unit] =
    pollOperation[ListRuntimeResponse](workspace, ctx, listAzureRuntimes)

  def pollAppDeletion(workspace: Workspace, ctx: RawlsRequestContext)(implicit ec: ExecutionContext): Future[Unit] =
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

  def when500(throwable: Throwable): Boolean =
    throwable match {
      case t: ApiException => t.getCode / 100 == 5
      case _               => false
    }
}

class LeonardoPollingException(message: String) extends WorkspaceDeletionActionFailureException(message)
class LeonardoOperationFailureException(message: String, val workspaceId: UUID)
    extends WorkspaceDeletionActionFailureException(message)
