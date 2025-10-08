package org.broadinstitute.dsde.rawls.dataaccess.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.workbench.client.leonardo.{ApiException, ApiException => LeoApiException}
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppStatus,
  ClusterStatus,
  ListAppResponse,
  ListRuntimeResponse
}

import java.util.UUID
import javax.ws.rs.ProcessingException
import scala.concurrent.{blocking, ExecutionContext, Future}

/**
 * Wraps the leonardo DAO with retry logic and error handling
 * @param leonardoDAO Instance of a LeonardoDAO
 * @param system Instance of an ActorSystem
 */
class LeonardoService(leonardoDAO: LeonardoDAO)(implicit
  val system: ActorSystem
) extends Retry
    with LazyLogging {

  def when500OrProcessingException(throwable: Throwable): Boolean =
    throwable match {
      case t: LeoApiException     => t.getCode / 100 == 5
      case _: ProcessingException => true
      case _                      => false
    }

  private def getAllApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListAppResponse]] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          leonardoDAO.listApps(ctx.userInfo.accessToken.token, workspace.googleProjectId)
        }
      }
    }

  def listRunningApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListAppResponse]] =
    getAllApps(workspace, ctx).map { allApps =>
      val statuses = Set(AppStatus.RUNNING, AppStatus.PROVISIONING, AppStatus.STARTING, AppStatus.DELETING);
      allApps.filter(app => statuses.contains(app.getStatus));
    }

  private def getAllRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListRuntimeResponse]] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          leonardoDAO.listRuntimes(ctx.userInfo.accessToken.token, workspace.googleProjectId)
        }
      }
    }

  def listRunningRuntimes(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListRuntimeResponse]] =
    getAllRuntimes(workspace, ctx).map { allRuntimes =>
      val statuses = Set(ClusterStatus.RUNNING,
                         ClusterStatus.STARTING,
                         ClusterStatus.STOPPING,
                         ClusterStatus.CREATING,
                         ClusterStatus.UPDATING,
                         ClusterStatus.DELETING
      );
      allRuntimes.filter(runtime => statuses.contains(runtime.getStatus));
    }

  // ** Check if a workspace has any active cloud environments.
  def hasActiveResources(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Boolean] =
    for {
      runtimes <- listRunningRuntimes(workspace, ctx)
      apps <- listRunningApps(workspace, ctx)
    } yield runtimes.nonEmpty || apps.nonEmpty

  /**
   * Notifies leonardo that it should delete any resource records related to the given google project ID *without*
   * deleting the actual cloud resources.
   *
   * NB: This should only be called after a workspace's google project has been deleted and we want to ensure there
   * are no further dangling references from Leo to resources within that google project.
   *
   * @param googleProjectId ID of the workspace's google project
   * @param workspaceId ID of the workspace
   * @param ctx RawlsRequestContext containing auth info
   */
  def cleanupResources(googleProjectId: GoogleProjectId, workspaceId: UUID, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Unit] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          logger.info(
            s"Sending resource cleanup request to Leonardo [workspaceId=$workspaceId, googleProjectId=${googleProjectId.value}]"
          )
          leonardoDAO.cleanupAllResources(ctx.userInfo.accessToken.token, googleProjectId)
        }
      }.recoverWith { case t: ApiException =>
        if (t.getCode != StatusCodes.NotFound.intValue) {
          logger.warn(
            s"Unexpected failure cleaning up leonardo workspace resources for workspaceId=$workspaceId . Received ${t.getCode}: [${t.getResponseBody}]"
          )
          Future.failed(t)
        } else {
          Future.successful()
        }
      }
    }

}
