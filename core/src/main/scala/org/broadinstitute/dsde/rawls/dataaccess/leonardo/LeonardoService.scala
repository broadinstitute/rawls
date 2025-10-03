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
  CloudProvider,
  ClusterStatus,
  DiskStatus,
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse
}

import java.util.UUID
import javax.ws.rs.ProcessingException
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Wraps the leonardo DAO with retry logic and error handling
 * @param leonardoDAO Instance of a LeonardoDAO
 * @param system Instance of an ActorSystem
 */
class LeonardoService(leonardoDAO: LeonardoDAO)(implicit
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

  def when500OrProcessingException(throwable: Throwable): Boolean =
    throwable match {
      case t: LeoApiException     => t.getCode / 100 == 5
      case _: ProcessingException => true
      case _                      => false
    }

  // TODO: Refactor to use getAllApps
  def listNonErroredApps(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListAppResponse]] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          val allApps = leonardoDAO.listApps(ctx.userInfo.accessToken.token, workspace.googleProjectId)
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

  private def getAllDisks(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListPersistentDiskResponse]] =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          val allDisks = leonardoDAO.listDisks(ctx.userInfo.accessToken.token, workspace.googleProjectId);
          allDisks.filter { disk =>
            val cloudContext = disk.getCloudContext
            cloudContext != null &&
            cloudContext.getCloudResource == workspace.googleProjectId.value &&
            cloudContext.getCloudProvider == CloudProvider.GCP
          }
        }
      }
    }

  def listRunningDisks(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Seq[ListPersistentDiskResponse]] =
    getAllDisks(workspace, ctx).map { allDisks =>
      val statuses = Set(DiskStatus.CREATING, DiskStatus.READY, DiskStatus.RESTORING, DiskStatus.DELETING);
      allDisks.filter(disk => statuses.contains(disk.getStatus));
    }

  def hasActiveResources(workspace: Workspace, ctx: RawlsRequestContext)(implicit
    ec: ExecutionContext
  ): Future[Boolean] =
    for {
      runtimes <- listRunningRuntimes(workspace, ctx)
      apps <- listRunningApps(workspace, ctx)
      disks <- listRunningDisks(workspace, ctx)
    } yield runtimes.nonEmpty || apps.nonEmpty || disks.nonEmpty

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
