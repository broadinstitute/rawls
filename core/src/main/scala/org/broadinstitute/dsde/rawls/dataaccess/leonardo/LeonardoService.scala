package org.broadinstitute.dsde.rawls.dataaccess.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.DeletionAction.when500OrProcessingException
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppStatus,
  ClusterStatus,
  ListAppResponse,
  ListRuntimeResponse,
  UpdateAppRequest,
  UpdateDiskRequest,
  UpdateRuntimeRequest
}
import spray.json.enrichAny

import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}
import spray.json._
import DefaultJsonProtocol._
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

  def updateRuntimeLabels(workspaceId: UUID, workspaceNamespace: String, ctx: RawlsRequestContext): Unit = {
    logger.info(s"Upserting workspace namespace label $workspaceNamespace for workspaceId=$workspaceId runtimes")
    val updateRequest = new UpdateRuntimeRequest()
    updateRequest.setLabelsToUpsert(Map("saturnWorkspaceNamespace" -> workspaceNamespace))
    val runtimes = leonardoDAO.listRuntimesByWorkspace(ctx.userInfo.accessToken.token, workspaceId)
    runtimes.foreach { runtime =>
      leonardoDAO.updateRuntimeConfig(ctx.userInfo.accessToken.token,
                                      runtime.getGoogleProject,
                                      runtime.getRuntimeName,
                                      updateRequest
      )
    }
  }

  def updateDiskLabels(workspaceId: UUID,
                       oldWorkspaceNamespace: String,
                       newWorkspaceNamespace: String,
                       ctx: RawlsRequestContext
  ): Unit = {
    logger.info(s"Upserting workspace namespace label $newWorkspaceNamespace for [workspaceId=${workspaceId}] disks")
    val label = Map("labels" -> Map("saturnWorkspaceNamespace" -> newWorkspaceNamespace))
    val updateRequest = UpdateDiskRequest.fromJson(label.toJson.compactPrint)

    val disks = leonardoDAO.listDisksByWorkspaceNamespace(ctx.userInfo.accessToken.token, oldWorkspaceNamespace)
    disks.foreach { disk =>
      leonardoDAO.updateDiskConfig(ctx.userInfo.accessToken.token,
                                   disk.getCloudContext.getCloudResource,
                                   disk.getName,
                                   updateRequest
      )
    }
  }

//   def updateAppLabels(workspace: Workspace, ctx: RawlsRequestContext): Unit = {
//    logger.info(s"Upserting workspace namespace label for [workspaceId=${workspace.workspaceIdAsUUID}]")
//    val updateRequest = new UpdateAppRequest()
//    updateRequest.setLabelsToUpsert(Map("saturnWorkspaceNamespace" -> workspace.namespace))
//    val apps = leonardoDAO.listApps(ctx.userInfo.accessToken.token, workspace.workspaceIdAsUUID)
//    apps.foreach { app =>
//      leonardoDAO.updateAppConfig(ctx.userInfo.accessToken.token,
//        app.getCloudContext.getCloudResource,
//        app.getAppName,
//        updateRequest
//      )
//    }

  def updateResourceLabelsOrRevert(workspaceId: UUID,
                                   oldWorkspaceNamespace: String,
                                   newWorkspaceNamespace: String,
                                   ctx: RawlsRequestContext
  )(implicit
    ec: ExecutionContext
  ): Unit =
    retry(when500OrProcessingException) { () =>
      Future {
        blocking {
          updateRuntimeLabels(workspaceId, newWorkspaceNamespace, ctx)
          updateDiskLabels(workspaceId, oldWorkspaceNamespace, newWorkspaceNamespace, ctx)
          // updateAppLabels(workspaceId, ctx) // Uncomment when app workspaceNamespace label updates are possible
        }
      }.recoverWith { case t: ApiException =>
        logger.warn(
          s"Unexpected failure updating resource labels for workspaceId=$workspaceId. Received ${t.getCode}: [${t.getResponseBody}]"
        )
        try {
          // If we fail to update the labels, revert the changes
          updateRuntimeLabels(workspaceId, oldWorkspaceNamespace, ctx)
          updateDiskLabels(workspaceId, newWorkspaceNamespace, oldWorkspaceNamespace, ctx)
        } catch {
          case revertException: Exception =>
            logger.error(
              s"Failed to revert workspace namespace label updates for workspaceId=$workspaceId. Error: ${revertException.getMessage}"
            )
        }
        Future.failed(t)
      }
    }

}
