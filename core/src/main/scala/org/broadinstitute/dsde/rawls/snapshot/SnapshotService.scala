package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.client.ApiException
import bio.terra.datarepo.model.SnapshotModel
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.TpsModel.TpsPolicies
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  SamWorkspaceActions,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.policy.{PolicyService, PolicyUtilities}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceService}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object SnapshotService {
  def constructor(workspaceRepository: WorkspaceRepository,
                  samDAO: SamDAO,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  terraDataRepoUrl: String,
                  dataRepoDAO: DataRepoDAO,
                  workspaceServiceConstructor: RawlsRequestContext => WorkspaceService,
                  policyService: PolicyService
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): SnapshotService =
    new SnapshotService(
      ctx,
      workspaceRepository,
      samDAO,
      workspaceManagerDAO,
      terraDataRepoUrl,
      dataRepoDAO,
      workspaceServiceConstructor,
      policyService
    )
}

class SnapshotService(protected val ctx: RawlsRequestContext,
                      val workspaceRepository: WorkspaceRepository,
                      val samDAO: SamDAO,
                      workspaceManagerDAO: WorkspaceManagerDAO,
                      terraDataRepoInstanceName: String,
                      dataRepoDAO: DataRepoDAO,
                      workspaceServiceConstructor: RawlsRequestContext => WorkspaceService,
                      policyService: PolicyService
)(implicit protected val executionContext: ExecutionContext)
    extends FutureSupport
    with WorkspaceSupport
    with LazyLogging {

  // Finds a workspace using the workspaceId then calls the createSnapshot method
  def createSnapshotsByWorkspaceIdV3(workspaceId: String, snapshotIds: Set[UUID]): Future[Unit] =
    getV2WorkspaceContextAndPermissionsById(workspaceId,
                                            SamWorkspaceActions.write,
                                            Some(WorkspaceAttributeSpecs(all = false))
    ).flatMap { rawlsWorkspace =>
      createSnapshots(rawlsWorkspace, snapshotIds)
    }

  // Find a workspace using the workspaceName then calls the createSnapshot method
  def createSnapshotsByWorkspaceNameV3(workspaceName: WorkspaceName, snapshotIds: Set[UUID]): Future[Unit] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ).flatMap(rawlsWorkspace => createSnapshots(rawlsWorkspace, snapshotIds))

  // Link the snapshot pao to the workspace pao
  private def createSnapshots(rawlsWorkspace: Workspace, snapshotIds: Set[UUID]): Future[Unit] =
    for {
      snapshotsFromDataRepo <- Future {
        snapshotIds.map(getSnapshotFromDataRepoWithId)
      }
      workspacePaoOpt <- policyService.getPao(rawlsWorkspace.workspaceIdAsUUID, ctx)
      workspacePao = workspacePaoOpt.getOrElse(
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound,
                      s"Workspace PAO not found for workspace ${rawlsWorkspace.workspaceIdAsUUID}"
          )
        )
      )

      // filter out any snapshots that are already linked to the workspace
      unlinkedSnapshots = snapshotsFromDataRepo.filterNot(snapshot =>
        workspacePao.getSourcesObjectIds.asScala.toSet.contains(snapshot.getId)
      )

      snapshotPaos <- Future.traverse(unlinkedSnapshots) { snapshot =>
        policyService.getOrCreateSnapshotPao(snapshot.getId, ctx)
      }

      _ <- Future.traverse(unlinkedSnapshots) { snapshot =>
        policyService.linkSnapshotPaoToWorkspacePao(snapshot.getId,
                                                    rawlsWorkspace.workspaceIdAsUUID,
                                                    dryRun = true,
                                                    ctx
        )
      }

      // if any snapshots contain protected data, the workspace must be protected
      _ = if (
        snapshotPaos.exists(
          PolicyUtilities.containsPolicy(_, TpsPolicies.ProtectedData)
        ) && !workspaceServiceConstructor(ctx).isBucketSecure(rawlsWorkspace)
      ) {
        throw new ProtectedDataException("Unable to add protected snapshot to unprotected workspace.")
      }

      // Region constraints can cause conflicts when combining PAOs. We check that no individual
      // snapshot PAO will conflict with the workspace PAO above, but there's no easy way to check
      // that all of the snapshot PAOs will combine together cleanly when they're all linked to
      // the same workspace. Snapshots shouldn't have region constraint policies, but throw here
      // just in case.
      _ = if (snapshotPaos.exists(PolicyUtilities.containsPolicy(_, TpsPolicies.RegionConstraint))) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, "Unable to add snapshot with region constraint to workspace.")
        )
      }

      snapshotGroups = snapshotPaos.flatMap { snapshotPao =>
        PolicyUtilities.getGroupConstraintGroups(snapshotPao)
      }
      newWorkspaceGroups = snapshotGroups -- PolicyUtilities.getGroupConstraintGroups(workspacePao)
      _ <-
        if (newWorkspaceGroups.nonEmpty) {
          workspaceServiceConstructor(ctx).addAuthDomainGroups(rawlsWorkspace.toWorkspaceName, newWorkspaceGroups, ctx)
        } else Future.unit

      _ <- Future.traverse(unlinkedSnapshots) { snapshot =>
        policyService.linkSnapshotPaoToWorkspacePao(snapshot.getId,
                                                    rawlsWorkspace.workspaceIdAsUUID,
                                                    dryRun = false,
                                                    ctx
        )
      }
    } yield ()

  private def getSnapshotFromDataRepoWithId(snapshotId: UUID): SnapshotModel =
    Try(dataRepoDAO.getSnapshot(snapshotId, ctx.userInfo.accessToken)) match {
      case Success(snapshot) => snapshot
      // if snapshot not found in TDR, this is a bad request
      case Failure(ex: ApiException) if ex.getCode == StatusCodes.NotFound.intValue =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Snapshot ${snapshotId} not found.")
        )
      // on some other TDR API exception, strip the stack trace and propagate
      case Failure(ex: ApiException) =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(ex.getCode, ex.getMessage)
        )
      // else, propagate by wrapping in an error report
      case Failure(other) =>
        logger.warn(s"Unexpected error when retrieving snapshot: ${other.getMessage}", other)
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, other.getMessage))
    }

}
