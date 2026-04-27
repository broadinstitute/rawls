package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  ErrorReportSource,
  GoogleProjectId,
  ManagedGroupRef,
  RawlsGroupName,
  RawlsRequestContext,
  SamResourceTypeAdminActions,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamWorkspacePolicyNames,
  Workspace,
  WorkspaceAdminResponse,
  WorkspaceAttributeSpecs,
  WorkspaceDetails,
  WorkspaceFeatureFlag,
  WorkspaceName,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.util._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object WorkspaceAdminService {
  def constructor(dataSource: SlickDataSource,
                  gcsDAO: GoogleServicesDAO,
                  samDAO: SamDAO,
                  workbenchMetricBaseName: String
  )(
    ctx: RawlsRequestContext
  )(implicit materializer: Materializer, executionContext: ExecutionContext): WorkspaceAdminService =
    new WorkspaceAdminService(
      ctx,
      dataSource,
      gcsDAO,
      samDAO,
      workbenchMetricBaseName,
      new WorkspaceRepository(dataSource),
      new WorkspaceSettingRepository(dataSource)
    )
}

class WorkspaceAdminService(
  protected val ctx: RawlsRequestContext,
  val dataSource: SlickDataSource,
  protected val gcsDAO: GoogleServicesDAO,
  val samDAO: SamDAO,
  override val workbenchMetricBaseName: String,
  val workspaceRepository: WorkspaceRepository,
  val workspaceSettingRepository: WorkspaceSettingRepository
)(implicit protected val executionContext: ExecutionContext)
    extends LazyLogging
    with RawlsInstrumented
    with RoleSupport
    with WorkspaceSupport {

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  // Admin endpoint, not limited to V2 workspaces
  def adminListWorkspaceFeatureFlags(workspaceName: WorkspaceName): Future[Seq[WorkspaceFeatureFlag]] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceFeatureFlagQuery.listAllForWorkspace(workspaceContext.workspaceIdAsUUID)
        }
      }
    }

  // Admin endpoint, not limited to V2 workspaces
  def adminOverwriteWorkspaceFeatureFlags(workspaceName: WorkspaceName,
                                          flagNames: List[String]
  ): Future[Seq[WorkspaceFeatureFlag]] =
    asFCAdmin {
      val flags = flagNames.map(WorkspaceFeatureFlag)

      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          for {
            _ <- dataAccess.workspaceFeatureFlagQuery.deleteAllForWorkspace(workspaceContext.workspaceIdAsUUID)
            _ <- dataAccess.workspaceFeatureFlagQuery.saveAll(workspaceContext.workspaceIdAsUUID, flags)
          } yield flags
        }
      }
    }

  def getWorkspaceById(workspaceId: UUID): Future[WorkspaceAdminResponse] =
    for {
      userIsAdmin <- samDAO.admin
        .userHasResourceTypeAdminPermission(SamResourceTypeNames.workspace,
                                            SamResourceTypeAdminActions.readSummaryInformation,
                                            ctx
        )
      _ = if (!userIsAdmin)
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.Forbidden, "You must be an admin to call this API.")
        )
      workspaceOpt <- workspaceRepository.getWorkspace(workspaceId)
      workspace = workspaceOpt.getOrElse(throw NoSuchWorkspaceException(workspaceId.toString))
      authDomains <- samDAO.admin.adminGetResourceAuthDomain(SamResourceTypeNames.workspace, workspaceId.toString, ctx)
      settings <- workspaceSettingRepository.getWorkspaceSettings(workspaceId)
    } yield WorkspaceAdminResponse(
      WorkspaceDetails.fromWorkspaceAndOptions(workspace,
                                               Some(authDomains.map(n => ManagedGroupRef(RawlsGroupName(n))).toSet),
                                               useAttributes = false
      ),
      settings
    )

  /**
   * Admin endpoint to delete a workspace of type MC
   */
  def adminDeleteMcWorkspace(workspaceName: WorkspaceName): Future[Unit] =
    asFCAdmin {
      for {
        // Get workspace to verify it's an MC workspace and to get its ID
        workspaceOpt <- workspaceRepository.getWorkspace(workspaceName)
        workspace = workspaceOpt.getOrElse(throw NoSuchWorkspaceException(workspaceName))
        _ = if (workspace.workspaceType != WorkspaceType.McWorkspace) {
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, s"Workspace ${workspaceName} is not an MC workspace")
          )
        }

        // Add the current caller to the workspace owner policy to ensure they have sufficient permissions
        _ <- samDAO.admin
          .addUserToPolicy(
            SamResourceTypeNames.workspace,
            workspace.workspaceId,
            SamWorkspacePolicyNames.owner,
            ctx.userInfo.userEmail.value,
            ctx
          )
          .recover {
            case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode.contains(StatusCodes.NotFound) => ()
          }

        _ <- recursivelyDeleteSamResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
        _ <- workspaceRepository.deleteMcWorkspaceDbEntries(workspace)
      } yield ()
    }

  /**
   * Recursively delete a SAM resource and all its children
   * @param resourceTypeName the resource type
   * @param resourceId the resource ID
   * @param ctx the request context
   * @return Future[Unit]
   */
  private[workspace] def recursivelyDeleteSamResource(resourceTypeName: SamResourceTypeName,
                                                      resourceId: String,
                                                      ctx: RawlsRequestContext
  ): Future[Unit] = samDAO.recursiveDeleteResource(resourceTypeName, resourceId, ctx)(executionContext, logger)

  def getWorkspaceId(workspaceName: WorkspaceName): Future[Option[String]] =
    for {
      userIsAdmin <- samDAO.admin
        .userHasResourceTypeAdminPermission(SamResourceTypeNames.workspace,
                                            SamResourceTypeAdminActions.readSummaryInformation,
                                            ctx
        )
      _ = if (!userIsAdmin)
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.Forbidden, "You must be an admin to call this API.")
        )
      workspaceOpt <- workspaceRepository.getWorkspaceId(workspaceName)
    } yield workspaceOpt.map(_.toString)

  def getWorkspaceByGoogleProjectId(googleProjectId: GoogleProjectId): Future[WorkspaceAdminResponse] =
    for {
      userIsAdmin <- samDAO.admin
        .userHasResourceTypeAdminPermission(SamResourceTypeNames.workspace,
                                            SamResourceTypeAdminActions.readSummaryInformation,
                                            ctx
        )
      _ = if (!userIsAdmin)
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.Forbidden, "You must be an admin to call this API.")
        )
      workspaceOpt <- workspaceRepository.getWorkspaceByGoogleProject(googleProjectId)
      workspace = workspaceOpt.getOrElse(throw NoSuchWorkspaceException(googleProjectId.toString))
      authDomains <- samDAO.admin.adminGetResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
      settings <- workspaceSettingRepository.getWorkspaceSettings(workspace.workspaceIdAsUUID)
    } yield WorkspaceAdminResponse(
      WorkspaceDetails.fromWorkspaceAndOptions(workspace,
                                               Some(authDomains.map(n => ManagedGroupRef(RawlsGroupName(n))).toSet),
                                               useAttributes = false
      ),
      settings
    )

  // moved out of WorkspaceSupport because the only usage was in this file,
  // and it has raw datasource/dataAccess usage, which is being refactored out of WorkspaceSupport
  private def withWorkspaceContext[T](workspaceName: WorkspaceName,
                                      dataAccess: DataAccess,
                                      attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  )(op: Workspace => ReadWriteAction[T]) =
    dataAccess.workspaceQuery.findByName(workspaceName, attributeSpecs) flatMap {
      case None            => throw NoSuchWorkspaceException(workspaceName)
      case Some(workspace) => op(workspace)
    }
}
