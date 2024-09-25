package org.broadinstitute.dsde.rawls.workspace

import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.NoSuchWorkspaceException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeValue,
  ErrorReportSource,
  ManagedGroupRef,
  RawlsGroupName,
  RawlsRequestContext,
  SamResourceTypeName,
  SamResourceTypeNames,
  Workspace,
  WorkspaceAdminResponse,
  WorkspaceAttributeSpecs,
  WorkspaceDetails,
  WorkspaceFeatureFlag,
  WorkspaceName
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
  def listAllWorkspaces(): Future[Seq[WorkspaceDetails]] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listAll().map(workspaces => workspaces.map(w => WorkspaceDetails(w, Set.empty)))
      }
    }

  // Admin endpoint, not limited to V2 workspaces
  def adminListWorkspacesWithAttribute(attributeName: AttributeName,
                                       attributeValue: AttributeValue
  ): Future[Seq[WorkspaceDetails]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.listWithAttribute(attributeName, attributeValue)
        }
        results <- Future.traverse(workspaces) { workspace =>
          loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId).map(
            WorkspaceDetails(workspace, _)
          )
        }
      } yield results
    }

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

  def getWorkspaceById(workspaceId: UUID): Future[WorkspaceAdminResponse] = asFCAdmin {
    for {
      workspaceOpt <- workspaceRepository.getWorkspace(workspaceId)
      workspace = workspaceOpt.getOrElse(throw NoSuchWorkspaceException(workspaceId.toString))
      settings <- workspaceSettingRepository.getWorkspaceSettings(workspaceId)
    } yield WorkspaceAdminResponse(WorkspaceDetails.fromWorkspaceAndOptions(workspace, None, useAttributes = false),
                                   settings
    )
  }

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

  private def loadResourceAuthDomain(resourceTypeName: SamResourceTypeName,
                                     resourceId: String
  ): Future[Set[ManagedGroupRef]] =
    samDAO
      .getResourceAuthDomain(resourceTypeName, resourceId, ctx)
      .map(_.map(g => ManagedGroupRef(RawlsGroupName(g))).toSet)

}
