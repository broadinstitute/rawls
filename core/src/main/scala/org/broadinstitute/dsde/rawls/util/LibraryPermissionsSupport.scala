package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.{ErrorReport, _}

import scala.collection.Set
import scala.concurrent.Future

/**
  * Created by ahaessly on 3/31/17.
  */
trait LibraryPermissionsSupport extends RoleSupport {
  val samDAO: SamDAO
  final val publishedFlag = AttributeName.withLibraryNS("published")
  final val discoverableWSAttribute = AttributeName.withLibraryNS("discoverableByGroups")

  def withLibraryPermissions(ctx: Workspace,
                             operations: Seq[AttributeUpdateOperation],
                             userInfo: UserInfo,
                             isCurator: Boolean
  )(op: => Future[Workspace]): Future[Workspace] = {
    val names = operations.map(attribute => attribute.name)

    getPermissionChecker(names, isCurator, ctx.workspaceId)(op)
  }

  def getPermissionChecker(names: Seq[AttributeName],
                           isCurator: Boolean,
                           workspaceId: String
  ): ((=> Future[Workspace]) => Future[Workspace]) =
    // need to combine multiple delete and add ops when changing discoverable attribute
    names.distinct match {
      case Seq(`publishedFlag`)           => changePublishedChecker(isCurator, workspaceId) _
      case Seq(`discoverableWSAttribute`) => changeDiscoverabilityChecker(workspaceId) _
      case x if x.contains(publishedFlag) =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, "Unsupported parameter - can't modify published with other attributes")
        )
      case x if x.contains(discoverableWSAttribute) => changeDiscoverabilityAndMetadataChecker(workspaceId) _
      case _                                        => changeMetadataChecker(workspaceId) _
    }

  private def maybeExecuteOp(canModify: Future[Boolean], cantModifyMessage: String, op: => Future[Workspace]) =
    canModify.flatMap {
      case true => op
      case false =>
        Future.failed(
          new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, cantModifyMessage))
        )
    }

  def hasAnyAction(workspaceId: String, actions: SamResourceAction*): Future[Boolean] =
    Future
      .traverse(actions) { action =>
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, action, ctx)
      }
      .map(_.contains(true))

  def canChangeMetadata(workspaceId: String): Future[Boolean] =
    hasAnyAction(workspaceId, SamWorkspaceActions.write, SamWorkspaceActions.catalog)

  def canChangeDiscoverability(workspaceId: String): Future[Boolean] =
    hasAnyAction(
      workspaceId,
      SamWorkspaceActions.own,
      SamWorkspaceActions.catalog,
      SamWorkspaceActions.sharePolicy(SamWorkspacePolicyNames.shareWriter.value),
      SamWorkspaceActions.sharePolicy(SamWorkspacePolicyNames.shareReader.value)
    )

  def canChangePublished(isCurator: Boolean, workspaceId: String): Future[Boolean] =
    if (!isCurator) {
      Future.successful(false)
    } else {
      hasAnyAction(workspaceId, SamWorkspaceActions.own, SamWorkspaceActions.catalog)
    }

  def changeMetadataChecker(workspaceId: String)(op: => Future[Workspace]): Future[Workspace] =
    maybeExecuteOp(canChangeMetadata(workspaceId), "You must have write+ or catalog with read permissions.", op)

  def changeDiscoverabilityChecker(workspaceId: String)(op: => Future[Workspace]): Future[Workspace] =
    maybeExecuteOp(canChangeDiscoverability(workspaceId),
                   "You must be an owner or have catalog or share permissions.",
                   op
    )

  def changePublishedChecker(isCurator: Boolean, workspaceId: String)(op: => Future[Workspace]): Future[Workspace] =
    maybeExecuteOp(canChangePublished(isCurator, workspaceId),
                   "You must be a curator and either be an owner or have catalog with read+.",
                   op
    )

  def changeDiscoverabilityAndMetadataChecker(workspaceId: String)(op: => Future[Workspace]): Future[Workspace] = {
    val canDo = Future
      .sequence(Seq(canChangeMetadata(workspaceId), canChangeDiscoverability(workspaceId)))
      .map(_.forall(identity))
    maybeExecuteOp(canDo, "You must be an owner or have catalog with read permissions.", op)
  }
}
