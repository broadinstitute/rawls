package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.{ErrorReport, WorkspaceAccessLevels, _}
import spray.http.StatusCodes

import scala.collection.Set

/**
  * Created by ahaessly on 3/31/17.
  */
trait LibraryPermissionsSupport extends RoleSupport {
  final val publishedFlag = AttributeName.withLibraryNS("published")
  final val discoverableWSAttribute = AttributeName.withLibraryNS("discoverableByGroups")

  def withLibraryPermissions(ctx: SlickWorkspaceContext,
                             operations: Seq[AttributeUpdateOperation],
                             dataAccess: DataAccess,
                             userInfo: UserInfo,
                             isCurator: Boolean,
                             userLevel: WorkspaceAccessLevel)
                            (op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] = {
    val names = operations.map(attribute => attribute.name)
    for {
      canShare <- dataAccess.workspaceQuery.getUserSharePermissions(RawlsUserSubjectId(userInfo.userSubjectId), ctx)
      hasCatalogOnly <- dataAccess.workspaceQuery.getUserCatalogPermissions(RawlsUserSubjectId(userInfo.userSubjectId), ctx)
      result <- getPermissionChecker(names, isCurator, canShare, hasCatalogOnly, userLevel)(op)
    } yield result
  }

  def getPermissionChecker(names: Seq[AttributeName], isCurator: Boolean, canShare: Boolean, hasCatalogOnly: Boolean, userLevel: WorkspaceAccessLevel): ((=> ReadWriteAction[Workspace]) => ReadWriteAction[Workspace]) = {
    val hasCatalog = hasCatalogOnly && userLevel >= WorkspaceAccessLevels.Read
    // need to combine multiple delete and add ops when changing discoverable attribute
    names.distinct match {
      case Seq(`publishedFlag`) => changePublishedChecker(isCurator, hasCatalog, userLevel) _
      case Seq(`discoverableWSAttribute`) => changeDiscoverabilityChecker(canShare, hasCatalog, userLevel) _
      case x if x.contains(publishedFlag) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Unsupported parameter - can't modify published with other attributes"))
      case x if x.contains(discoverableWSAttribute) => changeDiscoverabilityAndMetadataChecker(canShare, hasCatalog, userLevel) _
      case _ => changeMetadataChecker(hasCatalog, userLevel) _
    }
  }

  def withLibraryAttributeNamespaceCheck[T](attributeNames: Iterable[AttributeName])(op: => T): T = {
    val namespaces = attributeNames.map(_.namespace).toSet

    // only allow library namespace
    val invalidNamespaces = namespaces -- Set(AttributeName.libraryNamespace)
    if (invalidNamespaces.isEmpty) op
    else {
      val err = ErrorReport(statusCode = StatusCodes.BadRequest, message = s"All attributes must be in the library namespace")
      throw new RawlsExceptionWithErrorReport(errorReport = err)
    }
  }

  private def maybeExecuteOp(canModify: Boolean, cantModifyMessage: String, op: => ReadWriteAction[Workspace]) = {
    if (canModify) op
    else throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, cantModifyMessage))
  }

  def canChangeMetadata(hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel): Boolean =
    maxLevel >= WorkspaceAccessLevels.Write || hasCatalog
  def canChangeDiscoverability(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel): Boolean =
    maxLevel >= WorkspaceAccessLevels.Owner || canShare || hasCatalog
  def canChangePublished(isCurator: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel): Boolean =
    isCurator && (maxLevel >= WorkspaceAccessLevels.Owner || hasCatalog)


  def changeMetadataChecker(hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] =
    maybeExecuteOp(canChangeMetadata(hasCatalog, maxLevel), "You must have write+ or catalog with read permissions.", op)

  def changeDiscoverabilityChecker(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] =
    maybeExecuteOp(canChangeDiscoverability(canShare, hasCatalog, maxLevel), "You must be an owner or have catalog or share permissions.", op)

  def changePublishedChecker(isCurator: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] =
    maybeExecuteOp(canChangePublished(isCurator, hasCatalog, maxLevel), "You must be a curator and either be an owner or have catalog with read+.", op)

  def changeDiscoverabilityAndMetadataChecker(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] = {
    val canDo = canChangeMetadata(hasCatalog, maxLevel) && canChangeDiscoverability(canShare, hasCatalog, maxLevel)
    maybeExecuteOp(canDo, "You must be an owner or have catalog with read permissions.", op)
  }
}

