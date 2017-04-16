package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction}
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
      result <- getPermissionChecker(names, isCurator, canShare, hasCatalogOnly, userLevel).withPermissions(op)
    } yield result
  }

  def getPermissionChecker(names: Seq[AttributeName], isCurator: Boolean, canShare: Boolean, hasCatalogOnly: Boolean, userLevel: WorkspaceAccessLevel) = {
    val hasCatalog = hasCatalogOnly && userLevel >= WorkspaceAccessLevels.Read
    // need to multiple delete and add ops when changing discoverable attribute
    names.distinct match {
      case Seq(`publishedFlag`) => ChangePublishedChecker(isCurator, hasCatalog, userLevel)
      case Seq(`discoverableWSAttribute`) => ChangeDiscoverabilityChecker(canShare, hasCatalog, userLevel)
      case x if x.contains(publishedFlag) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Unsupported parameter - can't modify published with other attributes"))
      case x if x.contains(discoverableWSAttribute) => ChangeDiscoverabilityAndMetadataChecker(canShare, hasCatalog, userLevel)
      case _ => ChangeMetadataChecker(hasCatalog, userLevel)
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
}

trait LibraryPermissionChecker {
  def withPermissions(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace]
}

case class ChangeMetadataChecker(hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel) extends LibraryPermissionChecker {
  def canModify: Boolean = {
    maxLevel >= WorkspaceAccessLevels.Write || hasCatalog
  }
  def withPermissions(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] = {
    if (canModify)
      op
    else
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must have write+ or catalog with read permissions."))
  }
}

case class ChangeDiscoverabilityChecker(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel) extends LibraryPermissionChecker {
  def canModify: Boolean = {
    maxLevel >= WorkspaceAccessLevels.Owner || canShare || hasCatalog
  }
  def withPermissions(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] = {
    if (canModify)
      op
    else
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must be an owner or have catalog or share permissions."))
  }
}

case class ChangePublishedChecker(isCurator: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel) extends LibraryPermissionChecker {
  def withPermissions(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] = {
    if (isCurator && (maxLevel >= WorkspaceAccessLevels.Owner || hasCatalog))
      op
    else
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must be a curator and either be an owner or have catalog with read+."))
  }
}

case class ChangeDiscoverabilityAndMetadataChecker(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel) extends LibraryPermissionChecker {
  def withPermissions(op: => ReadWriteAction[Workspace]): ReadWriteAction[Workspace] = {
    if (ChangeMetadataChecker(hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel).canModify &&
      ChangeDiscoverabilityChecker(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel).canModify)
      op
    else
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must be an owner or have catalog with read permissions."))
  }
}
