package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.{ErrorReport, WorkspaceAccessLevels, _}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.PerRequestMessage
import spray.http.StatusCodes

import scala.concurrent.Future

/**
  * Created by ahaessly on 3/31/17.
  */
trait LibraryPermissionSupport extends RoleSupport {
  final val publishedFlag = AttributeName("library","published")
  final val discoverableWSAttribute = AttributeName("library","discoverableByGroups")

  def withLibraryPermissions(workspace: Workspace,
                             operations: Seq[AttributeUpdateOperation],
                             dataSource: SlickDataSource,
                             userInfo: UserInfo,
                             gm: (RawlsUser, SlickWorkspaceContext, DataAccess) => ReadAction[WorkspaceAccessLevel])
                            (op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    val names = operations.map(op => op.name)
    val ctx = SlickWorkspaceContext(workspace)
    tryIsCurator(userInfo.userEmail) flatMap { isCurator =>
      dataSource.inTransaction { dataAccess =>
        for {
          canShare <- dataAccess.workspaceQuery.getUserSharePermissions(RawlsUserSubjectId(userInfo.userSubjectId), ctx)
          hasCatalogOnly <- dataAccess.workspaceQuery.getUserCatalogPermissions(RawlsUserSubjectId(userInfo.userSubjectId), ctx)
          userLevel <- gm(RawlsUser(userInfo), ctx, dataAccess)
        } yield {//(canShare, hasCatalogOnly, userLevel)
//          results map { case (canShare, hasCatalogOnly, userLevel) =>
          val functionToInvoke = getPermissionFunction(names, isCurator, canShare, hasCatalogOnly, userLevel)
            functionToInvoke(op)
        }
      } flatMap(identity)
    }
  }

  private def getPermissionFunction(names: Seq[AttributeName], isCurator: Boolean, canShare: Boolean, hasCatalogOnly: Boolean, userLevel:WorkspaceAccessLevel) = {
    val hasCatalog = hasCatalogOnly && userLevel >= WorkspaceAccessLevels.Read
    if (names.size == 1) {
      if (names.head == publishedFlag)
        withChangePublishedPermissions(isCurator, hasCatalog, userLevel) _
      else if (names.head == discoverableWSAttribute)
        withDiscoverabilityModifyPermissions(canShare, hasCatalog, userLevel) _
      else
        withModifyPermissions(hasCatalog, userLevel) _
    } else {
      assert(!names.contains(publishedFlag))
      if (names.contains(discoverableWSAttribute))
        withModifyAndDiscoverabilityModifyPermissions(canShare, hasCatalog, userLevel) _
      else
        withModifyPermissions(hasCatalog, WorkspaceAccessLevels.Write) _
    }
  }

  def withLibraryAttributeNamespaceCheck[T](attributeNames: Iterable[AttributeName])(op: => T): T = {
    val namespaces = attributeNames.map(_.namespace).toSet

    // only allow library namespace
    val invalidNamespaces = namespaces -- Set(AttributeName.libraryNamespace)
    if (invalidNamespaces.isEmpty) op
    else {
      val err = ErrorReport(statusCode = StatusCodes.Forbidden, message = s"All attributes must be in the library namespace")
      throw new RawlsExceptionWithErrorReport(errorReport = err)
    }
  }

  def withChangePublishedPermissions(isCurator: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    if (isCurator && (maxLevel >= WorkspaceAccessLevels.Owner || hasCatalog))
      op
    else
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must be a curator and either be an owner or have catalog with read+.")))
  }

  protected def canModify(hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel): Boolean = {
    maxLevel >= WorkspaceAccessLevels.Write || hasCatalog
  }

  def withModifyPermissions(hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    if (canModify(hasCatalog, maxLevel))
      op
    else
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must have write+ or catalog with read permissions.")))
  }

  protected def canModifyDiscoverability(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel): Boolean = {
    maxLevel >= WorkspaceAccessLevels.Owner || canShare || hasCatalog
  }

  def withDiscoverabilityModifyPermissions(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    if (canModifyDiscoverability(canShare, hasCatalog, maxLevel))
      op
    else
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must be an owner or have catalog or share permissions.")))
  }

  def withModifyAndDiscoverabilityModifyPermissions(canShare: Boolean, hasCatalog: Boolean, maxLevel: WorkspaceAccessLevels.WorkspaceAccessLevel)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    if (canModify(hasCatalog, maxLevel) && canModifyDiscoverability(canShare, hasCatalog, maxLevel))
      op
    else
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You must be an owner or have catalog with read permissions.")))
  }

}
