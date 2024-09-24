package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import cats.implicits.{catsSyntaxApplyOps, toFoldableOps}
import cats.ApplicativeThrow
import org.broadinstitute.dsde.rawls._
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  SamResourceAction,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamWorkspaceActions,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait WorkspaceSupport {
  val samDAO: SamDAO
  val workspaceRepository: WorkspaceRepository
  protected val ctx: RawlsRequestContext
  implicit protected val executionContext: ExecutionContext

  // Access/permission helpers
  private def userEnabledCheck: Future[Unit] =
    samDAO.getUserStatus(ctx) flatMap {
      case Some(user) if user.enabled => Future.successful()
      case _ => Future.failed(new UserDisabledException(StatusCodes.Unauthorized, "Unauthorized"))
    }

  def accessCheck(workspace: Workspace, requiredAction: SamResourceAction): Future[Unit] =
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, requiredAction, ctx) flatMap {
      hasRequiredLevel =>
        if (hasRequiredLevel) {
          Future.successful(())
        } else {
          samDAO.userHasAction(SamResourceTypeNames.workspace,
                               workspace.workspaceId,
                               SamWorkspaceActions.read,
                               ctx
          ) flatMap { canRead =>
            if (canRead) Future.failed(WorkspaceAccessDeniedException(workspace.toWorkspaceName))
            else Future.failed(NoSuchWorkspaceException(workspace.toWorkspaceName))
          }
        }
    }

  def accessCheck(workspaceId: String, requiredAction: SamResourceAction): Future[Unit] =
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, requiredAction, ctx) flatMap { hasRequiredLevel =>
      if (hasRequiredLevel) {
        Future.successful(())
      } else if (requiredAction == SamWorkspaceActions.read) {
        Future.failed(NoSuchWorkspaceException(workspaceId))
      } else {
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.read, ctx) flatMap {
          canRead =>
            if (canRead) Future.failed(WorkspaceAccessDeniedException(workspaceId))
            else Future.failed(NoSuchWorkspaceException(workspaceId))
        }
      }
    }

  def checkLock(workspace: Workspace, requiredAction: SamResourceAction): Future[Unit] = {
    val actionsBlockedByLock =
      Set(SamWorkspaceActions.write, SamWorkspaceActions.compute, SamWorkspaceActions.delete)
    if (actionsBlockedByLock.contains(requiredAction) && workspace.isLocked)
      Future.failed(LockedWorkspaceException(workspace.toWorkspaceName))
    else
      Future.successful(())
  }

  def requireComputePermission(workspaceName: WorkspaceName): Future[Unit] =
    for {
      _ <- userEnabledCheck
      workspace <- getWorkspaceContext(workspaceName)
      _ <- accessCheck(workspace, SamWorkspaceActions.compute)
    } yield ()

  // can't use withClonedAuthDomain because the Auth Domain -> no Auth Domain logic is different
  def authDomainCheck(sourceWorkspaceADs: Set[String], destWorkspaceADs: Set[String]): Boolean =
    // if the source has any auth domains, the dest must also *at least* have those auth domains
    if (sourceWorkspaceADs.subsetOf(destWorkspaceADs)) true
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg =
        s"Source workspace has an Authorization Domain containing the groups ${missingGroups.mkString(", ")}, which are missing on the destination workspace"
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg))
    }

  // WorkspaceContext helpers

  // function name may be misleading. This returns the workspace context and checks the user's permission,
  // but does not return the permissions.
  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName,
                                        requiredAction: SamResourceAction,
                                        attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    for {
      _ <- userEnabledCheck
      workspace <- getWorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspace, requiredAction)
      _ <- checkLock(workspace, requiredAction)
    } yield workspace

  def getWorkspaceContext(
    workspaceName: WorkspaceName,
    attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    workspaceRepository.getWorkspace(workspaceName, attributeSpecs).map {
      case Some(workspace) => workspace
      case None            => throw NoSuchWorkspaceException(workspaceName)
    }

  def getV2WorkspaceContextAndPermissions(
    workspaceName: WorkspaceName,
    requiredAction: SamResourceAction,
    attributeSpecs: Option[WorkspaceAttributeSpecs] = None,
    ignoreLock: Boolean = false
  ): Future[Workspace] =
    for {
      workspace <- getV2WorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspace, requiredAction)
      _ <- if (ignoreLock) Future.successful() else checkLock(workspace, requiredAction)
    } yield workspace

  def getV2WorkspaceContextAndPermissionsById(
    workspaceId: String,
    requiredAction: SamResourceAction,
    attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    for {
      workspace <- getV2WorkspaceContextByWorkspaceId(workspaceId, attributeSpecs)
      _ <- accessCheck(workspaceId, requiredAction)
      _ <- checkLock(workspace, requiredAction)
    } yield workspace

  def getV2WorkspaceContextByWorkspaceId(workspaceId: String,
                                         attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] = for {
    _ <- userEnabledCheck
    workspaceUuid = Try(UUID.fromString(workspaceId)) match {
      case Success(uid) => uid
      case Failure(_) =>
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "invalid UUID"))
    }
    workspaceContext <- workspaceRepository.getWorkspace(workspaceUuid, attributeSpecs)
  } yield workspaceContext match {
    case Some(workspace) => workspace
    case None            => throw NoSuchWorkspaceException(workspaceId)
  }

  def getV2WorkspaceContext(workspaceName: WorkspaceName,
                            attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] = for {
    _ <- userEnabledCheck
    workspaceContext <- workspaceRepository.getWorkspace(workspaceName, attributeSpecs)
  } yield workspaceContext match {
    case Some(workspace) => workspace
    case None            => throw NoSuchWorkspaceException(workspaceName)
  }

}
