package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls._
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  SamResourceAction,
  SamResourceTypeNames,
  SamWorkspaceActions,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.broadinstitute.dsde.workbench.client.sam.ApiException

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait WorkspaceSupport {
  val samDAO: SamDAO
  val workspaceRepository: WorkspaceRepository
  protected val ctx: RawlsRequestContext
  implicit protected val executionContext: ExecutionContext

  // Access/permission helpers
  def accessCheck(workspace: Workspace, requiredAction: SamResourceAction): Future[Unit] =
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, requiredAction, ctx) flatMap {
      hasRequiredLevel =>
        if (hasRequiredLevel) {
          Future.successful(())
        } else {
          // If this access check is for any action other than read, check if the user has read
          // so we know what exception to throw. If this access check is for read, we already
          // know the answer
          val canReadFuture = if (requiredAction == SamWorkspaceActions.read) {
            Future(hasRequiredLevel)
          } else {
            samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx)
          }
          canReadFuture flatMap { canRead =>
            if (canRead) Future.failed(WorkspaceAccessDeniedException(workspace.toWorkspaceName))
            else Future.failed(NoSuchWorkspaceException(workspace.toWorkspaceName))
          }
        }
    } recoverWith {
      // samDAO.userHasAction will throw ApiExceptions in cases where the user is disabled or missing;
      // handle those here.
      case apiException: ApiException
          if apiException.getCode == StatusCodes.Unauthorized.intValue
            && apiException.getMessage.contains("Message: User is disabled.") =>
        Future.failed(new UserDisabledException(StatusCodes.Unauthorized, "Unauthorized"))
      case apiException: ApiException if apiException.getCode == StatusCodes.Forbidden.intValue =>
        // David An 2025-07-11: throwing UserDisabledException here preserves existing behavior
        // given the changes in https://github.com/broadinstitute/rawls/pull/3401. However,
        // we may want to throw a different exception at some point; returning UserDisabledException
        // when the user is forbidden or does not exist is not semantically correct.
        Future.failed(new UserDisabledException(StatusCodes.Unauthorized, "Unauthorized"))
    }

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

  def getV2WorkspaceContextAndPermissions(
    workspaceName: WorkspaceName,
    requiredAction: SamResourceAction,
    attributeSpecs: Option[WorkspaceAttributeSpecs] = None,
    ignoreLock: Boolean = false
  ): Future[Workspace] =
    for {
      // Does the workspace exist?
      maybeWorkspace <- workspaceRepository.getWorkspace(workspaceName, attributeSpecs)
      workspace <- checkWorkspace(maybeWorkspace, workspaceName.toString)
      // Does the user have the required permissions?
      _ <- accessCheck(workspace, requiredAction)
      // Is the workspace locked, and is the action blocked by the lock?
      _ <- if (ignoreLock) Future.successful() else checkLock(workspace, requiredAction)
    } yield workspace

  def getV2WorkspaceContextAndPermissionsById(
    workspaceId: String,
    requiredAction: SamResourceAction,
    attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    for {
      // Validate input UUID
      maybeUuid <- Future(Try(UUID.fromString(workspaceId)))
      workspaceUuid = maybeUuid match {
        case Success(uid) => uid
        case Failure(_) =>
          throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "invalid UUID"))
      }
      // Does the workspace exist?
      maybeWorkspace <- workspaceRepository.getWorkspace(workspaceUuid, attributeSpecs)
      workspace <- checkWorkspace(maybeWorkspace, workspaceUuid.toString)
      // Does the user have the required permissions?
      _ <- accessCheck(workspaceId, requiredAction)
      // Is the workspace locked, and is the action blocked by the lock?
      _ <- checkLock(workspace, requiredAction)
    } yield workspace

  def getV2WorkspaceContext(workspaceName: WorkspaceName,
                            attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] = for {
    _ <- userEnabledCheck
    workspaceContext <- workspaceRepository.getWorkspace(workspaceName, attributeSpecs)
  } yield workspaceContext match {
    case Some(workspace) => workspace
    case None            => throw NoSuchWorkspaceException(workspaceName)
  }

  // private internal methods

  private def accessCheck(workspaceId: String, requiredAction: SamResourceAction): Future[Unit] =
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

  private def checkLock(workspace: Workspace, requiredAction: SamResourceAction): Future[Unit] = {
    val actionsBlockedByLock =
      Set(SamWorkspaceActions.write, SamWorkspaceActions.compute, SamWorkspaceActions.delete)
    if (actionsBlockedByLock.contains(requiredAction) && workspace.isLocked)
      Future.failed(LockedWorkspaceException(workspace.toWorkspaceName))
    else
      Future.successful(())
  }

  private def checkWorkspace(maybeWorkspace: Option[Workspace], errorIdentifier: String): Future[Workspace] =
    maybeWorkspace match {
      case Some(workspace) => Future(workspace)
      case None            =>
        // The workspace does not exist. Check if the current user is enabled;
        // throw UserDisabledException if not, otherwise throw NoSuchWorkspaceException.
        userEnabledCheck map (_ => throw NoSuchWorkspaceException(errorIdentifier))
    }

  private def userEnabledCheck: Future[Unit] =
    samDAO.getUserStatus(ctx) flatMap {
      case Some(user) if user.enabled => Future.successful()
      case _ => Future.failed(new UserDisabledException(StatusCodes.Unauthorized, "Unauthorized"))
    }

}
