package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, ErrorReport, RawlsBillingProject, RawlsBillingProjectName, SamBillingProjectActions, SamResourceAction, SamResourceTypeNames, SamWorkspaceActions, SlickWorkspaceContext, UserInfo, Workspace, WorkspaceAttributeSpecs, WorkspaceName, WorkspaceRequest}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent

import scala.concurrent.{ExecutionContext, Future}

trait WorkspaceSupport {
  val samDAO: SamDAO
  protected val userInfo: UserInfo
  implicit protected val executionContext: ExecutionContext
  protected val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  //Access/permission helpers

  def accessCheck(workspace: Workspace, requiredAction: SamResourceAction, ignoreLock: Boolean): Future[Unit] = {
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, requiredAction, userInfo) flatMap { hasRequiredLevel =>
      if (hasRequiredLevel) {
        if (Set(SamWorkspaceActions.write, SamWorkspaceActions.compute).contains(requiredAction) && workspace.isLocked && !ignoreLock)
          Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"The workspace ${workspace.toWorkspaceName} is locked.")))
        else
          Future.successful(())
      } else {
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, userInfo) flatMap { canRead =>
          if (canRead) {
            Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
          }
          else {
            Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
          }
        }
      }
    }
  }

  def requireAccessF[T](workspace: Workspace, requiredAction: SamResourceAction)(codeBlock: => Future[T]): Future[T] = {
    accessCheck(workspace, requiredAction, ignoreLock = false) flatMap { _ => codeBlock }
  }

  def requireAccessIgnoreLockF[T](workspace: Workspace, requiredAction: SamResourceAction)(codeBlock: => Future[T]): Future[T] = {
    accessCheck(workspace, requiredAction, ignoreLock = true) flatMap { _ => codeBlock }
  }

  def requireComputePermission(workspaceName: WorkspaceName): Future[Unit] = {
    for {
      workspaceContext <- getWorkspaceContext(workspaceName)
      hasCompute <- {
        samDAO.userHasAction(SamResourceTypeNames.billingProject, workspaceName.namespace, SamBillingProjectActions.launchBatchCompute, userInfo).flatMap { projectCanCompute =>
          if (!projectCanCompute) Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspaceName))))
          else {
            samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceContext.workspace.workspaceId, SamWorkspaceActions.compute, userInfo).flatMap { launchBatchCompute =>
              if (launchBatchCompute) Future.successful(())
              else samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceContext.workspace.workspaceId, SamWorkspaceActions.read, userInfo).flatMap { workspaceRead =>
                if (workspaceRead) Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspaceName))))
                else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
              }
            }
          }
        }
      }
    } yield {
      hasCompute
    }
  }

  // TODO: find and assess all usages. This is written to reside inside a DB transaction, but it makes a REST call to Sam.
  def requireCreateWorkspaceAccess[T](workspaceRequest: WorkspaceRequest, dataAccess: DataAccess, parentSpan: Span = null)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    for {
      userHasAction <- traceDBIOWithParent("userHasAction", parentSpan)(_ => DBIO.from(samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectActions.createWorkspace, userInfo)))
      response <- userHasAction match {
        case true =>
          traceDBIOWithParent("loadBillingProject", parentSpan)( _ => dataAccess.rawlsBillingProjectQuery.load(projectName)).flatMap {
            case Some(RawlsBillingProject(_, _, CreationStatuses.Ready, _, _, _, _, _)) => op //Sam will check to make sure the Auth Domain selection is valid
            case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, CreationStatuses.Creating, _, _, _, _, _)) =>
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"${name} is still being created")))

            case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, CreationStatuses.Error, _, messageOp, _, _, _)) =>
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Error creating ${name}: ${messageOp.getOrElse("no message")}")))
            case Some(_) | None =>
              // this can't happen with the current code but a 404 would be the correct response
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${workspaceRequest.toWorkspaceName.namespace} does not exist")))
          }
        case false =>
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}")))
      }
    } yield response
  }

  // can't use withClonedAuthDomain because the Auth Domain -> no Auth Domain logic is different
  def authDomainCheck(sourceWorkspaceADs: Set[String], destWorkspaceADs: Set[String]): ReadWriteAction[Boolean] = {
    // if the source has any auth domains, the dest must also *at least* have those auth domains
    if(sourceWorkspaceADs.subsetOf(destWorkspaceADs)) DBIO.successful(true)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg = s"Source workspace has an Authorization Domain containing the groups ${missingGroups.mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }
  }

  //WorkspaceContext helpers

  // function name may be misleading. This returns the workspace context and checks the user's permission,
  // but does not return the permissions.
  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName, requiredAction: SamResourceAction, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): Future[SlickWorkspaceContext] = {
    for {
      workspaceContext <- getWorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspaceContext.workspace, requiredAction, ignoreLock = false) // throws if user does not have permission
    } yield workspaceContext
  }

  def getWorkspaceContext(workspaceName: WorkspaceName, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): Future[SlickWorkspaceContext] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess, attributeSpecs) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess, attributeSpecs: Option[WorkspaceAttributeSpecs] = None)(op: (SlickWorkspaceContext) => ReadWriteAction[T]) = {
    dataAccess.workspaceQuery.findByName(workspaceName, attributeSpecs) flatMap {
      case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName)))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }

  def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"
}
