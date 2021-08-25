package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, ErrorReport, RawlsBillingProject, RawlsBillingProjectName, SamBillingProjectActions, SamBillingProjectRoles, SamResourceAction, SamResourceTypeNames, SamWorkspaceActions, UserInfo, Workspace, WorkspaceAttributeSpecs, WorkspaceName, WorkspaceRequest}
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
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceContext.workspaceId, SamWorkspaceActions.compute, userInfo).flatMap { launchBatchCompute =>
          if (launchBatchCompute) Future.successful(())
          else samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceContext.workspaceId, SamWorkspaceActions.read, userInfo).flatMap { workspaceRead =>
            if (workspaceRead) Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspaceName))))
            else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
          }
        }
      }
    } yield {
      hasCompute
    }
  }

  // TODO: find and assess all usages. This is written to reside inside a DB transaction, but it makes a REST call to Sam.
  // Will process op only if User has the `createWorkspace` action on the specified Billing Project, otherwise will
  // Fail with 403 Forbidden
  def requireCreateWorkspaceAccess[T](workspaceRequest: WorkspaceRequest, dataAccess: DataAccess, parentSpan: Span = null)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    for {
      userHasAction <- traceDBIOWithParent("userHasAction", parentSpan)(_ => DBIO.from(samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectActions.createWorkspace, userInfo)))
      response <- userHasAction match {
        case true => op
        case false => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}")))
      }
    } yield response
  }

  // Creating a Workspace without an Owner policy is allowed only if the requesting User has the `owner` role
  // granted on the Workspace's Billing Project
  def maybeRequireBillingProjectOwnerAccess[T](workspaceRequest: WorkspaceRequest, parentSpan: Span = null)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    workspaceRequest.noWorkspaceOwner match {
      case Some(true) =>
        for {
          billingProjectRoles <- traceDBIOWithParent("listUserRolesForResource", parentSpan)(_ => DBIO.from(samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, workspaceRequest.namespace, userInfo)))
          userIsBillingProjectOwner = billingProjectRoles.contains(SamBillingProjectRoles.owner)
          response <- userIsBillingProjectOwner match {
            case true => op
            case false => DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, s"Missing ${SamBillingProjectRoles.owner} role on billing project '${workspaceRequest.namespace}'.")))
          }
        } yield response
      case _ =>
        op
    }

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
  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName, requiredAction: SamResourceAction, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): Future[Workspace] = {
    for {
      workspaceContext <- getWorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspaceContext, requiredAction, ignoreLock = false) // throws if user does not have permission
    } yield workspaceContext
  }

  def getWorkspaceContext(workspaceName: WorkspaceName, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): Future[Workspace] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess, attributeSpecs) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess, attributeSpecs: Option[WorkspaceAttributeSpecs] = None)(op: (Workspace) => ReadWriteAction[T]) = {
    dataAccess.workspaceQuery.findByName(workspaceName, attributeSpecs) flatMap {
      case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName)))
      case Some(workspace) => op(workspace)
    }
  }

  def withWorkspaceBucketRegionCheck[T](bucketRegion: Option[String])(op: => Future[T]): Future[T] = {
    bucketRegion match {
      case Some(region) =>
        // if the user specifies a region for the workspace bucket, it must be in the proper format for a single region or the default bucket location (US multi region)
        val singleRegionPattern = "[A-Za-z]+-[A-Za-z]+[0-9]+"
        val defaultBucketRegion = "US"
        if (region.matches(singleRegionPattern) || region.equals(defaultBucketRegion)) op
        else {
          val err = ErrorReport(statusCode = StatusCodes.BadRequest, message = s"Workspace bucket location must be a single " +
            s"region of format: $singleRegionPattern or the default bucket location ('US').")
          throw new RawlsExceptionWithErrorReport(errorReport = err)
        }
      case None => op
    }
  }

  def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist or you do not have permission to use it"
  def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"
}
