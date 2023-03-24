package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import cats.implicits.{catsSyntaxApply, toFoldableOps}
import cats.{Applicative, ApplicativeThrow}
import org.broadinstitute.dsde.rawls._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamBillingProjectRoles,
  SamResourceAction,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamWorkspaceActions,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{traceDBIOWithParent, traceWithParent}

import scala.concurrent.{ExecutionContext, Future}

trait WorkspaceSupport {
  val samDAO: SamDAO
  protected val ctx: RawlsRequestContext
  implicit protected val executionContext: ExecutionContext
  protected val dataSource: SlickDataSource

  import dataSource.dataAccess.driver.api._

  // Access/permission helpers
  private def userEnabledCheck: Future[Unit] =
    samDAO.getUserStatus(ctx) flatMap {
      case Some(user) if user.enabled => Future.successful()
      case _ => Future.failed(new UserDisabledException(StatusCodes.Unauthorized, "Unauthorized"))
    }

  def accessCheck(workspace: Workspace, requiredAction: SamResourceAction, ignoreLock: Boolean): Future[Unit] =
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, requiredAction, ctx) flatMap {
      hasRequiredLevel =>
        if (hasRequiredLevel) {
          val actionsBlockedByLock =
            Set(SamWorkspaceActions.write, SamWorkspaceActions.compute, SamWorkspaceActions.delete)
          if (actionsBlockedByLock.contains(requiredAction) && workspace.isLocked && !ignoreLock)
            Future.failed(LockedWorkspaceException(workspace.toWorkspaceName))
          else
            Future.successful(())
        } else {
          samDAO.userHasAction(SamResourceTypeNames.workspace,
                               workspace.workspaceId,
                               SamWorkspaceActions.read,
                               ctx
          ) flatMap { canRead =>
            if (canRead) {
              Future.failed(WorkspaceAccessDeniedException(workspace.toWorkspaceName))
            } else {
              Future.failed(NoSuchWorkspaceException(workspace.toWorkspaceName))
            }
          }
        }
    }

  def requireAccessF[T](workspace: Workspace, requiredAction: SamResourceAction)(codeBlock: => Future[T]): Future[T] =
    accessCheck(workspace, requiredAction, ignoreLock = false) flatMap { _ => codeBlock }

  def requireAccessIgnoreLockF[T](workspace: Workspace, requiredAction: SamResourceAction)(
    codeBlock: => Future[T]
  ): Future[T] =
    accessCheck(workspace, requiredAction, ignoreLock = true) flatMap { _ => codeBlock }

  def requireComputePermission(workspaceName: WorkspaceName): Future[Unit] =
    getWorkspaceContext(workspaceName).flatMap { workspace =>
      def require(action: SamResourceAction, mkThrowable: WorkspaceName => Throwable) =
        raiseUnlessUserHasAction(action, SamResourceTypeNames.workspace, workspace.workspaceId, ctx) {
          mkThrowable(workspaceName)
        }

      require(SamWorkspaceActions.compute, WorkspaceAccessDeniedException.apply).recoverWith { case t: Throwable =>
        // verify the user has `read` on the workspace to avoid exposing its existence
        require(SamWorkspaceActions.read, NoSuchWorkspaceException.apply) *> Future.failed(t)
      }
    }

  def requireCreateWorkspaceAction(project: RawlsBillingProjectName, context: RawlsRequestContext = ctx): Future[Unit] =
    raiseUnlessUserHasAction(SamBillingProjectActions.createWorkspace,
                             SamResourceTypeNames.billingProject,
                             project.value,
                             context
    ) {
      RawlsExceptionWithErrorReport(
        ErrorReport(
          StatusCodes.Forbidden,
          s"You are not authorized to create a workspace in billing project $project"
        )
      )
    }

  def raiseUnlessUserHasAction(action: SamResourceAction,
                               resType: SamResourceTypeName,
                               resId: String,
                               context: RawlsRequestContext = ctx
  )(
    throwable: Throwable
  ): Future[Unit] =
    samDAO
      .userHasAction(resType, resId, action, context)
      .flatMap(ApplicativeThrow[Future].raiseUnless(_)(throwable))

  // Creating a Workspace without an Owner policy is allowed only if the requesting User has the `owner` role
  // granted on the Workspace's Billing Project
  def requireBillingProjectOwnerAccess(projectName: RawlsBillingProjectName,
                                       parentContext: RawlsRequestContext
  ): Future[Unit] =
    for {
      billingProjectRoles <- traceWithParent("listUserRolesForResource", parentContext)(context =>
        samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, context)
      )
      _ <- ApplicativeThrow[Future].raiseUnless(billingProjectRoles.contains(SamBillingProjectRoles.owner)) {
        RawlsExceptionWithErrorReport(
          ErrorReport(
            StatusCodes.Forbidden,
            s"Missing ${SamBillingProjectRoles.owner} role on billing project '$projectName'."
          )
        )
      }
    } yield ()

  // can't use withClonedAuthDomain because the Auth Domain -> no Auth Domain logic is different
  def authDomainCheck(sourceWorkspaceADs: Set[String], destWorkspaceADs: Set[String]): ReadWriteAction[Boolean] =
    // if the source has any auth domains, the dest must also *at least* have those auth domains
    if (sourceWorkspaceADs.subsetOf(destWorkspaceADs)) DBIO.successful(true)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg =
        s"Source workspace has an Authorization Domain containing the groups ${missingGroups.mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }

  // WorkspaceContext helpers

  // function name may be misleading. This returns the workspace context and checks the user's permission,
  // but does not return the permissions.
  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName,
                                        requiredAction: SamResourceAction,
                                        attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    for {
      workspaceContext <- getWorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspaceContext, requiredAction, ignoreLock = false) // throws if user does not have permission
    } yield workspaceContext

  def getWorkspaceContext(workspaceName: WorkspaceName,
                          attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    userEnabledCheck.flatMap { _ =>
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess, attributeSpecs)(DBIO.successful)
      }
    }

  def withWorkspaceContext[T](workspaceName: WorkspaceName,
                              dataAccess: DataAccess,
                              attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  )(op: (Workspace) => ReadWriteAction[T]) =
    dataAccess.workspaceQuery.findByName(workspaceName, attributeSpecs) flatMap {
      case None            => throw NoSuchWorkspaceException(workspaceName)
      case Some(workspace) => op(workspace)
    }

  def getV2WorkspaceContextAndPermissions(workspaceName: WorkspaceName,
                                          requiredAction: SamResourceAction,
                                          attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    for {
      workspaceContext <- getV2WorkspaceContext(workspaceName, attributeSpecs)
      _ <- accessCheck(workspaceContext, requiredAction, ignoreLock = false) // throws if user does not have permission
    } yield workspaceContext

  def getV2WorkspaceContext(workspaceName: WorkspaceName,
                            attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  ): Future[Workspace] =
    userEnabledCheck.flatMap { _ =>
      dataSource.inTransaction { dataAccess =>
        withV2WorkspaceContext(workspaceName, dataAccess, attributeSpecs)(DBIO.successful)
      }
    }

  def withV2WorkspaceContext[T](workspaceName: WorkspaceName,
                                dataAccess: DataAccess,
                                attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  )(op: (Workspace) => ReadWriteAction[T]) =
    dataAccess.workspaceQuery.findV2WorkspaceByName(workspaceName, attributeSpecs) flatMap {
      case None            => throw NoSuchWorkspaceException(workspaceName)
      case Some(workspace) => op(workspace)
    }

  def failIfBucketRegionInvalid(bucketRegion: Option[String]): Future[Unit] =
    bucketRegion.traverse_ { region =>
      // if the user specifies a region for the workspace bucket, it must be in the proper format
      // for a single region or the default bucket location (US multi region)
      val singleRegionPattern = "[A-Za-z]+-[A-Za-z]+[0-9]+"
      val validUSPattern = "US"
      ApplicativeThrow[Future].raiseUnless(region.matches(singleRegionPattern) || region.equals(validUSPattern)) {
        RawlsExceptionWithErrorReport(
          ErrorReport(
            StatusCodes.BadRequest,
            s"Workspace bucket location must be a single " +
              s"region of format: $singleRegionPattern or the default bucket location ('US')."
          )
        )
      }
    }

  /**
    * Load the specified billing project, throwing if the billing project is not ready.
    */
  def getBillingProjectContext(projectName: RawlsBillingProjectName,
                               context: RawlsRequestContext = ctx
  ): Future[RawlsBillingProject] =
    for {
      maybeBillingProject <- dataSource.inTransaction { dataAccess =>
        traceDBIOWithParent("loadBillingProject", context) { _ =>
          dataAccess.rawlsBillingProjectQuery.load(projectName)
        }
      }

      billingProject = maybeBillingProject.getOrElse(
        throw RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Billing Project $projectName does not exist")
        )
      )
      _ <- failUnlessBillingProjectReady(billingProject)
    } yield billingProject

  def failUnlessBillingProjectReady(billingProject: RawlsBillingProject): Future[Unit] =
    Applicative[Future].unlessA(billingProject.status == CreationStatuses.Ready) {
      Future.failed(
        RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Billing Project ${billingProject.projectName} is not ready")
        )
      )
    }

  def failIfWorkspaceExists(name: WorkspaceName): ReadWriteAction[Unit] =
    dataSource.dataAccess.workspaceQuery.getWorkspaceId(name).map { workspaceId =>
      if (workspaceId.isDefined)
        throw RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.Conflict, s"Workspace '$name' already exists")
        )
    }

}
