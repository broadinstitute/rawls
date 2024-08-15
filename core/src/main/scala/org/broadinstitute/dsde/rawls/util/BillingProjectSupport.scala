package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import cats.{Applicative, ApplicativeThrow}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
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
  SamResourceTypeNames
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceFutureWithParent

import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait for common functionality for billing projects.
  * Split from WorkspaceSupport, to better fit scope
  */
trait BillingProjectSupport {
  val samDAO: SamDAO
  val billingRepository: BillingRepository
  protected val ctx: RawlsRequestContext
  implicit protected val executionContext: ExecutionContext

  /**
    * Throws an exception of the user does not have the "create_workspace" permission on the specified billing project
    */
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

  // Creating a Workspace without an Owner policy is allowed only if the requesting User has the `owner` role
  // granted on the Workspace's Billing Project
  def requireBillingProjectOwnerAccess(projectName: RawlsBillingProjectName,
                                       parentContext: RawlsRequestContext
  ): Future[Unit] =
    for {
      billingProjectRoles <- traceFutureWithParent("listUserRolesForResource", parentContext)(context =>
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

  /**
    * Load the specified billing project, throwing if the billing project is not ready.
    */
  def getBillingProjectContext(projectName: RawlsBillingProjectName,
                               context: RawlsRequestContext = ctx
  ): Future[RawlsBillingProject] =
    for {

      maybeBillingProject <- traceFutureWithParent("loadBillingProject", context) { _ =>
        billingRepository.getBillingProject(projectName)
      }

      billingProject = maybeBillingProject.getOrElse(
        throw RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Billing Project $projectName does not exist")
        )
      )
      _ <- failUnlessBillingProjectReady(billingProject)
    } yield billingProject

  /**
    * Throws an exception if the specified billing project status is not in a Ready state.
    */
  def failUnlessBillingProjectReady(billingProject: RawlsBillingProject): Future[Unit] =
    Applicative[Future].unlessA(billingProject.status == CreationStatuses.Ready) {
      Future.failed(
        RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Billing Project ${billingProject.projectName} is not ready")
        )
      )
    }

  /**
    * Throws the passed exception if the user does not have the specified action on the specified resource
    */
  private def raiseUnlessUserHasAction(action: SamResourceAction,
                                       resType: SamResourceTypeName,
                                       resId: String,
                                       context: RawlsRequestContext = ctx
  )(
    throwable: Throwable
  ): Future[Unit] =
    samDAO
      .userHasAction(resType, resId, action, context)
      .flatMap(ApplicativeThrow[Future].raiseUnless(_)(throwable))

}
