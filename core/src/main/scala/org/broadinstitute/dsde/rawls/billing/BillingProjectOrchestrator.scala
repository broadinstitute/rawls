package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport,
  ErrorReportSource,
  ProjectAccessUpdate,
  ProjectRoles,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamBillingProjectPolicyNames,
  SamBillingProjectRoles,
  SamPolicy,
  SamResourcePolicyName,
  SamResourceTypeNames,
  UserInfo
}
import org.broadinstitute.dsde.rawls.util.UserUtils
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchUserId}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Knows how to provision billing projects with external cloud providers (that is, implementors of the
 * BillingProjectLifecycle trait)
 *
 * All billing projects are created following this algorithm:
 * 1. Pre-flight validation with a billing project creator. Right now, the creator is determined according
 * to the nature of the billing project creation request and is not client-configurable.
 * 2. Create the rawls internal billing project record
 * 3. Post-flight steps; this may include syncing of groups, reaching out to external services to sync state, etc.
 * This step is delegated to the billing project creator as well.
 */
class BillingProjectOrchestrator(ctx: RawlsRequestContext,
                                 val samDAO: SamDAO,
                                 notificationDAO: NotificationDAO,
                                 billingRepository: BillingRepository,
                                 googleBillingProjectLifecycle: BillingProjectLifecycle,
                                 bpmBillingProjectLifecycle: BillingProjectLifecycle,
                                 config: MultiCloudWorkspaceConfig
)(implicit val executionContext: ExecutionContext)
    extends StringValidationUtils
    with UserUtils
    with LazyLogging {
  implicit val errorReportSource = ErrorReportSource("rawls")

  /**
   * Creates a "v2" billing project, using either Azure managed app coordinates or a Google Billing Account
   */
  def createBillingProjectV2(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit] = {
    val billingProjectLifecycle = createProjectRequest.billingInfo match {
      case Left(_)  => googleBillingProjectLifecycle
      case Right(_) => bpmBillingProjectLifecycle
    }
    val billingProjectName = createProjectRequest.projectName

    for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)

      _ = logger.info(s"Validating billing project creation request [name=${billingProjectName.value}]")
      _ <- billingProjectLifecycle.validateBillingProjectCreationRequest(createProjectRequest, ctx)

      _ = logger.info(s"Creating billing project record [name=${billingProjectName}]")
      _ <- createV2BillingProjectInternal(createProjectRequest, ctx)

      _ = logger.info(s"Created billing project record, running post-creation steps [name=${billingProjectName.value}]")
      creationStatus <- billingProjectLifecycle.postCreationSteps(createProjectRequest, config, ctx).recoverWith {
        case t: Throwable =>
          logger.error(s"Error in post-creation steps for billing project [name=${billingProjectName.value}]", t)
          rollbackCreateV2BillingProjectInternal(createProjectRequest).map(throw t)
      }
      _ = logger.info(s"Post-creation steps succeeded, setting billing project status [status=$creationStatus]")
      _ <- billingRepository.updateCreationStatus(
        createProjectRequest.projectName,
        creationStatus,
        None
      )
    } yield {}
  }

  private def rollbackCreateV2BillingProjectInternal(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest
  ): Future[Unit] =
    for {
      _ <- billingRepository.deleteBillingProject(createProjectRequest.projectName).recover { case e =>
        logger.error(
          s"Failure deleting billing project from DB during error recovery [name=${createProjectRequest.projectName.value}]",
          e
        )
      }
      _ <- samDAO
        .deleteResource(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, ctx)
        .recover { case e =>
          logger.error(
            s"Failure deleting billing project resource in SAM during error recovery [name=${createProjectRequest.projectName.value}]",
            e
          )
        }
    } yield {}

  private def createV2BillingProjectInternal(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                             ctx: RawlsRequestContext
  ): Future[Unit] = {

    val additionalMembers = createProjectRequest.members.getOrElse(Set.empty)
    val policies = BillingProjectOrchestrator.buildBillingProjectPolicies(additionalMembers, ctx)
    val inviteUsersNotFound = createProjectRequest.inviteUsersNotFound.getOrElse(false)

    for {
      maybeProject <- billingRepository.getBillingProject(createProjectRequest.projectName)
      _ <- maybeProject match {
        case Some(_) =>
          Future.failed(
            new DuplicateBillingProjectException(
              ErrorReport(StatusCodes.Conflict, "project by that name already exists")
            )
          )
        case None => Future.successful(())
      }
      membersToInvite <- collectMissingUsers(additionalMembers.map(_.email), ctx)
      _ <- membersToInvite match {
        case result if result.nonEmpty && !inviteUsersNotFound =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Conflict, s"Users ${membersToInvite.mkString(",")} have not signed up for Terra")
            )
          )
        case _ => Future.successful(())
      }
      invites <- Future.traverse(membersToInvite) { invite =>
        samDAO.inviteUser(invite, ctx).map { _ =>
          Notifications.BillingProjectInvitedNotification(
            WorkbenchEmail(invite),
            WorkbenchUserId(ctx.userInfo.userSubjectId.value),
            createProjectRequest.projectName.value
          )
        }
      }
      _ <- samDAO.createResourceFull(
        SamResourceTypeNames.billingProject,
        createProjectRequest.projectName.value,
        policies,
        Set.empty,
        ctx,
        None
      )
      _ <- billingRepository.createBillingProject(
        RawlsBillingProject(createProjectRequest.projectName,
                            CreationStatuses.Creating,
                            createProjectRequest.billingAccount,
                            None,
                            None,
                            createProjectRequest.servicePerimeter
        )
      )
    } yield notificationDAO.fireAndForgetNotifications(invites)
  }

  def deleteBillingProjectV2(projectName: RawlsBillingProjectName): Future[Unit] =
    for {
      _ <- samDAO
        .userHasAction(SamResourceTypeNames.billingProject,
                       projectName.value,
                       SamBillingProjectActions.deleteBillingProject,
                       ctx
        )
        .flatMap {
          case true => Future.successful()
          case false =>
            Future.failed(
              new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a project owner.")
              )
            )
        }
      billingProfileId <- billingRepository.getBillingProfileId(projectName)
      projectLifecycle = billingProfileId match {
        case None    => googleBillingProjectLifecycle
        case Some(_) => bpmBillingProjectLifecycle
      }
      _ <- billingRepository.failUnlessHasNoWorkspaces(projectName)
      _ <- projectLifecycle.preDeletionSteps(projectName, ctx)
      _ <- unregisterBillingProjectWithUserInfo(projectName, ctx.userInfo)
    } yield {}

  // This code also lives in UserService.
  def unregisterBillingProjectWithUserInfo(projectName: RawlsBillingProjectName,
                                           ownerUserInfo: UserInfo
  ): Future[Unit] =
    for {
      _ <- billingRepository.deleteBillingProject(projectName)
      _ <- samDAO
        .deleteResource(SamResourceTypeNames.billingProject,
                        projectName.value,
                        ctx.copy(userInfo = ownerUserInfo)
        ) recoverWith { // Moving this to the end so that the rawls record is cleared even if there are issues clearing the Sam resource (theoretical workaround for https://broadworkbench.atlassian.net/browse/CA-1206)
        case t: Throwable =>
          logger.warn(
            s"Unexpected failure deleting billing project (while deleting billing project in Sam) for billing project `${projectName.value}`",
            t
          )
          throw t
      }
    } yield {}
}

object BillingProjectOrchestrator {
  def constructor(samDAO: SamDAO,
                  notificationDAO: NotificationDAO,
                  billingRepository: BillingRepository,
                  googleBillingProjectLifecycle: GoogleBillingProjectLifecycle,
                  bpmBillingProjectLifecycle: BpmBillingProjectLifecycle,
                  config: MultiCloudWorkspaceConfig
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): BillingProjectOrchestrator =
    new BillingProjectOrchestrator(ctx,
                                   samDAO,
                                   notificationDAO,
                                   billingRepository,
                                   googleBillingProjectLifecycle,
                                   bpmBillingProjectLifecycle,
                                   config
    )

  def buildBillingProjectPolicies(additionalMembers: Set[ProjectAccessUpdate],
                                  ctx: RawlsRequestContext
  ): Map[SamResourcePolicyName, SamPolicy] = {
    val owners = additionalMembers
      .filter(_.role == ProjectRoles.Owner)
      .map(member => WorkbenchEmail(member.email)) + WorkbenchEmail(ctx.userInfo.userEmail.value)
    val users = additionalMembers.filter(_.role == ProjectRoles.User).map(member => WorkbenchEmail(member.email))

    Map(
      SamBillingProjectPolicyNames.owner -> SamPolicy(owners, Set.empty, Set(SamBillingProjectRoles.owner)),
      SamBillingProjectPolicyNames.workspaceCreator -> SamPolicy(users,
                                                                 Set.empty,
                                                                 Set(SamBillingProjectRoles.workspaceCreator)
      )
    )
  }
}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneCreationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class BillingProjectDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
