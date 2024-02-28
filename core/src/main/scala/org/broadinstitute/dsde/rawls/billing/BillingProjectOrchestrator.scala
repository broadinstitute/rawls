package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.ServerError
import bio.terra.profile.client.{ApiException => BpmApiException}
import bio.terra.profile.model.CloudPlatform
import bio.terra.workspace.client.{ApiException => WsmApiException}
import com.typesafe.scalalogging.LazyLogging
import io.sentry.{Sentry, SentryEvent}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
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
  SamResourceTypeNames
}
import org.broadinstitute.dsde.rawls.util.UserUtils
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchUserId}

import java.util.UUID
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
                                 billingProfileManagerDAO: BillingProfileManagerDAO,
                                 billingRepository: BillingRepository,
                                 googleBillingProjectLifecycle: BillingProjectLifecycle,
                                 azureBillingProjectLifecycle: BillingProjectLifecycle,
                                 config: MultiCloudWorkspaceConfig,
                                 resourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao
)(implicit val executionContext: ExecutionContext)
    extends StringValidationUtils
    with UserUtils
    with LazyLogging {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  /**
   * Creates a "v2" billing project, using either Azure managed app coordinates or a Google Billing Account
   */
  def createBillingProjectV2(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit] = {
    def tagAndCaptureSentryEvent(e: Throwable): Unit = {
      val sentryEvent = new SentryEvent()
      sentryEvent.setTag("component", "billing")
      sentryEvent.setThrowable(e)
      Sentry.captureEvent(sentryEvent)
      throw e
    }

    val billingProjectLifecycle = createProjectRequest.billingInfo match {
      case Left(_)  => googleBillingProjectLifecycle
      case Right(_) => azureBillingProjectLifecycle
    }
    val billingProjectName = createProjectRequest.projectName

    (for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)
      _ = logger.info(s"Validating billing project creation request [name=${billingProjectName.value}]")
      _ <- billingProjectLifecycle.validateBillingProjectCreationRequest(createProjectRequest, ctx)

      _ = logger.info(s"Creating billing project record [name=${billingProjectName}]")
      _ <- createV2BillingProjectInternal(createProjectRequest, ctx)

      _ = logger.info(s"Created billing project record, running post-creation steps [name=${billingProjectName.value}]")
      creationStatus <- billingProjectLifecycle
        .postCreationSteps(createProjectRequest, config, ctx)
        .recoverWith { case t: Throwable =>
          logger.error(s"Error in post-creation steps for billing project [name=${billingProjectName.value}]", t)
          billingProjectLifecycle.unregisterBillingProject(createProjectRequest.projectName, ctx).map(throw t)
        }
      _ = logger.info(s"Post-creation steps succeeded, setting billing project status [status=$creationStatus]")
      _ <- billingRepository.updateCreationStatus(
        createProjectRequest.projectName,
        creationStatus,
        None
      )
    } yield {}).recover {
      case e: RawlsExceptionWithErrorReport =>
        e.errorReport.statusCode.collect {
          case _: ServerError =>
            tagAndCaptureSentryEvent(e)
          case _ => throw e
        }
      case wsmException: WsmApiException if wsmException.getCode >= 500 => tagAndCaptureSentryEvent(wsmException)
      case bpmException: BpmApiException if bpmException.getCode >= 500 => tagAndCaptureSentryEvent(bpmException)
    }
  }

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
      cloudPlatform = billingProfileId match {
        case Some(profileId) =>
          val billingProfile = billingProfileManagerDAO.getBillingProfile(UUID.fromString(profileId), ctx)
          billingProfile
            .getOrElse(
              throw new BillingProjectDeletionException(
                ErrorReport(
                  s"Unable to find billing profile with billingProfileId: $profileId. Deletion cannot continue because the CloudPlatform cannot be determined."
                )
              )
            )
            .getCloudPlatform
        case None => CloudPlatform.GCP
      }
      projectLifecycle = cloudPlatform match {
        case CloudPlatform.GCP   => googleBillingProjectLifecycle
        case CloudPlatform.AZURE => azureBillingProjectLifecycle
      }
      _ <- billingRepository.failUnlessHasNoWorkspaces(projectName)
      _ <- billingRepository.getCreationStatus(projectName).map { status =>
        if (!CreationStatuses.terminal.contains(status))
          throw new BillingProjectDeletionException(
            ErrorReport(
              s"Billing project ${projectName.value} cannot be deleted when in status of $status"
            )
          )
      }
      jobControlId <- projectLifecycle.initiateDelete(projectName, ctx)
      _ <- jobControlId match {
        case Some(id) =>
          resourceMonitorRecordDao
            .create(
              WorkspaceManagerResourceMonitorRecord.forBillingProjectDelete(
                id,
                projectName,
                ctx.userInfo.userEmail,
                projectLifecycle.deleteJobType
              )
            )
            .flatMap(_ => billingRepository.updateCreationStatus(projectName, CreationStatuses.Deleting, None))
        case None => projectLifecycle.finalizeDelete(projectName, ctx)
      }
    } yield ()

}

object BillingProjectOrchestrator {
  def constructor(
    samDAO: SamDAO,
    notificationDAO: NotificationDAO,
    billingProfileManagerDAO: BillingProfileManagerDAO,
    billingRepository: BillingRepository,
    googleBillingProjectLifecycle: GoogleBillingProjectLifecycle,
    azureBillingProjectLifecycle: AzureBillingProjectLifecycle,
    resourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
    config: MultiCloudWorkspaceConfig
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): BillingProjectOrchestrator =
    new BillingProjectOrchestrator(
      ctx,
      samDAO,
      notificationDAO,
      billingProfileManagerDAO,
      billingRepository,
      googleBillingProjectLifecycle,
      azureBillingProjectLifecycle,
      config,
      resourceMonitorRecordDao
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
