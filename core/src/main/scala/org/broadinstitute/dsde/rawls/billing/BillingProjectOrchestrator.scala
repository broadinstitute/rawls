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
  RawlsBillingProject,
  RawlsRequestContext,
  SamBillingProjectPolicyNames,
  SamBillingProjectRoles,
  SamPolicy,
  SamResourcePolicyName,
  SamResourceTypeNames
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}

/**
 * Knows how to provision billing projects with external cloud providers (that is, implementors of the
 * BillingProjectCreator trait)
 *
 * All billing projects are created following this algorithm:
 * 1. Pre-flight validation with a billing project creator. Right now, the creator is determined according
 * to the nature of the billing project creation request and is not client-configurable.
 * 2. Create the rawls internal billing project record
 * 3. Post-flight steps; this may include syncing of groups, reaching out to external services to sync state, etc.
 * This step is delegated to the billing project creator as well.
 */
class BillingProjectOrchestrator(ctx: RawlsRequestContext,
                                 samDAO: SamDAO,
                                 billingRepository: BillingRepository,
                                 googleBillingProjectCreator: BillingProjectCreator,
                                 bpmBillingProjectCreator: BillingProjectCreator,
                                 config: MultiCloudWorkspaceConfig
)(implicit val executionContext: ExecutionContext)
    extends StringValidationUtils
    with LazyLogging {
  implicit val errorReportSource = ErrorReportSource("rawls")

  /**
   * Creates a "v2" billing project, using either Azure managed app coordinates or a Google Billing Account
   */
  def createBillingProjectV2(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit] = {
    val billingProjectCreator = createProjectRequest.billingInfo match {
      case Left(_)  => googleBillingProjectCreator
      case Right(_) => bpmBillingProjectCreator
    }
    val billingProjectName = createProjectRequest.projectName

    for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)

      _ = logger.info(s"Validating billing project creation request [name=${billingProjectName.value}]")
      _ <- billingProjectCreator.validateBillingProjectCreationRequest(createProjectRequest, ctx)

      _ = logger.info(s"Creating billing project record [name=${billingProjectName}]")
      _ <- createV2BillingProjectInternal(createProjectRequest, ctx)

      _ = logger.info(s"Created billing project record, running post-creation steps [name=${billingProjectName.value}]")
      creationStatus <- billingProjectCreator.postCreationSteps(createProjectRequest, config, ctx).recoverWith {
        case t: Throwable =>
          logger.error(s"Error in post-creation steps for billing project [name=${billingProjectName.value}]")
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
  ): Future[Unit] =
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
      _ <- samDAO.createResourceFull(
        SamResourceTypeNames.billingProject,
        createProjectRequest.projectName.value,
        BillingProjectOrchestrator.defaultBillingProjectPolicies(ctx),
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
    } yield {}
}

object BillingProjectOrchestrator {
  def constructor(samDAO: SamDAO,
                  billingRepository: BillingRepository,
                  googleBillingProjectCreator: GoogleBillingProjectCreator,
                  bpmBillingProjectCreator: BpmBillingProjectCreator,
                  config: MultiCloudWorkspaceConfig
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): BillingProjectOrchestrator =
    new BillingProjectOrchestrator(ctx,
                                   samDAO,
                                   billingRepository,
                                   googleBillingProjectCreator,
                                   bpmBillingProjectCreator,
                                   config
    )

  def defaultBillingProjectPolicies(ctx: RawlsRequestContext): Map[SamResourcePolicyName, SamPolicy] =
    Map(
      SamBillingProjectPolicyNames.owner -> SamPolicy(Set(WorkbenchEmail(ctx.userInfo.userEmail.value)),
                                                      Set.empty,
                                                      Set(SamBillingProjectRoles.owner)
      ),
      SamBillingProjectPolicyNames.workspaceCreator -> SamPolicy(Set.empty,
                                                                 Set.empty,
                                                                 Set(SamBillingProjectRoles.workspaceCreator)
      )
    )
}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneCreationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
