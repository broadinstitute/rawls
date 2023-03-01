package org.broadinstitute.dsde.rawls.billing

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  ErrorReport,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamResourceTypeNames
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
 * Handles provisioning and deleting billing projects with external providers. Implementors of this trait are not concerned
 * with internal Rawls state (db records, etc.), but rather ensuring that
 * a) the creation request is valid for the given cloud provider
 * b) external state is valid after rawls internal state is updated (i.e, syncing groups, etc.)
 */
trait BillingProjectLifecycle extends LazyLogging {

  // It's probably reasonable to expect that the billing project lifecyle always includes these two resources
  val samDAO: SamDAO
  val billingRepository: BillingRepository

  // This code also lives in UserService as unregisterBillingProjectWithUserInfo
  // if this was scala 3.x, we could just use a parameterized trait and this would work basically everywhere
  protected def unregisterBillingProject(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] =
    for {
      _ <- billingRepository.deleteBillingProject(projectName)
      _ <- samDAO
        .deleteResource(SamResourceTypeNames.billingProject, projectName.value, ctx) recoverWith { // Moving this to the end so that the rawls record is cleared even if there are issues clearing the Sam resource (theoretical workaround for https://broadworkbench.atlassian.net/browse/CA-1206)
        case t: Throwable =>
          logger.warn(
            s"Unexpected failure deleting billing project (while deleting billing project in Sam) for billing project `${projectName.value}`",
            t
          )
          throw t
      }
    } yield {}

  def validateBillingProjectCreationRequest(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
    ctx: RawlsRequestContext
  ): Future[Unit]

  def postCreationSteps(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
    config: MultiCloudWorkspaceConfig,
    ctx: RawlsRequestContext
  ): Future[CreationStatus]

  def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[(UUID, JobType)]

  def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit]

}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneCreationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class BillingProjectDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
