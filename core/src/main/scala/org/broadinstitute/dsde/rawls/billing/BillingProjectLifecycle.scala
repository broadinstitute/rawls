package org.broadinstitute.dsde.rawls.billing

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, ErrorReport, RawlsRequestContext}

import scala.concurrent.Future

/**
 * Handles provisioning and deleting billing projects with external providers. Implementors of this trait are not concerned
 * with internal Rawls state (db records, etc.), but rather ensuring that
 * a) the creation request is valid for the given cloud provider
 * b) external state is valid after rawls internal state is updated (i.e., syncing groups, etc.)
 */
trait BillingProjectLifecycle extends LazyLogging {

  // Resources common to all implementations.
  val samDAO: SamDAO
  val billingRepository: BillingRepository

  // The type of WorkspaceManagerResourceMonitorRecord job that should be created to finalize deletion when necessary
  val deleteJobType: JobType

  def validateBillingProjectCreationRequest(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
    ctx: RawlsRequestContext
  ): Future[Unit]

  def postCreationSteps(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
    billingProjectDeletion: BillingProjectDeletion,
    ctx: RawlsRequestContext
  ): Future[CreationStatus]
}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class BillingProjectDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
