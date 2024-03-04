package org.broadinstitute.dsde.rawls.billing

import bio.terra.profile.model.ProfileModel
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, ErrorReport, RawlsRequestContext}

import scala.concurrent.{blocking, ExecutionContext, Future}

/**
 * Handles provisioning and deleting billing projects with external providers. Implementors of this trait are not concerned
 * with internal Rawls state (db records, etc.), but rather ensuring that
 * a) the creation request is valid for the given cloud provider
 * b) external state is valid after rawls internal state is updated (i.e, syncing groups, etc.)
 */
trait BillingProjectLifecycle extends LazyLogging {

  // Resources common to all implementations.
  val samDAO: SamDAO
  val billingRepository: BillingRepository
  val billingProfileManagerDAO: BillingProfileManagerDAO

  // The type of WorkspaceManagerResourceMonitorRecord job that should be created to finalize deletion when necessary
  val deleteJobType: JobType

  def validateBillingProjectCreationRequest(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
    ctx: RawlsRequestContext
  ): Future[Unit]

  def postCreationSteps(
    createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
    config: MultiCloudWorkspaceConfig,
    billingProjectDeletion: BillingProjectDeletion,
    ctx: RawlsRequestContext
  ): Future[CreationStatus]

  def createBillingProfile(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, ctx: RawlsRequestContext)(
    implicit executionContext: ExecutionContext
  ): Future[ProfileModel] =
    Future(blocking {
      val policies: Map[String, List[(String, String)]] =
        if (createProjectRequest.protectedData.getOrElse(false)) Map("protected-data" -> List[(String, String)]())
        else Map.empty
      val profileModel = billingProfileManagerDAO.createBillingProfile(
        createProjectRequest.projectName.value,
        createProjectRequest.billingInfo,
        policies,
        ctx
      )
      logger.info(
        s"Creating BPM-backed billing project ${createProjectRequest.projectName.value}, created profile with ID ${profileModel.getId}."
      )
      profileModel
    })(executionContext)

  def addMembersToBillingProfile(profileModel: ProfileModel,
                                 createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                 ctx: RawlsRequestContext
  )(implicit executionContext: ExecutionContext): Future[Set[Unit]] = {
    val members = createProjectRequest.members.getOrElse(Set.empty)
    Future.traverse(members) { member =>
      Future(blocking {
        billingProfileManagerDAO.addProfilePolicyMember(profileModel.getId,
                                                        ProfilePolicy.fromProjectRole(member.role),
                                                        member.email,
                                                        ctx
        )
      })
    }
  }
}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneCreationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class BillingProjectDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
