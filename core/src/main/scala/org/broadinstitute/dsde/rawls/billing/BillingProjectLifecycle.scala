package org.broadinstitute.dsde.rawls.billing

import bio.terra.profile.model.ProfileModel
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
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

  // This code also lives in UserService as unregisterBillingProjectWithUserInfo
  // if this was scala 3.x, we could just use a parameterized trait and this would work basically everywhere
  def unregisterBillingProject(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
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

  /**
    * Initiates deletion of a billing project
    * @return an id of an async job the final stages of deleting are waiting on, if applicable.
    *         If None is returned, the project can be deleted immediately via finalizeDelete
    */
  def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Option[UUID]]

  def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit]

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

  def deleteBillingProfileAndUnregisterBillingProject(projectName: RawlsBillingProjectName,
                                                      billingProfileExpected: Boolean,
                                                      ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): Future[Unit] = for {
    billingProfileId <- billingRepository.getBillingProfileId(projectName)
    _ <- (billingProfileId, billingProfileExpected) match {
      case (Some(id), _) => cleanupBillingProfile(UUID.fromString(id), projectName, ctx)
      case (None, true) =>
        logger.warn(
          s"Deleting billing project $projectName that was expected to have a billing profile, but no associated billing profile record to delete"
        )
        Future.successful()
      case (None, false) =>
        logger.info(
          s"Deleting billing project $projectName, but no associated billing profile record to delete (could be a legacy project)"
        )
        Future.successful()
    }
  } yield unregisterBillingProject(projectName, ctx)

  /**
    * Delete the billing profile if no other billing projects reference it. If an exception
    * is failed during deletion, allow it to pass up so caller can choose to disallow deletion
    * of parent billing project.
    */
  def cleanupBillingProfile(profileModelId: UUID, projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(
    implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    val numOtherProjectsWithProfile = for {
      allProjectsWithProfile <- billingRepository
        .getBillingProjectsWithProfile(Some(profileModelId))
      filtered = allProjectsWithProfile.filterNot(_.projectName == projectName)
    } yield filtered.length
    numOtherProjectsWithProfile map {
      case 0 =>
        logger.info(
          s"Deleting BPM-backed billing project ${projectName.value}, deleting billing profile record $profileModelId"
        )
        billingProfileManagerDAO.deleteBillingProfile(profileModelId, ctx)
      case num =>
        logger.info(
          s"Deleting BPM-backed billing project ${projectName.value}, but not deleting billing profile record $profileModelId because $num other project(s) reference it"
        )
    }
  }
}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneCreationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class LandingZoneDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class BillingProjectDeletionException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
