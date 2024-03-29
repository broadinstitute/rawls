package org.broadinstitute.dsde.rawls.billing

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsRequestContext, SamResourceTypeNames}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class BillingProjectDeletion(
  val samDAO: SamDAO,
  val billingRepository: BillingRepository,
  val billingProfileManagerDAO: BillingProfileManagerDAO
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging {

  /**
    * Delete the billing project and associated billing profile.
    *
    * @param projectName            the Rawls billing project name
    * @param ctx                    the Rawls request context
    */
  def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] = for {
    billingProfileId <- billingRepository.getBillingProfileId(projectName)
    _ <- billingProfileId match {
      case Some(id) => cleanupBillingProfile(UUID.fromString(id), projectName, ctx)
      case None     =>
        // Can change to a warning once all billing projects have billing profile IDs (WOR-866)
        logger.info(
          s"Deleting billing project $projectName, but no associated billing profile record to delete (could be a legacy project)"
        )
        Future.successful()
    }
  } yield unregisterBillingProject(projectName, ctx)

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
