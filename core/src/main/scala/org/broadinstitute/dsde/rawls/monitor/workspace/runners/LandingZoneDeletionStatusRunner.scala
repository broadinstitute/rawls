package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkspaceManagerResourceJobRunner, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, Incomplete, JobStatus}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class LandingZoneDeletionStatusRunner(
  val samDAO: SamDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  billingRepository: BillingRepository,
  val gcsDAO: GoogleServicesDAO,
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging with UserCtxCreator {

  override def apply(
    job: WorkspaceManagerResourceMonitorRecord
  )(implicit executionContext: ExecutionContext): Future[JobStatus] = {
    val billingProjectName = job.billingProjectId match {
      case Some(name) => RawlsBillingProjectName(name)
      case None =>
        logger.error(
          s"Job to monitor AzureLandingZoneDeleteResult created with id ${job.jobControlId} but no billing project set"
        )
        return Future.successful(Complete) // nothing more this runner can do with it
    }
    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        logger.error(
          s"Job to monitor AzureLandingZoneDeleteResult ${billingProjectName} created with id ${job.jobControlId} but no user email set"
        )
        val errorMsg =
          s"Unable to update ${billingProjectName.value} with landing zone deletion status because no user email is stored on monitoring job"
        return billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(errorMsg))
          .map(_ => Complete)
    }
    getUserCtx(userEmail).transformWith {
      case Failure(t) =>
        val msg = s"Unable to retrieve landing zone deletion results: unable to retrieve request context for $userEmail"
        logger.error(
          s"AzureLandingZoneDeleteResult job ${job.jobControlId} for billing project: $billingProjectName failed: $msg",
          t
        )
        billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(msg))
          .map(_ => Incomplete)
      case Success(userCtx) => ???
      //BPMBillingProjectLifecycle.cleanupBillingProfile(UUID.fromString(billingProfileId), projectName, ctx)

    }

  }

}
