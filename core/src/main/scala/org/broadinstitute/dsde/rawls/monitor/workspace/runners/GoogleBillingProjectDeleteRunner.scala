package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProjectLifecycle, BillingRepository}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus
}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class GoogleBillingProjectDeleteRunner(
  val samDAO: SamDAO,
  val gcsDAO: GoogleServicesDAO,
  billingRepository: BillingRepository,
  billingLifecycle: BillingProjectLifecycle
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging
    with UserCtxCreator {

  override def apply(
    job: WorkspaceManagerResourceMonitorRecord
  )(implicit executionContext: ExecutionContext): Future[JobStatus] = {
    val billingProjectName = job.billingProjectId match {
      case Some(name) => RawlsBillingProjectName(name)
      case None =>
        logger.error(
          s"Job to run GoogleBillingProjectDelete created with id ${job.jobControlId} but no billing project set"
        )
        return Future.successful(Complete) // nothing more this runner can do with it
    }
    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        logger.error(
          s"Job to run GoogleBillingProjectDelete $billingProjectName created with id ${job.jobControlId} but no user email set"
        )
        val errorMsg = s"Unable to delete ${billingProjectName.value} because no user email is stored on monitoring job"
        return billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.DeletionFailed, Some(errorMsg))
          .map(_ => Complete)
    }
    getUserCtx(userEmail).transformWith {
      case Failure(t) =>
        val msg = s"Unable to delete billing project: unable to retrieve request context for $userEmail"
        logger.error(
          s"GoogleBillingProjectDelete job ${job.jobControlId} for billing project: $billingProjectName failed: $msg",
          t
        )
        billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.DeletionFailed, Some(msg))
          .map(_ => Incomplete)
      case Success(userCtx) =>
        billingLifecycle
          .finalizeDelete(billingProjectName, userCtx)
          .map(_ => Complete)
          .recoverWith { case t: Throwable =>
            val msg = s"Exception encountered when deleting billing project: $t"
            billingRepository
              .updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(msg))
              .map(_ => Complete)
          }
    }

  }

}
