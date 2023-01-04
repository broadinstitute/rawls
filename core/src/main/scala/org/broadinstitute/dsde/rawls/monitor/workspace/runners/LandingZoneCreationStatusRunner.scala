package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.JobReport
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName, RawlsRequestContext}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class LandingZoneCreationStatusRunner(
  samDAO: SamDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  billingRepository: BillingRepository,
  gcsDAO: GoogleServicesDAO
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging {
  override def apply(
    job: WorkspaceManagerResourceMonitorRecord
  )(implicit executionContext: ExecutionContext): Future[JobStatus] = {
    val billingProjectName = job.billingProjectId match {
      case Some(name) => RawlsBillingProjectName(name)
      case None =>
        logger.error(
          s"Job to monitor AzureLandingZoneResult created with id ${job.jobControlId} but no billing project set"
        )
        return Future.successful(Complete) // nothing more this runner can do with it
    }
    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        logger.error(
          s"Job to monitor AzureLandingZoneResult ${billingProjectName} created with id ${job.jobControlId} but no user email set"
        )
        val errorMsg =
          s"Unable to update ${billingProjectName.value} with landing zone status because no user email is stored on monitoring job"
        return billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(errorMsg))
          .map(_ => Complete)
    }

    getUserCtx(userEmail).transformWith {
      case Failure(t) =>
        val msg = s"Unable to retrieve landing zone creation results: unable to retrieve request context for $userEmail"
        logger.error(
          s"AzureLandingZoneResult job ${job.jobControlId} for billing project: $billingProjectName failed: $msg",
          t
        )
        billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(msg))
          .map(_ => Incomplete)
      case Success(userCtx) =>
        Try(workspaceManagerDAO.getCreateAzureLandingZoneResult(job.jobControlId.toString, userCtx)) match {
          case Failure(exception) =>
            val message = Some(s"Api call to get landing zone from workspace manager failed: ${exception.getMessage}")
            billingRepository
              .updateCreationStatus(billingProjectName, CreationStatuses.Error, message)
              .map(_ => Incomplete)
          case Success(result) =>
            Option(result.getJobReport).map(_.getStatus) match {
              case Some(JobReport.StatusEnum.RUNNING) => Future.successful(Incomplete)
              case Some(JobReport.StatusEnum.SUCCEEDED) =>
                billingRepository
                  .updateCreationStatus(billingProjectName, CreationStatuses.Ready, None)
                  .map(_ => Complete)
              // set the error, and indicate this runner is finished with the job
              case Some(JobReport.StatusEnum.FAILED) =>
                val msg = Option(result.getErrorReport)
                  .map(_.getMessage)
                  .getOrElse("Failure Reported, but no errors returned")
                billingRepository
                  .updateCreationStatus(
                    billingProjectName,
                    CreationStatuses.Error,
                    Some(s"Landing Zone creation failed: $msg")
                  )
                  .map(_ => Complete)
              case None =>
                val msg = Option(result.getErrorReport)
                  .map(_.getMessage)
                  .getOrElse("No job status or errors returned from landing zone creation")
                billingRepository
                  .updateCreationStatus(
                    billingProjectName,
                    CreationStatuses.Error,
                    Some(s"Landing Zone creation failed: $msg")
                  )
                  .map(_ => Complete)
            }
        }
    }
  }

  def getUserCtx(userEmail: String)(implicit executionContext: ExecutionContext): Future[RawlsRequestContext] = for {
    petKey <- samDAO.getUserArbitraryPetServiceAccountKey(userEmail)
    userInfo <- gcsDAO.getUserInfoUsingJson(petKey)
  } yield RawlsRequestContext(userInfo)

}
