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
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class LandingZoneCreationStatusRunner(
  val samDAO: SamDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  billingRepository: BillingRepository,
  val gcsDAO: GoogleServicesDAO
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

    Try(workspaceManagerDAO.getCreateAzureLandingZoneResult(job.jobControlId.toString, samDAO.rawlsSAContext)) match {
      case Failure(t) =>
        job.retryOrTimeout(() =>
          billingRepository.updateCreationStatus(
            billingProjectName,
            CreationStatuses.Error,
            Some(s"Unable to retrieve landing zone creation results: ${t.getMessage}")
          )
        )
      case Success(result) =>
        Option(result.getJobReport).map(_.getStatus) match {
          case Some(JobReport.StatusEnum.RUNNING) =>
            logger.info(s"Landing zone creation still running, jobControlId = ${job.jobControlId}")
            Future.successful(Incomplete)
          case Some(JobReport.StatusEnum.SUCCEEDED) =>
            logger.info(s"Landing zone creation succeeded, jobControlId = ${job.jobControlId}")
            billingRepository
              .updateCreationStatus(billingProjectName, CreationStatuses.Ready, None)
              .map(_ => Complete)
          // set the error, and indicate this runner is finished with the job
          case Some(JobReport.StatusEnum.FAILED) =>
            val msg = Option(result.getErrorReport)
              .map(_.getMessage)
              .getOrElse("Failure Reported, but no errors returned")
            logger.warn(s"Landing zone creation failed, jobControlId = ${job.jobControlId}, message = ${msg}")
            billingRepository
              .updateCreationStatus(
                billingProjectName,
                CreationStatuses.Error,
                Some(s"Landing Zone creation failed: $msg")
              )
              .flatMap(_ => billingRepository.updateLandingZoneId(billingProjectName, None))
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
