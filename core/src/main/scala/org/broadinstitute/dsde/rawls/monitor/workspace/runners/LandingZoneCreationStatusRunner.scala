package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.{AzureLandingZoneResult, JobReport}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
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

  override val jobType: JobType = JobType.AzureLandingZoneResult

  def failureMessage(result: AzureLandingZoneResult): Some[String] = {
    val msg = () match {
      case _ if result.getErrorReport != null && result.getErrorReport.getMessage != null =>
        result.getErrorReport.getMessage
      case _ if result.getJobReport != null =>
        result.getJobReport.getStatus match {
          case JobReport.StatusEnum.FAILED => "Failure Reported, but no errors returned"
          case JobReport.StatusEnum.SUCCEEDED if result.getLandingZone == null =>
            "Result marked as success, but no landing zone returned"
          case JobReport.StatusEnum.RUNNING | JobReport.StatusEnum.SUCCEEDED =>
            "Invalid Failure" // FIXME: improve message (we should never get into this state)
          case null => "No status reported"
        }
      // no job report, no error report (or at least no error report message), also no lz (since that would still be a success)
      case _ => "No landing zone, job report, or errors returned from landing zone creation"
    }
    Some(s"Landing Zone creation failed: $msg")
  }

  override def run(
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
          s"Job to monitor AzureLandingZoneResult for billing project $billingProjectName created with id ${job.jobControlId} but no user email set"
        )
        val errorMsg =
          s"Unable to update ${billingProjectName.value} with landing zone status because no user email is stored on monitoring job"
        billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(errorMsg))
        return Future.successful(Complete)
    }
    getUserCtx(userEmail)
      .map(userCtx =>
        Try(workspaceManagerDAO.getCreateAzureLandingZoneResult(job.jobControlId.toString, userCtx)) match {
          case Failure(exception) =>
            val message = Some(s"Api call to get landing zone from workspace manager failed: ${exception.getMessage}")
            billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Error, message)
            Incomplete
          case Success(result) if result.getJobReport != null =>
            result.getJobReport.getStatus match {
              // the job just isn't done yet - return indicates this runner isn't finished with the job so it's rescheduled
              case JobReport.StatusEnum.RUNNING => Incomplete
              // a normal success
              case JobReport.StatusEnum.SUCCEEDED if result.getLandingZone != null =>
                billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Ready, None)
                Complete
              // set the error, and indicate this runner is finished with the job
              case _ =>
                billingRepository
                  .updateCreationStatus(billingProjectName, CreationStatuses.Error, failureMessage(result))
                Complete
            }
          // this case is only hit if the job report is null but the landing zone is present in the response
          case Success(result) if result.getLandingZone != null && result.getLandingZone.getId != null =>
            billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Ready, None)
            Complete
          case Success(result) if result.getErrorReport != null =>
            billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Error, failureMessage(result))
            Complete
          // no landing zone, no error report, and no job report: set job status to failure but retry in the future
          case Success(result) =>
            billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Error, failureMessage(result))
            Incomplete
        }
      )
      .recover { case t: Throwable =>
        logger.error(
          s"Unable to retrieve Pet service account for: $userEmail, to update status on billing project: $billingProjectName, for job: ${job.jobControlId}",
          t
        )
        val msg = s"Unable to retrieve landing zone creation results: unable to build request context for $userEmail"
        billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(msg))
        Incomplete
      }
  }

  def getUserCtx(userEmail: String)(implicit executionContext: ExecutionContext): Future[RawlsRequestContext] = for {
    petKey <- samDAO.getUserArbitraryPetServiceAccountKey(userEmail)
    userInfo <- gcsDAO.getUserInfoUsingJson(petKey)
  } yield RawlsRequestContext(userInfo)

}
