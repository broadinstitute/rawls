package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.JobReport
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProjectLifecycle, BillingRepository}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.AzureBillingProjectDelete
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
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Delete a BPM billing project after the job corresponding with jobControlId has finished in WSM
  * (which will be a landing zone deletion job)
  * TODO: tenatively: Handles both AzureBillingProjectDelete and OtherBpmBillingProjectDelete
  *   If it's AzureBillingProjectDelete, it means we need to wait for the associated lz job to complete
  *   If it's OtherBpmBillingProjectDelete, we can delete right away
  */
class BPMBillingProjectDeleteRunner(
  val samDAO: SamDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  billingRepository: BillingRepository,
  billingLifecycle: BillingProjectLifecycle,
  val gcsDAO: GoogleServicesDAO
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
          s"Job to monitor AzureLandingZoneDeleteResult created with id ${job.jobControlId} but no billing project set"
        )
        return Future.successful(Complete) // nothing more this runner can do with it
    }
    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        logger.error(
          s"Job to monitor ${job.jobType} $billingProjectName created with id ${job.jobControlId} but no user email set"
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
          s"${job.jobType} job ${job.jobControlId} for billing project: $billingProjectName failed: $msg",
          t
        )
        billingRepository
          .updateCreationStatus(billingProjectName, CreationStatuses.Error, Some(msg))
          .map(_ => Incomplete)
      case Success(userCtx) =>
        job.jobType match {
          case AzureBillingProjectDelete =>
            Try(workspaceManagerDAO.getJob(job.jobControlId.toString, userCtx)) match {
              case Failure(exception) =>
                val message =
                  Some(s"Api call to get landing zone from workspace manager failed: ${exception.getMessage}")
                billingRepository
                  .updateCreationStatus(billingProjectName, CreationStatuses.Error, message)
                  .map(_ => Incomplete)
              case Success(result) =>
                result.getStatus match {
                  case JobReport.StatusEnum.RUNNING => Future.successful(Incomplete)
                  case JobReport.StatusEnum.SUCCEEDED =>
                    billingLifecycle.finalizeDelete(billingProjectName, userCtx).map(_ => Complete)
                  case JobReport.StatusEnum.FAILED =>
                    billingRepository
                      .updateCreationStatus(
                        billingProjectName,
                        CreationStatuses.Error,
                        Some(s"Landing Zone deletion failed: ${/*fixme: msg*/ }")
                      )
                      .map(_ => Complete)
                }
            }
          case _ => billingLifecycle.finalizeDelete(billingProjectName, userCtx).map(_ => Complete)
        }

    }
    /*
getUserCtx(userEmail).transformWith {
  case Failure(t) =>
    val msg =
      s"Unable to retrieve clone workspace results for workspace $workspaceId: unable to retrieve request context for $userEmail"
    logFailure(msg, Some(t))
    cloneFail(workspaceId, msg).map(_ => Incomplete)
  case Success(ctx) =>
    Try(workspaceManagerDAO.getJob(job.jobControlId.toString, ctx)) match {
      case Success(result) => handleCloneResult(workspaceId, result)
      case Failure(e: ApiException) =>
        e.getMessage
        e.getCode match {
          case 500 =>
            val msg = s"Clone Container operation with jobId ${job.jobControlId} failed: ${e.getMessage}"
            cloneFail(workspaceId, msg).map(_ => Complete)
          case 404 =>
            val msg = s"Unable to find jobId ${job.jobControlId} in WSM for clone container operation"
            cloneFail(workspaceId, msg).map(_ => Complete)
          case code =>
            logFailure(s"API call to get clone result failed with status code $code: ${e.getMessage}")
            Future.successful(Incomplete)
        }
      case Failure(t) =>
        val msg = s"API call to get clone result from workspace manager failed with: ${t.getMessage}"
        logFailure(msg, Some(t))
        cloneFail(workspaceId, msg).map(_ => Incomplete)
    }
}

}

def handleCloneResult(workspaceId: UUID, result: JobReport)(implicit
executionContext: ExecutionContext
): Future[JobStatus] = result.getStatus match {
case JobReport.StatusEnum.RUNNING => Future.successful(Incomplete)
case JobReport.StatusEnum.SUCCEEDED =>
  val completeTime = DateTime.parse(result.getCompleted)
  cloneSuccess(workspaceId, completeTime).map(_ => Complete)
// set the error, and indicate this runner is finished with the job
case JobReport.StatusEnum.FAILED =>
  cloneFail(workspaceId, "Cloning workspace resource container failed").map(_ => Complete)
}
     */
  }

  /*
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
   */
}
