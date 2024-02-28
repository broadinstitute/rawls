package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.{DeleteAzureLandingZoneJobResult, JobReport}
import com.typesafe.scalalogging.LazyLogging
import bio.terra.workspace.client.ApiException
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingProjectLifecycle, BillingRepository}
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
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.{Deleting, DeletionFailed}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Delete a BPM billing project after the job corresponding with jobControlId has finished in WSM
  * (which will be a landing zone deletion job)
  * Handles both AzureBillingProjectDelete and OtherBpmBillingProjectDelete
  *   If it's AzureBillingProjectDelete, it means we need to wait for the associated lz job to complete
  *   If it's OtherBpmBillingProjectDelete, we can delete right away
  */
class BPMBillingProjectDeleteRunner(
  val samDAO: SamDAO,
  val gcsDAO: GoogleServicesDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  billingProfileManagerDAO: BillingProfileManagerDAO,
  billingRepository: BillingRepository,
  billingLifecycle: BillingProjectLifecycle
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging
    with UserCtxCreator {

  override def apply(
    job: WorkspaceManagerResourceMonitorRecord
  )(implicit executionContext: ExecutionContext): Future[JobStatus] = {
    val projectName = job.billingProjectId match {
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
          s"Job to monitor ${job.jobType} $projectName created with id ${job.jobControlId} but no user email set"
        )
        val errorMsg =
          s"Unable to update ${projectName.value} with landing zone deletion status because no user email is stored on monitoring job"
        return billingRepository
          .updateCreationStatus(projectName, DeletionFailed, Some(errorMsg))
          .map(_ => Complete)
    }

    getUserCtx(userEmail).transformWith {
      case Success(userCtx) =>
        billingRepository.getLandingZoneId(projectName).flatMap {
          case Some(landingZoneId) =>
            handleLandingZoneDeletion(job.jobControlId, projectName, UUID.fromString(landingZoneId), userCtx)
          case None =>
            billingLifecycle.finalizeDelete(projectName, billingProfileManagerDAO, userCtx).map(_ => Complete)
        }
      case Failure(t) =>
        val msg = s"Unable to complete billing project deletion: unable to retrieve request context for $userEmail"
        logger.error(s"${job.jobType} job ${job.jobControlId} for billing project: $projectName failed: $msg", t)
        billingRepository.updateCreationStatus(projectName, Deleting, Some(msg)).map(_ => Incomplete)
    }
  }

  private def errorReportMessage(result: DeleteAzureLandingZoneJobResult): String = Option(result.getErrorReport)
    .map { report =>
      s"Landing Zone deletion failed: ${report.getMessage}"
    }
    .getOrElse(s"Landing Zone deletion failed: no error reported in response")

  private def handleLandingZoneDeletion(jobId: UUID,
                                        projectName: RawlsBillingProjectName,
                                        lzId: UUID,
                                        ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): Future[JobStatus] =
    Try(workspaceManagerDAO.getDeleteLandingZoneResult(jobId.toString, lzId, ctx)) match {
      case Failure(e: ApiException) =>
        val msg =
          s"API call to get Landing Zone deletion operation failed with status code ${e.getCode}: ${e.getMessage}"
        e.getCode match {
          case code if code >= 400 && code < 500 =>
            billingRepository.updateCreationStatus(projectName, DeletionFailed, Some(msg)).map(_ => Complete)
          case _ =>
            billingRepository.updateCreationStatus(projectName, Deleting, Some(msg)).map(_ => Incomplete)
        }
      case Failure(e) =>
        val msg = s"Api call to get landing zone delete job $jobId from workspace manager failed: ${e.getMessage}"
        billingRepository.updateCreationStatus(projectName, DeletionFailed, Some(msg)).map(_ => Incomplete)
      case Success(result) =>
        Option(result.getJobReport).map(_.getStatus) match {
          case Some(JobReport.StatusEnum.RUNNING) => Future.successful(Incomplete)
          case Some(JobReport.StatusEnum.SUCCEEDED) =>
            billingLifecycle.finalizeDelete(projectName, billingProfileManagerDAO, ctx).map(_ => Complete)
          case Some(JobReport.StatusEnum.FAILED) =>
            billingRepository
              .updateCreationStatus(projectName, DeletionFailed, Some(errorReportMessage(result)))
              .map(_ => Complete)
          case None =>
            billingRepository
              .updateCreationStatus(projectName, DeletionFailed, Some(errorReportMessage(result)))
              .map(_ => Complete)
        }
    }

}
