package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{DeleteAzureLandingZoneJobResult, JobReport}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProjectDeletion, BillingRepository}
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
import org.broadinstitute.dsde.rawls.model.CreationStatuses.{Deleting, DeletionFailed}
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsRequestContext}

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
  billingRepository: BillingRepository,
  billingProjectDeletion: BillingProjectDeletion
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging
    with RawlsSAContextCreator {

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

    getRawlsSAContext().transformWith {
      case Success(userCtx) =>
        billingRepository.getLandingZoneId(projectName).flatMap {
          case Some(landingZoneId) =>
            handleLandingZoneDeletion(job, projectName, UUID.fromString(landingZoneId), userCtx)
          case None => billingProjectDeletion.finalizeDelete(projectName, userCtx).map(_ => Complete)
        }
      case Failure(t) =>
        val msg = s"Unable to complete billing project deletion: unable to retrieve rawls SA context"
        logger.error(s"${job.jobType} job ${job.jobControlId} for billing project: $projectName failed: $msg", t)
        job.retryOrTimeout(() => billingRepository.updateCreationStatus(projectName, DeletionFailed, Some(msg)))
    }
  }

  private def errorReportMessage(result: DeleteAzureLandingZoneJobResult): String = Option(result.getErrorReport)
    .map { report =>
      s"Landing Zone deletion failed: ${report.getMessage}"
    }
    .getOrElse(s"Landing Zone deletion failed: no error reported in response")

  private def handleLandingZoneDeletion(job: WorkspaceManagerResourceMonitorRecord,
                                        projectName: RawlsBillingProjectName,
                                        lzId: UUID,
                                        ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): Future[JobStatus] =
    Try(workspaceManagerDAO.getDeleteLandingZoneResult(job.jobControlId.toString, lzId, ctx)) match {
      case Failure(e: ApiException) if e.getCode == StatusCodes.Forbidden.intValue =>
        logger.info(
          s"LZ deletion result status = ${e.getCode} for LZ ID ${lzId}, LZ record is gone. Proceeding with rawls billing project deletion"
        )
        billingProjectDeletion.finalizeDelete(projectName, ctx).map(_ => Complete)
      case Failure(e: ApiException) =>
        val msg =
          s"API call to get Landing Zone deletion operation failed with status code ${e.getCode}: ${e.getMessage}"
        e.getCode match {
          case code if code >= 400 && code < 500 =>
            billingRepository.updateCreationStatus(projectName, DeletionFailed, Option(msg)).map(_ => Complete)
          case _ =>
            job.retryOrTimeout(() => billingRepository.updateCreationStatus(projectName, DeletionFailed, Option(msg)))
        }
      case Failure(t) =>
        job.retryOrTimeout(() =>
          billingRepository.updateCreationStatus(projectName, DeletionFailed, Option(t.getMessage))
        )
      case Success(result) =>
        Option(result.getJobReport).map(_.getStatus) match {
          case Some(JobReport.StatusEnum.RUNNING) => Future.successful(Incomplete)
          case Some(JobReport.StatusEnum.SUCCEEDED) =>
            billingProjectDeletion.finalizeDelete(projectName, ctx).map(_ => Complete)
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
