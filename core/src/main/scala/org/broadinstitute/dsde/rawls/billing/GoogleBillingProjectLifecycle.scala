package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{
  GoogleBillingProjectDelete,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport,
  ErrorReportSource,
  RawlsBillingProjectName,
  RawlsRequestContext
}
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService.{
  deleteGoogleProjectIfChild,
  syncBillingProjectOwnerPolicyToGoogleAndGetEmail
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class GoogleBillingProjectLifecycle(
  val billingRepository: BillingRepository,
  val samDAO: SamDAO,
  gcsDAO: GoogleServicesDAO
)(implicit
  executionContext: ExecutionContext
) extends BillingProjectLifecycle {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  override val deleteJobType: JobType = GoogleBillingProjectDelete

  /**
   * Validates that the desired billing account has granted Terra proper access as well as any needed service
   * perimeter access.
   * @return A successful future in the event of a passed validation, a failed future with an Exception in the event of
   *         validation failure.
   */
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                                     billingProfileManagerDAO: BillingProfileManagerDAO,
                                                     ctx: RawlsRequestContext
  ): Future[Unit] =
    for {
      _ <- ServicePerimeterService.checkServicePerimeterAccess(samDAO, createProjectRequest.servicePerimeter, ctx)
      hasAccess <- gcsDAO.testTerraAndUserBillingAccountAccess(createProjectRequest.billingAccount.get, ctx.userInfo)
      _ = if (!hasAccess) {
        throw new GoogleBillingAccountAccessException(
          ErrorReport(StatusCodes.BadRequest,
                      "Billing account does not exist, user does not have access, or Terra does not have access"
          )
        )
      }
    } yield {}

  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                 config: MultiCloudWorkspaceConfig,
                                 billingProfileManagerDAO: BillingProfileManagerDAO,
                                 ctx: RawlsRequestContext
  ): Future[CreationStatus] =
    for {
      profileModel <- createBillingProfile(createProjectRequest, billingProfileManagerDAO, ctx)
      _ <- addMembersToBillingProfile(profileModel, createProjectRequest, billingProfileManagerDAO, ctx)
      _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield CreationStatuses.Ready

  override def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Option[UUID]] =
    // Note: GoogleBillingProjectLifecycleSpec does not test that this method is called because the method
    // lives in a companion object (which makes straight mocking impossible), and the method will be removed
    // once workspace migration is complete. Note also that the more "integration" level test BillingApiServiceV2Spec
    // does verify that code in this method is executed when a Google-based project is deleted.
    deleteGoogleProjectIfChild(projectName, ctx.userInfo, gcsDAO, samDAO, ctx).map(_ => None)

  override def finalizeDelete(projectName: RawlsBillingProjectName,
                              billingProfileManagerDAO: BillingProfileManagerDAO,
                              ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): Future[Unit] =
    // Should change this to expecting a billing profile once we have migrated all billing projects (WOR-866)
    deleteBillingProfileAndUnregisterBillingProject(projectName,
                                                    billingProfileExpected = false,
                                                    billingProfileManagerDAO,
                                                    ctx
    )
}
