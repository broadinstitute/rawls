package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
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
  RawlsRequestContext,
  SamResourceTypeNames,
  UserInfo
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

  /**
   * Validates that the desired billing account has granted Terra proper access as well as any needed service
   * perimeter access.
   * @return A successful future in the event of a passed validation, a failed future with an Exception in the event of
   *         validation failure.
   */
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                                     ctx: RawlsRequestContext
  ): Future[Unit] =
    for {
      _ <- ServicePerimeterService.checkServicePerimeterAccess(samDAO, createProjectRequest.servicePerimeter, ctx)
      hasAccess <- gcsDAO.testBillingAccountAccess(createProjectRequest.billingAccount.get, ctx.userInfo)
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
                                 ctx: RawlsRequestContext
  ): Future[CreationStatus] =
    for {
      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield CreationStatuses.Ready

  override def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[(Option[UUID], JobType)] =
    // Note: GoogleBillingProjectLifecycleSpec does not test that this method is called because the method
    // lives in a companion object (which makes straight mocking impossible), and the method will be removed
    // once workspace migration is complete. Note also that the more "integration" level test BillingApiServiceV2Spec
    // does verify that code in this method is executed when a Google-based project is deleted.
    deleteGoogleProjectIfChild(projectName, ctx.userInfo, gcsDAO, samDAO, ctx)
      .map(_ => (None, GoogleBillingProjectDelete))

  override def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] =
    unregisterBillingProject(projectName, ctx)

}
