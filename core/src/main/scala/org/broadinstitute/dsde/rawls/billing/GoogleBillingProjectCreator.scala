package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport,
  ErrorReportSource,
  RawlsRequestContext
}
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService.syncBillingProjectOwnerPolicyToGoogleAndGetEmail

import scala.concurrent.{ExecutionContext, Future}

class GoogleBillingProjectCreator(samDAO: SamDAO, gcsDAO: GoogleServicesDAO)(implicit
  executionContext: ExecutionContext
) extends BillingProjectCreator {
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
}
