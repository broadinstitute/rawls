package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  GoogleProjectRegistration,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamGoogleProjectActions,
  SamResourceTypeNames
}

import scala.concurrent.{ExecutionContext, Future}

object GoogleProjectRegistrationService {
  def constructor(dataSource: SlickDataSource,
                  samDAO: SamDAO,
                  googleProjectRegRepository: GoogleProjectRegistrationRepository,
                  billingRepository: BillingRepository,
                  googleServicesDAO: GoogleServicesDAO
  )(
    ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): GoogleProjectRegistrationService =
    new GoogleProjectRegistrationService(ctx,
                                         dataSource,
                                         samDAO,
                                         googleProjectRegRepository,
                                         billingRepository,
                                         googleServicesDAO
    )

}

class GoogleProjectRegistrationService(protected val ctx: RawlsRequestContext,
                                       val dataSource: SlickDataSource,
                                       val samDAO: SamDAO,
                                       val googleProjectRegRepo: GoogleProjectRegistrationRepository,
                                       val billingRepository: BillingRepository,
                                       val googleServicesDAO: GoogleServicesDAO
)(implicit
  protected val executionContext: ExecutionContext
) {

  def registerGoogleProject(googleProjectReg: GoogleProjectRegistration): Future[GoogleProjectRegistration] =
    for {
      existingGoogleProjectReg <- googleProjectRegRepo.getGoogleProjectRegistration(googleProjectReg.googleProjectId)
      billingProject <- billingRepository.getBillingProject(googleProjectReg.billingProjectId).map {
        maybeBillingProject =>
          maybeBillingProject.getOrElse(
            throw new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(
                StatusCodes.NotFound,
                "Billing project does not exist or you do not have permission to perform this action."
              )
            )
          )
      }
      _ <- samDAO
        .listUserActionsForResource(SamResourceTypeNames.billingProject, billingProject.projectName.value, ctx)
        .map { actions =>
          if (actions.isEmpty || !actions.contains(SamBillingProjectActions.link))
            throw new RawlsExceptionWithErrorReport(
              errorReport =
                ErrorReport(StatusCodes.NotFound,
                            "Billing project does not exist or you do not have permission to perform this action."
                )
            )
        }
      canLinkGoogleProject <- samDAO
        .userHasAction(SamResourceTypeNames.googleProject,
                       googleProjectReg.googleProjectId.value,
                       SamGoogleProjectActions.link,
                       ctx
        )
      _ = if (!canLinkGoogleProject)
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
        )
      result <- existingGoogleProjectReg match {
        case Some(existing) if existing.billingProjectId == googleProjectReg.billingProjectId =>
          Future.successful(existing)
        case Some(existing) if existing.billingProjectId != googleProjectReg.billingProjectId =>
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.Conflict,
                                      "This google project id is already registered with a different billing project."
            )
          )
        case _ =>
          val updatedGoogleProjectReg = googleProjectReg.copy(billingAccount = billingProject.billingAccount)
          for {
            _ <- googleServicesDAO.setBillingAccountName(googleProjectReg.googleProjectId,
                                                         billingProject.billingAccount.get,
                                                         ctx.toTracingContext
            )
            registeredGoogleProject <- googleProjectRegRepo.registerGoogleProject(updatedGoogleProjectReg)
          } yield registeredGoogleProject
      }
    } yield result

}
