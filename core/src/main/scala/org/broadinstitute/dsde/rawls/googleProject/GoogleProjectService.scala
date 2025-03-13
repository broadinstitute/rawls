package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsBillingAccountName,
  RawlsGoogleProject,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamGoogleProjectActions,
  SamResourceTypeNames
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object GoogleProjectService {
  def constructor(dataSource: SlickDataSource,
                  samDAO: SamDAO,
                  googleProjectRepository: GoogleProjectRepository,
                  billingRepository: BillingRepository,
                  googleServicesDAO: GoogleServicesDAO
  )(
    ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): GoogleProjectService =
    new GoogleProjectService(ctx, dataSource, samDAO, googleProjectRepository, billingRepository, googleServicesDAO)

}

class GoogleProjectService(protected val ctx: RawlsRequestContext,
                           val dataSource: SlickDataSource,
                           val samDAO: SamDAO,
                           val googleProjectRepository: GoogleProjectRepository,
                           val billingRepository: BillingRepository,
                           val googleServicesDAO: GoogleServicesDAO
)(implicit
  protected val executionContext: ExecutionContext
) {

  def createGoogleProject(googleProject: RawlsGoogleProject): Future[RawlsGoogleProject] =
    for {
      _ <- googleProjectRepository.getGoogleProject(googleProject.googleProjectId).map {
        case Some(_) =>
          throw new RawlsExceptionWithErrorReport(errorReport =
            ErrorReport(StatusCodes.Conflict, "Google project is already registered.")
          )
        case None =>
      }
      billingProject <- billingRepository.getBillingProject(googleProject.billingProjectId).map { maybeBillingProject =>
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
          if (actions.isEmpty)
            throw new RawlsExceptionWithErrorReport(
              errorReport =
                ErrorReport(StatusCodes.NotFound,
                            "Billing project does not exist or you do not have permission to perform this action."
                )
            )
          else if (!actions.contains(SamBillingProjectActions.link))
            throw new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
            )
        }
      canLinkGoogleProject <- samDAO
        .userHasAction(SamResourceTypeNames.googleProject,
                       googleProject.googleProjectId.value,
                       SamGoogleProjectActions.link,
                       ctx
        )
      _ = if (!canLinkGoogleProject)
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
        )
      updatedGoogleProject = googleProject.copy(billingAccount = billingProject.billingAccount)
      _ <- googleServicesDAO.setBillingAccountName(googleProject.googleProjectId,
                                                   billingProject.billingAccount.get,
                                                   ctx.toTracingContext
      )
      result <- googleProjectRepository.createGoogleProject(updatedGoogleProject)
    } yield result

}
