package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
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
                  billingRepository: BillingRepository
  )(
    ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): GoogleProjectService =
    new GoogleProjectService(ctx, dataSource, samDAO, googleProjectRepository, billingRepository)

}

class GoogleProjectService(protected val ctx: RawlsRequestContext,
                           val dataSource: SlickDataSource,
                           val samDAO: SamDAO,
                           val googleProjectRepository: GoogleProjectRepository,
                           val billingRepository: BillingRepository
)(implicit
  protected val executionContext: ExecutionContext
) {

  def createGoogleProject(googleProject: RawlsGoogleProject): Future[RawlsGoogleProject] =
    for {
      doesBillingProjectExist <- billingRepository.getBillingProject(googleProject.billingProjectId)
      _ <- doesBillingProjectExist match {
        case Some(project) =>
          samDAO
            .listUserActionsForResource(SamResourceTypeNames.billingProject, project.projectName.value, ctx)
            .flatMap { actions =>
              if (actions.nonEmpty) {
                if (actions.contains(SamBillingProjectActions.link)) Future.successful(())
                else
                  Future.failed(
                    new RawlsExceptionWithErrorReport(
                      errorReport =
                        ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
                    )
                  )
              } else {
                Future.failed(
                  new RawlsExceptionWithErrorReport(
                    errorReport =
                      ErrorReport(StatusCodes.Forbidden,
                                  "Billing project does not exist or you do not have permission to perform this action."
                      )
                  )
                )
              }
            }
        case None =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              errorReport =
                ErrorReport(StatusCodes.Forbidden,
                            "Billing project does not exist or you do not have permission to perform this action."
                )
            )
          )
      }
      canLinkGoogleProject <- samDAO
        .userHasAction(SamResourceTypeNames.googleProject,
                       googleProject.googleProjectId,
                       SamGoogleProjectActions.link,
                       ctx
        )
      _ <-
        if (canLinkGoogleProject) Future.successful(())
        else
          Future.failed(
            new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
            )
          )
      result <- googleProjectRepository.createGoogleProject(googleProject)
    } yield result

}
