package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsGoogleProject,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamGoogleProjectActions,
  SamResourceTypeNames
}

import scala.concurrent.{ExecutionContext, Future}

object GoogleProjectService {
  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, googleProjectRepository: GoogleProjectRepository)(
    ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): GoogleProjectService =
    new GoogleProjectService(ctx, dataSource, samDAO, googleProjectRepository)

}

class GoogleProjectService(protected val ctx: RawlsRequestContext,
                           val dataSource: SlickDataSource,
                           val samDAO: SamDAO,
                           val googleProjectRepository: GoogleProjectRepository
)(implicit
  protected val executionContext: ExecutionContext
) {

  def createGoogleProject(googleProject: RawlsGoogleProject): Future[RawlsGoogleProject] =
    for {
      canLinkBillingProject <- samDAO
        .userHasAction(SamResourceTypeNames.billingProject,
                       googleProject.billingProjectId,
                       SamBillingProjectActions.link,
                       ctx
        )
      _ <-
        if (canLinkBillingProject) Future.successful(())
        else
          Future.failed(
            new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have permission to perform this action.")
            )
          )
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
