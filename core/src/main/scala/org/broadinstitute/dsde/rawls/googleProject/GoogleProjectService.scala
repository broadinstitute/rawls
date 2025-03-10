package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{ErrorReportSource, RawlsGoogleProject, RawlsRequestContext}

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
  import dataSource.dataAccess.driver.api._
//  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def createGoogleProject(googleProject: RawlsGoogleProject): Future[RawlsGoogleProject] =
    // TODO validation
    googleProjectRepository.createGoogleProject(googleProject)

}
