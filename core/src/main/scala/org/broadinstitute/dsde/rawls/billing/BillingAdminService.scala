package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{
  BillingProjectAdminResponse,
  ErrorReport,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamResourceTypeAdminActions,
  SamResourceTypeNames
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import scala.concurrent.{ExecutionContext, Future}

class BillingAdminService(samDAO: SamDAO,
                          billingRepository: BillingRepository,
                          workspaceRepository: WorkspaceRepository,
                          ctx: RawlsRequestContext
)(implicit protected val ec: ExecutionContext)
    extends LazyLogging {

  def getBillingProject(billingProjectName: RawlsBillingProjectName): Future[BillingProjectAdminResponse] =
    for {
      userIsAdmin <- samDAO.admin.userHasResourceTypeAdminPermission(SamResourceTypeNames.billingProject,
                                                                     SamResourceTypeAdminActions.readSummaryInformation,
                                                                     ctx
      )
      _ = if (!userIsAdmin)
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.Forbidden, "You must be an admin to call this API.")
        )

      billingProjectOpt <- billingRepository.getBillingProject(billingProjectName)
      billingProject = billingProjectOpt.getOrElse(
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.NotFound, s"Billing project ${billingProjectName.value} not found.")
        )
      )
      workspaces <- workspaceRepository.listWorkspacesByBillingProject(billingProjectName)
    } yield BillingProjectAdminResponse(billingProject, workspaces.map(ws => (ws.name, ws.workspaceIdAsUUID)).toMap)
}
