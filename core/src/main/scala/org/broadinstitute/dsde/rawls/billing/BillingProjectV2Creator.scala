package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, CreationStatuses, ErrorReport, RawlsBillingProject, SamBillingProjectPolicyNames, SamBillingProjectRoles, SamPolicy, SamResourcePolicyName, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.user.UserService.syncBillingProjectOwnerPolicyToGoogleAndGetEmail
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ExecutionContext, Future}



trait BillingProjectV2Creator {
  def validateCreateProjectRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit]
  def postCreationAction(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit]
}




class GoogleBillingProjectCreator(samDAO: SamDAO, gcsDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext) extends BillingProjectV2Creator {
  override def validateCreateProjectRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      _ <- checkServicePerimeterAccess(createProjectRequest.servicePerimeter, userInfo)
      hasAccess <- gcsDAO.testBillingAccountAccess(createProjectRequest.billingAccount, userInfo)
      _ = if (!hasAccess) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Billing account does not exist, user does not have access, or Terra does not have access"))
      }
    } yield {}
  }

  override def postCreationAction(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit] = {
      samDAO
        .syncPolicyToGoogle(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, SamBillingProjectPolicyNames.owner)
        .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
  }

  private def checkServicePerimeterAccess(servicePerimeterOption: Option[ServicePerimeterName], userInfo: UserInfo): Future[Unit] = {
    servicePerimeterOption.map { servicePerimeter =>
      samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeter.value, UTF_8.name), SamServicePerimeterActions.addProject, userInfo).flatMap {
        case true => Future.successful(())
        case false => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, s"You do not have the action ${SamServicePerimeterActions.addProject.value} for $servicePerimeter")))
      }
    }.getOrElse(Future.successful(()))
  }

}
