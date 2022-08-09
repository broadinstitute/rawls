package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, ErrorReport, ErrorReportSource, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.user.UserService.syncBillingProjectOwnerPolicyToGoogleAndGetEmail

import java.net.URLEncoder
import scala.concurrent.{ExecutionContext, Future}
import java.nio.charset.StandardCharsets.UTF_8

class GoogleBillingProjectCreator(samDAO: SamDAO, gcsDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext) extends BillingProjectCreator {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      _ <- GoogleBillingProjectCreator.checkServicePerimeterAccess(createProjectRequest.servicePerimeter, samDAO, userInfo)
      hasAccess <- gcsDAO.testBillingAccountAccess(createProjectRequest.billingAccount.get, userInfo)
      _ = if (!hasAccess) {
        throw new GoogleBillingAccountAccessException(ErrorReport(StatusCodes.BadRequest, "Billing account does not exist, user does not have access, or Terra does not have access"))
      }
    } yield {}
  }


  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield {}
  }
}

object GoogleBillingProjectCreator {
  def checkServicePerimeterAccess(servicePerimeterOption: Option[ServicePerimeterName], samDAO: SamDAO, userInfo: UserInfo)(implicit executionContext: ExecutionContext): Future[Unit] = {
    servicePerimeterOption.map { servicePerimeter =>
      samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeter.value, UTF_8.name), SamServicePerimeterActions.addProject, userInfo).flatMap {
        case true => Future.successful(())
        case false => Future.failed(new ServicePerimeterAccessException(ErrorReport(StatusCodes.Forbidden, s"You do not have the action ${SamServicePerimeterActions.addProject.value} for $servicePerimeter")))
      }
    }.getOrElse(Future.successful(()))
  }
}
