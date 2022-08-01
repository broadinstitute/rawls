package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, CreationStatuses, ErrorReport, ErrorReportSource, RawlsBillingProject, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.user.UserService.syncBillingProjectOwnerPolicyToGoogleAndGetEmail
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ExecutionContext, Future}

/**
 * Handles provisioning billing projects with external providers. Implementors of this trait are not concerned
 * with internal Rawls state (db records, etc.), but rather ensuring that
 * a) the creation request is valid for the given cloud provider
 * b) external state is valid after rawls internal state is updated (i.e, syncing groups, etc.)
 */
trait BillingProjectCreator {
  def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit]
  def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit]
}


class GoogleBillingProjectCreator(samDAO: SamDAO, gcsDAO: GoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends BillingProjectCreator with StringValidationUtils {
  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)
      _ <- GoogleBillingProjectCreator.checkServicePerimeterAccess(createProjectRequest.servicePerimeter, samDAO, userInfo)
      hasAccess <- gcsDAO.testBillingAccountAccess(createProjectRequest.billingAccount.get, userInfo)
      _ = if (!hasAccess) {
        throw new GoogleBillingAccountAccessException(ErrorReport(StatusCodes.BadRequest, "Billing account does not exist, user does not have access, or Terra does not have access"))
      }
    } yield {}
  }


  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit] = {
    for {
      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield {}
  }
}

object GoogleBillingProjectCreator {
  def checkServicePerimeterAccess(servicePerimeterOption: Option[ServicePerimeterName], samDAO: SamDAO, userInfo: UserInfo)(implicit executionContext: ExecutionContext) = {
    servicePerimeterOption.map { servicePerimeter =>
      samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeter.value, UTF_8.name), SamServicePerimeterActions.addProject, userInfo).flatMap {
        case true => Future.successful(())
        case false => Future.failed(new ServicePerimeterAccessException(ErrorReport(StatusCodes.Forbidden, s"You do not have the action ${SamServicePerimeterActions.addProject.value} for $servicePerimeter")))
      }
    }.getOrElse(Future.successful(()))
  }
}
