package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, CreationStatuses, ErrorReport, ErrorReportSource, RawlsBillingProject, SamBillingProjectPolicyNames, SamBillingProjectRoles, SamPolicy, SamResourcePolicyName, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.user.UserService.syncBillingProjectOwnerPolicyToGoogleAndGetEmail
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ExecutionContext, Future}


/**
 * Knows how to provision billing projects with external cloud providers
 */
class BillingProjectOrchestrator(samDAO: SamDAO, gcsDAO: GoogleServicesDAO, billingRepository: BillingRepository)(implicit val executionContext: ExecutionContext) extends StringValidationUtils {
  implicit val errorReportSource = ErrorReportSource("rawls")

  def createBillingProjectV2(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)
      _ <- checkServicePerimeterAccess(createProjectRequest.servicePerimeter, userInfo)
      hasAccess <- gcsDAO.testBillingAccountAccess(createProjectRequest.billingAccount, userInfo)
      _ = if (!hasAccess) {
        throw new GoogleBillingAccountAccessException(ErrorReport(StatusCodes.BadRequest, "Billing account does not exist, user does not have access, or Terra does not have access"))
      }
      result <- createV2BillingProjectInternal(createProjectRequest, userInfo)
    } yield result
  }

  def checkServicePerimeterAccess(servicePerimeterOption: Option[ServicePerimeterName], userInfo: UserInfo): Future[Unit] = {
    servicePerimeterOption.map { servicePerimeter =>
      samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeter.value, UTF_8.name), SamServicePerimeterActions.addProject, userInfo).flatMap {
        case true => Future.successful(())
        case false => Future.failed(new ServicePerimeterAccessException(ErrorReport(StatusCodes.Forbidden, s"You do not have the action ${SamServicePerimeterActions.addProject.value} for $servicePerimeter")))
      }
    }.getOrElse(Future.successful(()))
  }

  private def createV2BillingProjectInternal(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      maybeProject <- billingRepository.getBillingProject(createProjectRequest.projectName)
      _ <- maybeProject match {
        case Some(_) => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists")))
        case None => Future.successful(())
      }

      _ <- samDAO.createResourceFull(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo), Set.empty, userInfo, None)

      _ <- billingRepository.createBillingProject(RawlsBillingProject(createProjectRequest.projectName, CreationStatuses.Ready, Option(createProjectRequest.billingAccount), None, None, createProjectRequest.servicePerimeter))

      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield {}
  }
}

object BillingProjectOrchestrator {
  def defaultBillingProjectPolicies(userInfo: UserInfo): Map[SamResourcePolicyName, SamPolicy] = {
    Map(
      SamBillingProjectPolicyNames.owner -> SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set(SamBillingProjectRoles.owner)),
      SamBillingProjectPolicyNames.workspaceCreator -> SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator))
    )
  }
}

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
