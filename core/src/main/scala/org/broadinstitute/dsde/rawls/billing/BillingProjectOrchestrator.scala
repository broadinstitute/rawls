package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, CreationStatuses, ErrorReport, ErrorReportSource, RawlsBillingProject, SamBillingProjectPolicyNames, SamBillingProjectRoles, SamPolicy, SamResourcePolicyName, SamResourceTypeNames, SamServicePerimeterActions, ServicePerimeterName, UserInfo}
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService.syncBillingProjectOwnerPolicyToGoogleAndGetEmail
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.{ExecutionContext, Future}


/**
 * Knows how to provision billing projects with external cloud providers (that is, implementors of the
 * BillingProjectCreator trait)
 */
class BillingProjectOrchestrator(userInfo: UserInfo,
                                 samDAO: SamDAO,
                                 gcsDAO: GoogleServicesDAO,
                                 billingRepository: BillingRepository,
                                 billingProfileManagerDAO: BillingProfileManagerDAO)
                                (implicit val executionContext: ExecutionContext) {
  implicit val errorReportSource = ErrorReportSource("rawls")

  /**
   * Creates a "v2" billing project, using either Azure managed app coordinates or a Google Billing Account
   */
  def createBillingProjectV2(createProjectRequest: CreateRawlsV2BillingProjectFullRequest): Future[Unit] = {
    val billingProjectCreator = createProjectRequest.billingInfo match {
      case Left(_) => new GoogleBillingProjectCreator(samDAO, gcsDAO)
      case Right(_) => new AzureBillingProjectCreator(billingRepository, billingProfileManagerDAO)
    }

    for {
      _ <- billingProjectCreator.validateBillingProjectCreationRequest(createProjectRequest, userInfo)
      result <- createV2BillingProjectInternal(createProjectRequest, userInfo)
      _ <- billingProjectCreator.postCreationSteps(createProjectRequest, userInfo)
    } yield result
  }


  private def createV2BillingProjectInternal(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      maybeProject <- billingRepository.getBillingProject(createProjectRequest.projectName)
      _ <- maybeProject match {
        case Some(_) => Future.failed(new DuplicateBillingProjectException(ErrorReport(StatusCodes.Conflict, "project by that name already exists")))
        case None => Future.successful(())
      }
      _ <- samDAO.createResourceFull(SamResourceTypeNames.billingProject,
        createProjectRequest.projectName.value,
        BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo),
        Set.empty,
        userInfo,
        None)
      _ <- billingRepository.createBillingProject(
        RawlsBillingProject(createProjectRequest.projectName,
          CreationStatuses.Ready,
          createProjectRequest.billingAccount,
          None,
          None,
          createProjectRequest.servicePerimeter))
    } yield {}
  }
}

object BillingProjectOrchestrator {
  def constructor(samDAO: SamDAO, gcsDAO: GoogleServicesDAO, billingRepository: BillingRepository, billingProfileManagerDAO: BillingProfileManagerDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext): BillingProjectOrchestrator = {
    new BillingProjectOrchestrator(userInfo, samDAO, gcsDAO, billingRepository, billingProfileManagerDAO)
  }

  def defaultBillingProjectPolicies(userInfo: UserInfo): Map[SamResourcePolicyName, SamPolicy] = {
    Map(
      SamBillingProjectPolicyNames.owner -> SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set(SamBillingProjectRoles.owner)),
      SamBillingProjectPolicyNames.workspaceCreator -> SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator))
    )
  }
}

class DuplicateBillingProjectException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class ServicePerimeterAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class GoogleBillingAccountAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
