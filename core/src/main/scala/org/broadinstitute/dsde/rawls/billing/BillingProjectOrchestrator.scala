package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, CreationStatuses, ErrorReport, RawlsBillingProject, SamBillingProjectPolicyNames, SamBillingProjectRoles, SamPolicy, SamResourcePolicyName, SamResourceTypeNames, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}


/**
 * Knows how to provision billing projects with external cloud providers (that is, implementors of the
 * BillingProjectCreator trait)
 */
class BillingProjectOrchestrator(samDAO: SamDAO, gcsDAO: GoogleServicesDAO, billingRepository: BillingRepository)(implicit val executionContext: ExecutionContext) {

  def createBillingProjectV2(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    val billingProjectCreator = createProjectRequest.billingInfo match {
      case Left(_) => new GoogleBillingProjectCreator(samDAO, gcsDAO)
      case Right(_) => ???
    }

    for {
      _ <- billingProjectCreator.validateBillingProjectCreationRequest(createProjectRequest, userInfo)
      result <- createV2BillingProjectInternal(createProjectRequest, userInfo)
      _ <- billingProjectCreator.postCreationSteps(createProjectRequest)
    } yield result
  }


  private def createV2BillingProjectInternal(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      maybeProject <- billingRepository.getBillingProject(createProjectRequest.projectName)
      _ <- maybeProject match {
        case Some(_) => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists")))
        case None => Future.successful(())
      }

      _ <- samDAO.createResourceFull(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo), Set.empty, userInfo, None)

      _ <- billingRepository.createBillingProject(RawlsBillingProject(createProjectRequest.projectName, CreationStatuses.Ready, createProjectRequest.billingAccount, None, None, createProjectRequest.servicePerimeter))

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
