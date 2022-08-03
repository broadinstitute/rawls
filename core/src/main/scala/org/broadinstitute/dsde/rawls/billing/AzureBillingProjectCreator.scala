package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, ErrorReport, UserInfo}

import scala.concurrent.{ExecutionContext, Future}


class AzureBillingProjectCreator(billingRepository: BillingRepository, billingProfileManagerDAO: BillingProfileManagerDAO) extends BillingProjectCreator {
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo)
                                                    (implicit executionContext: ExecutionContext): Future[Unit] = {
    billingProfileManagerDAO.verifyAccess(createProjectRequest.billingInfo, userInfo)
  }

  /**
   * Verifies user access to the desired managed app coordinates and creates a billing profile against those coordinates
   */
  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo)
                                (implicit executionContext: ExecutionContext): Future[Unit] = {
    for {
      profileModel <- billingProfileManagerDAO.createBillingProfile(createProjectRequest.projectName.value,  createProjectRequest.billingInfo, userInfo)
      _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
    } yield {}
  }
}


class ManagedApplicationAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class InvalidCreationRequest(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
