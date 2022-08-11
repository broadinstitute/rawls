package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{CreateRawlsV2BillingProjectFullRequest, ErrorReport, UserInfo}

import scala.concurrent.{ExecutionContext, Future, blocking}


/**
 * This class knows how to validate Rawls billing project requests and instantiate linked billing profiles in the
 * billing profile manager service.
 */
class BpmBillingProjectCreator(billingRepository: BillingRepository,
                               billingProfileManagerDAO: BillingProfileManagerDAO)
                              (implicit val executionContext: ExecutionContext) extends BillingProjectCreator {

  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                                     userInfo: UserInfo): Future[Unit] = {
    val azureManagedAppCoordinates = createProjectRequest.billingInfo match {
      case Left(_) => throw new NotImplementedError("Google billing accounts not supported in billing profiles")
      case Right(coords) => coords
    }

    for {
      apps <- blocking {
        billingProfileManagerDAO.listManagedApps(azureManagedAppCoordinates.subscriptionId, userInfo)
      }
      _ = apps.find(app => app.getSubscriptionId == azureManagedAppCoordinates.subscriptionId &&
        app.getManagedResourceGroupId == azureManagedAppCoordinates.managedResourceGroupId &&
        app.getTenantId == azureManagedAppCoordinates.tenantId
      ) match {
        case None => throw new ManagedAppNotFoundException(ErrorReport(StatusCodes.Forbidden, "Managed application not found"))
        case Some(_) => ()
      }
    } yield {}
  }

  /**
   * Creates a billing profile with the given billing creation info and links the previously created billing project
   * with it
   */
  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest, userInfo: UserInfo): Future[Unit] = {
    for {
      profileModel <- blocking {
        billingProfileManagerDAO.createBillingProfile(createProjectRequest.projectName.value, createProjectRequest.billingInfo, userInfo)
      }
      _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
    } yield {}
  }

}


class ManagedApplicationAccessException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)

class InvalidCreationRequest(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
