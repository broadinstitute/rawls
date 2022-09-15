package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreateRawlsV2BillingProjectFullRequest, ErrorReport, RawlsRequestContext, UserInfo}

import scala.concurrent.{ExecutionContext, Future, blocking}

/**
 * This class knows how to validate Rawls billing project requests and instantiate linked billing profiles in the
 * billing profile manager service.
 */
class BpmBillingProjectCreator(billingRepository: BillingRepository,
                               billingProfileManagerDAO: BillingProfileManagerDAO,
                               wsmDAO: WorkspaceManagerDAO
)(implicit val executionContext: ExecutionContext)
    extends BillingProjectCreator {

  /**
   * Validates that the desired azure managed application access.
   * @return A successful future in the event of a passed validation, a failed future with an ManagedAppNotFoundException
   *         in the event of validation failure.
   */
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                                     ctx: RawlsRequestContext
  ): Future[Unit] = {
    val azureManagedAppCoordinates: AzureManagedAppCoordinates = getRequiredManagedAppCoords(createProjectRequest)

    for {
      apps <- blocking {
        billingProfileManagerDAO.listManagedApps(azureManagedAppCoordinates.subscriptionId, ctx)
      }
      _ = apps.find(app =>
        app.getSubscriptionId == azureManagedAppCoordinates.subscriptionId &&
          app.getManagedResourceGroupId == azureManagedAppCoordinates.managedResourceGroupId &&
          app.getTenantId == azureManagedAppCoordinates.tenantId
      ) match {
        case None =>
          throw new ManagedAppNotFoundException(
            ErrorReport(
              StatusCodes.Forbidden,
              s"Managed application not found [tenantId=${azureManagedAppCoordinates.tenantId}, subscriptionId=${azureManagedAppCoordinates.subscriptionId}, mrg_id=${azureManagedAppCoordinates.managedResourceGroupId}"
            )
          )
        case Some(_) => ()
      }
    } yield {}
  }

  private def getRequiredManagedAppCoords(createProjectRequest: CreateRawlsV2BillingProjectFullRequest) = {
    val azureManagedAppCoordinates = createProjectRequest.billingInfo match {
      case Left(_) => throw new NotImplementedError("Google billing accounts not supported in billing profiles")
      case Right(coords) => coords
    }
    azureManagedAppCoordinates
  }

  /**
   * Creates a billing profile with the given billing creation info and links the previously created billing project
   * with it
   */
  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                 ctx: RawlsRequestContext
  ): Future[Unit] = {
    val managedAppCoordinates = getRequiredManagedAppCoords(createProjectRequest)
    for {
      profileModel <- blocking {
        billingProfileManagerDAO.createBillingProfile(createProjectRequest.projectName.value,
                                                      createProjectRequest.billingInfo,
                                                      ctx
        )
      }
      lzResult <- Future {
        wsmDAO.createLandingZone(managedAppCoordinates, ctx)
      }.recoverWith { case t: Throwable =>
        // TODO should we create the LZ first?
        billingProfileManagerDAO.deleteBillingProfile(profileModel.getId, ctx).map(throw t)
      }
      _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
      _ <- billingRepository.setLandingZoneJobControlId(lzResult.getJobReport.getId)
      // TODO billing project should not be "ready" but "creating" or "waiting_on_lz" at this stage
    } yield {}
  }

}
