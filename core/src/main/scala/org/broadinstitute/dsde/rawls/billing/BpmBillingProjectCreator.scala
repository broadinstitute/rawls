package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.HttpWorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport => RawlsErrorReport,
  RawlsRequestContext
}

import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}

/**
 * This class knows how to validate Rawls billing project requests and instantiate linked billing profiles in the
 * billing profile manager service.
 */
class BpmBillingProjectCreator(
  billingRepository: BillingRepository,
  billingProfileManagerDAO: BillingProfileManagerDAO,
  workspaceManagerDAO: HttpWorkspaceManagerDAO,
  resourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao
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
    val azureManagedAppCoordinates = createProjectRequest.billingInfo match {
      case Left(_)       => throw new NotImplementedError("Google billing accounts not supported in billing profiles")
      case Right(coords) => coords
    }

    val apps = blocking {
      billingProfileManagerDAO.listManagedApps(azureManagedAppCoordinates.subscriptionId, ctx)
    }

    apps.find(app =>
      app.getSubscriptionId == azureManagedAppCoordinates.subscriptionId &&
        app.getManagedResourceGroupId == azureManagedAppCoordinates.managedResourceGroupId &&
        app.getTenantId == azureManagedAppCoordinates.tenantId
    ) match {
      case None =>
        throw new ManagedAppNotFoundException(
          RawlsErrorReport(
            StatusCodes.Forbidden,
            s"Managed application not found [tenantId=${azureManagedAppCoordinates.tenantId}, subscriptionId=${azureManagedAppCoordinates.subscriptionId}, mrg_id=${azureManagedAppCoordinates.managedResourceGroupId}"
          )
        )
      case Some(_) => Future.successful()
    }
  }

  /**
   * Creates a billing profile with the given billing creation info and links the previously created billing project
   * with it
   */
  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                 config: MultiCloudWorkspaceConfig,
                                 ctx: RawlsRequestContext
  ): Future[CreationStatus] =
    try {
      val profileModel = blocking {
        billingProfileManagerDAO.createBillingProfile(createProjectRequest.projectName.value,
                                                      createProjectRequest.billingInfo,
                                                      ctx
        )
      }

      val landingZoneResponse = blocking {
        workspaceManagerDAO.createLandingZone(
          config.azureConfig.get.landingZoneDefinition,
          config.azureConfig.get.landingZoneVersion,
          profileModel.getId,
          ctx
        )
      }
      val errorReport = Option(landingZoneResponse.getErrorReport)
      errorReport match {
        case Some(errorReport) =>
          throw new LandingZoneCreationException(
            RawlsErrorReport(StatusCode.int2StatusCode(errorReport.getStatusCode), errorReport.getMessage)
          )
        case None => ()
      }
      for {
        _ <- resourceMonitorRecordDao.create(
          UUID.fromString(landingZoneResponse.getJobReport.getId),
          createProjectRequest.projectName.value
        )
        _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
      } yield CreationStatuses.CreatingLandingZone
    } catch {
      case exception: Exception => Future.failed(exception)
    }
}
