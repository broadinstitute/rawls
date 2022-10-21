package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.HttpWorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport => RawlsErrorReport,
  RawlsBillingProjectName,
  RawlsRequestContext
}

import java.util.UUID
import scala.concurrent.{blocking, ExecutionContext, Future}

/**
 * This class knows how to validate Rawls billing project requests and instantiate linked billing profiles in the
 * billing profile manager service.
 */
class BpmBillingProjectLifecycle(
  billingRepository: BillingRepository,
  billingProfileManagerDAO: BillingProfileManagerDAO,
  workspaceManagerDAO: HttpWorkspaceManagerDAO,
  resourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao
)(implicit val executionContext: ExecutionContext)
    extends BillingProjectLifecycle with LazyLogging {

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
        // This starts a landing zone creation job. There is a separate monitor that polls to see when it
        // completes and then updates the billing project status accordingly.
        workspaceManagerDAO.createLandingZone(
          config.azureConfig.get.landingZoneDefinition,
          config.azureConfig.get.landingZoneVersion,
          profileModel.getId,
          ctx
        )
      }
      Option(landingZoneResponse.getErrorReport) match {
        case Some(errorReport) =>
          throw new LandingZoneCreationException(
            RawlsErrorReport(StatusCode.int2StatusCode(errorReport.getStatusCode), errorReport.getMessage)
          )
        case None => ()
      }
      for {
        _ <- resourceMonitorRecordDao.create(
          UUID.fromString(landingZoneResponse.getJobReport.getId),
          JobType.AzureLandingZoneResult,
          createProjectRequest.projectName.value
        )
        _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
      } yield CreationStatuses.CreatingLandingZone
    } catch {
      case exception: Exception => Future.failed(exception)
    }

  override def preDeletionSteps(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext): Future[Unit] =
    for {
      _ <- billingRepository.getCreationStatus(projectName).flatMap {
        case CreationStatuses.CreatingLandingZone =>
          Future.failed(
            new BillingProjectDeletionException(
              RawlsErrorReport(
                s"Billing project ${projectName.value} cannot be deleted because its landing zone is still being created"
              )
            )
          )
        case _ => Future.successful()
      }
      _ <- billingRepository.getLandingZoneId(projectName).flatMap {
        case Some(landingZoneId) =>
          val landingZoneResponse = blocking {
            // Note that this actually just starts a landing zone deletion job (and thus returns quickly).
            // We are not attempting to ensure that the landing zone deletion completes successfully.
            workspaceManagerDAO.deleteLandingZone(UUID.fromString(landingZoneId), ctx)
          }
          Option(landingZoneResponse.getErrorReport) match {
            case Some(errorReport) =>
              throw new BillingProjectDeletionException(
                RawlsErrorReport(StatusCode.int2StatusCode(errorReport.getStatusCode), errorReport.getMessage)
              )
            case None => Future.successful()
          }
        case None =>
          logger.warn(s"Deleting BPM-backed billing project ${projectName}, but no associated landing zone to delete")
          Future.successful()
      }
    } yield {}
}
