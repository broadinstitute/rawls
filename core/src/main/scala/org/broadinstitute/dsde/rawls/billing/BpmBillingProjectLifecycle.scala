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
import scala.util.{Failure, Success, Try}

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
    extends BillingProjectLifecycle
    with LazyLogging {

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
      billingProfileManagerDAO.listManagedApps(azureManagedAppCoordinates.subscriptionId,
                                               includeAssignedApps = false,
                                               ctx
      )
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
  ): Future[CreationStatus] = {
    val projectName = createProjectRequest.projectName
    var profileModelId = None: Option[UUID]
    var landingZoneId = None: Option[UUID]
    try {
      val profileModel = blocking {
        billingProfileManagerDAO.createBillingProfile(projectName.value, createProjectRequest.billingInfo, ctx)
      }
      logger.info(
        s"Creating BPM-backed billing project ${projectName.value}, created profile with ID ${profileModel.getId}."
      )
      profileModelId = Some(profileModel.getId)

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
        case None =>
          landingZoneId = Some(landingZoneResponse.getLandingZoneId)
          logger.info(
            s"Creating BPM-backed billing project ${projectName.value}, initiated creation of landing zone ${landingZoneId.get} with jobId ${landingZoneResponse.getJobReport.getId}"
          )
      }
      (for {
        _ <- billingRepository.updateLandingZoneId(createProjectRequest.projectName, landingZoneId.get)
        _ <- resourceMonitorRecordDao.create(
          UUID.fromString(landingZoneResponse.getJobReport.getId),
          JobType.AzureLandingZoneResult,
          projectName.value
        )
        _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
      } yield CreationStatuses.CreatingLandingZone).recoverWith { case t: Throwable =>
        for {
          _ <- cleanupLandingZone(landingZoneId, projectName, ctx)
          _ <- cleanupBillingProfile(profileModelId, projectName, ctx)
        } yield throw t
      }
    } catch {
      case t: Throwable =>
        for {
          _ <- cleanupLandingZone(landingZoneId, projectName, ctx)
          _ <- cleanupBillingProfile(profileModelId, projectName, ctx)
        } yield throw t
    }
  }

  private def cleanupLandingZone(landingZoneId: Option[UUID],
                                 projectName: RawlsBillingProjectName,
                                 ctx: RawlsRequestContext
  ): Future[Unit] =
    landingZoneId match {
      case Some(id) =>
        val landingZoneResponse = Try(blocking {
          // Note that this actually just starts a landing zone deletion job (and thus returns quickly).
          // We are not attempting to ensure that the landing zone deletion completes successfully.
          workspaceManagerDAO.deleteLandingZone(id, ctx)
        })

        landingZoneResponse match {
          case Success(response) =>
            Option(response) match {
              case Some(result) =>
                Option(result.getErrorReport) match {
                  case Some(errorReport) =>
                    logger.warn(
                      s"Unable to delete landing zone with ID ${id} for BPM-backed billing project ${projectName.value}: ${errorReport.getMessage}."
                    )
                  case None =>
                    logger.info(
                      s"Initiated deletion of landing zone ${landingZoneId} for BPM-backed billing project ${projectName.value}."
                    )
                }
              case None =>
                logger.warn(
                  s"Unable to delete landing zone with ID ${id} for BPM-backed billing project ${projectName.value}, no landingZoneResponse from WSM."
                )
            }
          case Failure(e) =>
            logger.warn(
              s"Unable to delete landing zone with ID ${id} for BPM-backed billing project ${projectName.value}.",
              e
            )
        }
        Future.successful()
      case None =>
        logger.info(s"Deleting BPM-backed billing project ${projectName.value}, no associated landing zone to delete.")
        Future.successful()
    }

  private def cleanupBillingProfile(profileModelId: Option[UUID],
                                    projectName: RawlsBillingProjectName,
                                    ctx: RawlsRequestContext
  ): Future[Unit] =
    profileModelId match {
      case Some(id) =>
        val numOtherProjectsWithProfile = for {
          allProjectsWithProfile <- billingRepository
            .getBillingProjectsWithProfile(Some(id))
          filtered = allProjectsWithProfile.filterNot(_.projectName == projectName)
        } yield filtered.length
        numOtherProjectsWithProfile map {
          case 0 =>
            logger.info(
              s"Deleting BPM-backed billing project ${projectName.value}, deleting billing profile record ${id}"
            )
            billingProfileManagerDAO.deleteBillingProfile(id, ctx)
          case num =>
            logger.info(
              s"Deleting BPM-backed billing project ${projectName.value}, but not deleting billing profile record ${id} because ${num} other project(s) reference it"
            )
        }
      case None =>
        logger.info(
          s"Deleting BPM-backed billing project ${projectName.value}, no associated billing profile record to delete"
        )
        Future.successful()
    }

  override def preDeletionSteps(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext): Future[Unit] =
    for {
      _ <- billingRepository.getCreationStatus(projectName).map {
        case CreationStatuses.CreatingLandingZone =>
          throw new BillingProjectDeletionException(
            RawlsErrorReport(
              s"Billing project ${projectName.value} cannot be deleted because its landing zone is still being created"
            )
          )
        case _ => ()
      }
      _ <- billingRepository.getLandingZoneId(projectName).flatMap {
        case Some(landingZoneId) =>
          cleanupLandingZone(Some(UUID.fromString(landingZoneId)), projectName, ctx)
        case None =>
          logger.warn(s"Deleting BPM-backed billing project ${projectName}, but no associated landing zone to delete")
          Future.successful()
      }
      _ <- billingRepository.getBillingProfileId(projectName).flatMap {
        case Some(billingProfileId) =>
          cleanupBillingProfile(Some(UUID.fromString(billingProfileId)), projectName, ctx)
        case None =>
          logger.warn(
            s"Deleting BPM-backed billing project ${projectName}, but no associated billing profile record to delete"
          )
          Future.successful()
      }
    } yield {}
}
