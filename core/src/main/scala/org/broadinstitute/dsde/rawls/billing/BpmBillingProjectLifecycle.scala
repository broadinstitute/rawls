package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.model.CreateLandingZoneResult
import cats.implicits.{catsSyntaxFlatMapOps, toTraverseOps}
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

    def createBillingProfile: Future[ProfileModel] =
      Future(blocking {
        val profileModel = billingProfileManagerDAO.createBillingProfile(
          projectName.value,
          createProjectRequest.billingInfo,
          ctx
        )
        logger.info(
          s"Creating BPM-backed billing project ${projectName.value}, created profile with ID ${profileModel.getId}."
        )
        profileModel
      })

    // This starts a landing zone creation job. There is a separate monitor that polls to see when it
    // completes and then updates the billing project status accordingly.
    def createLandingZone(profileModel: ProfileModel): Future[CreateLandingZoneResult] =
      Future(blocking {
        workspaceManagerDAO.createLandingZone(
          config.azureConfig.get.landingZoneDefinition,
          config.azureConfig.get.landingZoneVersion,
          profileModel.getId,
          ctx
        )
      })

    createBillingProfile.flatMap { profileModel =>
      createLandingZone(profileModel)
        .flatMap { landingZone =>
          (for {
            _ <- Option(landingZone.getErrorReport).traverse { errorReport =>
              Future.failed(
                new LandingZoneCreationException(
                  RawlsErrorReport(StatusCode.int2StatusCode(errorReport.getStatusCode), errorReport.getMessage)
                )
              )
            }
            jobReport = Option(landingZone.getJobReport).getOrElse(
              throw new LandingZoneCreationException(
                RawlsErrorReport(InternalServerError, "CreateLandingZoneResult is missing the JobReport.")
              )
            )
            _ = logger.info(
              s"Initiated creation of landing zone ${landingZone.getLandingZoneId} with jobId ${jobReport.getId}"
            )
            _ <- billingRepository.updateLandingZoneId(createProjectRequest.projectName, landingZone.getLandingZoneId)
            _ <- resourceMonitorRecordDao.create(
              UUID.fromString(jobReport.getId),
              JobType.AzureLandingZoneResult,
              projectName.value,
              ctx.userInfo.userEmail
            )
            _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
          } yield CreationStatuses.CreatingLandingZone).recoverWith { case t: Throwable =>
            logger.error("Billing project creation failed, cleaning up landing zone", t)
            cleanupLandingZone(landingZone.getLandingZoneId, projectName, ctx) >> Future.failed(t)
          }
        }
        .recoverWith { case t: Throwable =>
          logger.error("Billing project creation failed, cleaning up billing profile", t)
          cleanupBillingProfile(profileModel.getId, projectName, ctx) >> Future.failed(t)
        }
    }
  }

  private def cleanupLandingZone(landingZoneId: UUID,
                                 projectName: RawlsBillingProjectName,
                                 ctx: RawlsRequestContext
  ): Future[Unit] =
    (for {
      // Note that this actually just starts a landing zone deletion job (and thus returns quickly).
      // We are not attempting to ensure that the landing zone deletion completes successfully.
      landingZoneResponse <- Future(blocking {
        val response = workspaceManagerDAO.deleteLandingZone(landingZoneId, ctx)
        logger.info(
          s"Initiated deletion of landing zone $landingZoneId for BPM-backed billing project ${projectName.value}."
        )
        response
      })

      _ = Option(landingZoneResponse.getErrorReport).map { errorReport =>
        logger.warn(
          s"Unable to delete landing zone with ID $landingZoneId for BPM-backed " +
            s"billing project ${projectName.value}: ${errorReport.getMessage}."
        )
      }
    } yield ()).recover { case t: Throwable =>
      logger.warn(
        s"Unable to delete landing zone with ID $landingZoneId for BPM-backed billing project ${projectName.value}.",
        t
      )
    }

  private def cleanupBillingProfile(profileModelId: UUID,
                                    projectName: RawlsBillingProjectName,
                                    ctx: RawlsRequestContext
  ): Future[Unit] = {
    val numOtherProjectsWithProfile = for {
      allProjectsWithProfile <- billingRepository
        .getBillingProjectsWithProfile(Some(profileModelId))
      filtered = allProjectsWithProfile.filterNot(_.projectName == projectName)
    } yield filtered.length
    numOtherProjectsWithProfile map {
      case 0 =>
        logger.info(
          s"Deleting BPM-backed billing project ${projectName.value}, deleting billing profile record ${profileModelId}"
        )
        billingProfileManagerDAO.deleteBillingProfile(profileModelId, ctx)
      case num =>
        logger.info(
          s"Deleting BPM-backed billing project ${projectName.value}, but not deleting billing profile record ${profileModelId} because ${num} other project(s) reference it"
        )
    }
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
          cleanupLandingZone(UUID.fromString(landingZoneId), projectName, ctx)
        case None =>
          logger.warn(s"Deleting BPM-backed billing project ${projectName}, but no associated landing zone to delete")
          Future.successful()
      }
      _ <- billingRepository.getBillingProfileId(projectName).flatMap {
        case Some(billingProfileId) =>
          cleanupBillingProfile(UUID.fromString(billingProfileId), projectName, ctx)
        case None =>
          logger.warn(
            s"Deleting BPM-backed billing project ${projectName}, but no associated billing profile record to delete"
          )
          Future.successful()
      }
    } yield {}
}
