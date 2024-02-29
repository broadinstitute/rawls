package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{CreateLandingZoneResult, DeleteAzureLandingZoneResult}
import cats.implicits.{catsSyntaxFlatMapOps, toTraverseOps}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{
  BpmBillingProjectDelete,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, WorkspaceManagerResourceMonitorRecordDao}
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
class AzureBillingProjectLifecycle(
  val samDAO: SamDAO,
  val billingRepository: BillingRepository,
  val billingProfileManagerDAO: BillingProfileManagerDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  resourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao
)(implicit val executionContext: ExecutionContext)
    extends BillingProjectLifecycle {

  override val deleteJobType: JobType = BpmBillingProjectDelete

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
                                 billingProjectDeletion: BillingProjectDeletion,
                                 ctx: RawlsRequestContext
  ): Future[CreationStatus] = {
    val projectName = createProjectRequest.projectName

    // This starts a landing zone creation job. There is a separate monitor that polls to see when it
    // completes and then updates the billing project status accordingly.
    def createLandingZone(profileModel: ProfileModel): Future[CreateLandingZoneResult] = {
      val lzId = createProjectRequest.managedAppCoordinates.get.landingZoneId
      if (lzId.isDefined && !config.azureConfig.landingZoneAllowAttach) {
        throw new LandingZoneCreationException(
          RawlsErrorReport("Landing Zone ID provided but attachment is not permitted in this environment")
        )
      }

      val maybeAttach = if (lzId.isDefined) Map("attach" -> "true") else Map.empty
      val costSavingParams =
        if (createProjectRequest.costSavings.contains(true)) config.azureConfig.costSavingLandingZoneParameters
        else Map.empty
      val params = config.azureConfig.landingZoneParameters ++ maybeAttach ++ costSavingParams
      val lzDefinition = createProjectRequest.protectedData match {
        case Some(true) => config.azureConfig.protectedDataLandingZoneDefinition
        case _          => config.azureConfig.landingZoneDefinition
      }

      Future(blocking {
        workspaceManagerDAO.createLandingZone(
          lzDefinition,
          config.azureConfig.landingZoneVersion,
          params,
          profileModel.getId,
          ctx,
          lzId
        )
      })
    }

    createBillingProfile(createProjectRequest, ctx).flatMap { profileModel =>
      addMembersToBillingProfile(profileModel, createProjectRequest, ctx).flatMap { _ =>
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
              _ <- billingRepository.updateLandingZoneId(createProjectRequest.projectName,
                                                         Option(landingZone.getLandingZoneId)
              )
              _ <- resourceMonitorRecordDao.create(
                WorkspaceManagerResourceMonitorRecord.forAzureLandingZoneCreate(
                  UUID.fromString(jobReport.getId),
                  projectName,
                  ctx.userInfo.userEmail
                )
              )
              _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, profileModel.getId)
            } yield CreationStatuses.CreatingLandingZone).recoverWith { case t: Throwable =>
              Option(landingZone.getLandingZoneId) match {
                case Some(landingZoneId) =>
                  logger.error("Billing project creation failed, cleaning up landing zone")
                  cleanupLandingZone(landingZoneId, ctx)
                  throw t
                case _ =>
                  logger.error("Billing project creation failed, no landing zone to clean up")
                  throw t
              }
            }
          }
          .recoverWith { case t: Throwable =>
            logger.error("Billing project creation failed, cleaning up billing profile", t)
            billingProjectDeletion.cleanupBillingProfile(profileModel.getId, projectName, ctx).recover {
              case cleanupError: Throwable =>
                // Log the exception that prevented cleanup from completing, but do not throw it so original
                // cause of billing project failure is shown to user.
                logger.warn(
                  s"Unable to delete billing profile with ID ${profileModel.getId} for BPM-backed billing project ${projectName.value}.",
                  cleanupError
                )
            } >> Future.failed(t)
          }
      }
    }
  }

  /**
    *  starts a landing zone deletion job
    *  does not ensure that the landing zone deletion completes successfully.
    */
  private def cleanupLandingZone(
    landingZoneId: UUID,
    ctx: RawlsRequestContext
  ): Option[DeleteAzureLandingZoneResult] = Try(workspaceManagerDAO.deleteLandingZone(landingZoneId, ctx)) match {
    case Failure(e: ApiException) =>
      val msg = s"Unable to delete landing zone: ${e.getMessage}"
      throw new LandingZoneDeletionException(RawlsErrorReport(StatusCode.int2StatusCode(e.getCode), msg, e))
    case Failure(t) =>
      logger.warn(s"Unable to delete landing zone with ID $landingZoneId for BPM-backed billing project.", t)
      throw new LandingZoneDeletionException(RawlsErrorReport(t))
    case Success(None) => None
    case Success(Some(landingZoneResponse)) =>
      logger.info(
        s"Initiated deletion of landing zone $landingZoneId for BPM-backed billing project."
      )
      Option(landingZoneResponse.getErrorReport) match {
        case Some(errorReport) =>
          val msg = s"Unable to delete landing zone with ID $landingZoneId for BPM-backed " +
            s"billing project: ${errorReport.getMessage}."
          logger.warn(msg)
          val status = Option(errorReport.getStatusCode).map(code => StatusCode.int2StatusCode(code))
          throw new LandingZoneDeletionException(
            RawlsErrorReport("WorkspaceManager", msg, status, Seq.empty, Seq.empty, None)
          )
        case None => Some(landingZoneResponse)
      }
  }

  override def maybeCleanupResources(projectName: RawlsBillingProjectName,
                                     maybeGoogleProject: Boolean,
                                     ctx: RawlsRequestContext
  )(implicit
    executionContext: ExecutionContext
  ): Future[Option[UUID]] =
    for {
      jobControlId <- billingRepository.getLandingZoneId(projectName).map {
        case Some(landingZoneId) =>
          val result = cleanupLandingZone(UUID.fromString(landingZoneId), ctx)
          result.map(_.getJobReport.getId).map(UUID.fromString)
        case None =>
          if (!maybeGoogleProject) {
            logger.warn(s"Deleting billing project $projectName, but no associated landing zone to delete")
          }
          None
      }
    } yield jobControlId
}
