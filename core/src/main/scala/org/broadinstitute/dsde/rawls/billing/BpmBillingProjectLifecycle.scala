package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{CreateLandingZoneResult, DeleteAzureLandingZoneResult}
import cats.implicits.{catsSyntaxFlatMapOps, toTraverseOps}
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
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
  val samDAO: SamDAO,
  val billingRepository: BillingRepository,
  billingProfileManagerDAO: BillingProfileManagerDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  resourceMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao
)(implicit val executionContext: ExecutionContext)
    extends BillingProjectLifecycle {

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
    def createLandingZone(profileModel: ProfileModel): Future[CreateLandingZoneResult] = {
      val hardcodedLzId = UUID.fromString("f41c1a97-179b-4a18-9615-5214d79ba600")
      logger.warn(s"***** ATTACHING TO LANDING ZONE w/ID = ${hardcodedLzId}")
      val params =  config.azureConfig.get.landingZoneParameters ++ Map("ATTACH" -> "true")
      logger.warn(s"***** LZ PARAMS = ${params}")
      Future(blocking {
        workspaceManagerDAO.createLandingZone(
          config.azureConfig.get.landingZoneDefinition,
          config.azureConfig.get.landingZoneVersion,
          params,
          profileModel.getId,
          ctx,
          Some(hardcodedLzId)
        )
      })
    }

    def addMembersToBillingProfile(profileModel: ProfileModel): Future[Set[Unit]] = {
      val members = createProjectRequest.members.getOrElse(Set.empty)
      Future.traverse(members) { member =>
        Future(blocking {
          billingProfileManagerDAO.addProfilePolicyMember(profileModel.getId,
                                                          ProfilePolicy.fromProjectRole(member.role),
                                                          member.email,
                                                          ctx
          )
        })
      }
    }

    createBillingProfile.flatMap { profileModel =>
      addMembersToBillingProfile(profileModel).flatMap { _ =>
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
            logger.error("Billing project creation failed, cleaning up billing profile")
            cleanupBillingProfile(profileModel.getId, projectName, ctx).recover { case cleanupError: Throwable =>
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
  ): DeleteAzureLandingZoneResult = Try(workspaceManagerDAO.deleteLandingZone(landingZoneId, ctx)) match {
    case Failure(e: ApiException) =>
      val msg = s"Unable to delete landing zone: ${e.getMessage}"
      throw new LandingZoneDeletionException(RawlsErrorReport(StatusCode.int2StatusCode(e.getCode), msg, e))
    case Failure(t) =>
      logger.warn(s"Unable to delete landing zone with ID $landingZoneId for BPM-backed billing project.", t)
      throw new LandingZoneDeletionException(RawlsErrorReport(t))
    case Success(landingZoneResponse) =>
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
        case None => landingZoneResponse
      }
  }

  /**
    * Delete the billing profile if no other billing projects reference it. If an exception
    * is failed during deletion, allow it to pass up so caller can choose to disallow deletion
    * of parent billing project.
    */
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
          s"Deleting BPM-backed billing project ${projectName.value}, deleting billing profile record $profileModelId"
        )
        billingProfileManagerDAO.deleteBillingProfile(profileModelId, ctx)
      case num =>
        logger.info(
          s"Deleting BPM-backed billing project ${projectName.value}, but not deleting billing profile record $profileModelId because $num other project(s) reference it"
        )
    }
  }

  override def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Option[UUID]] =
    for {
      jobControlId <- billingRepository.getLandingZoneId(projectName).map {
        case Some(landingZoneId) =>
          val result = cleanupLandingZone(UUID.fromString(landingZoneId), ctx)
          Some(UUID.fromString(result.getJobReport.getId))
        case None =>
          logger.warn(s"Deleting BPM-backed billing project $projectName, but no associated landing zone to delete")
          None
      }
    } yield jobControlId

  override def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ): Future[Unit] = for {
    billingProfileId <- billingRepository.getBillingProfileId(projectName)
    _ <- billingProfileId match {
      case Some(id) => cleanupBillingProfile(UUID.fromString(id), projectName, ctx)
      case None =>
        logger.warn(
          s"Deleting BPM-backed billing project $projectName, but no associated billing profile record to delete"
        )
        Future.successful()
    }
  } yield unregisterBillingProject(projectName, ctx)

}
