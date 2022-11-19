package org.broadinstitute.dsde.rawls.user

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport
import cats.Applicative
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.DeploymentManagerConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.monadThrowDBIOAction
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{BigQueryTableName, GoogleProject}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {

  val allUsersGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))

  def constructor(
    dataSource: SlickDataSource,
    googleServicesDAO: GoogleServicesDAO,
    samDAO: SamDAO,
    bqServiceFactory: GoogleBigQueryServiceFactory,
    bigQueryCredentialJson: String,
    requesterPaysRole: String,
    dmConfig: DeploymentManagerConfig,
    projectTemplate: ProjectTemplate,
    servicePerimeterService: ServicePerimeterService,
    adminRegisterBillingAccountId: RawlsBillingAccountName,
    billingProfileManagerDAO: BillingProfileManagerDAO,
    workspaceManagerDAO: WorkspaceManagerDAO
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext) =
    new UserService(
      ctx,
      dataSource,
      googleServicesDAO,
      samDAO,
      bqServiceFactory,
      bigQueryCredentialJson,
      requesterPaysRole,
      dmConfig,
      projectTemplate,
      servicePerimeterService,
      adminRegisterBillingAccountId,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      new BillingRepository(dataSource),
      new WorkspaceManagerResourceMonitorRecordDao(dataSource)
    )

  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList)

  def syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO: SamDAO, projectName: RawlsBillingProjectName)(implicit
    ec: ExecutionContext
  ): Future[WorkbenchEmail] =
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectPolicyNames.owner)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))

  // this will no longer be used after v1 compute permissions are removed from billing projects (https://broadworkbench.atlassian.net/browse/CA-913)
  def syncBillingProjectComputeUserPolicyToGoogleAndGetEmail(samDAO: SamDAO, projectName: RawlsBillingProjectName)(
    implicit ec: ExecutionContext
  ): Future[WorkbenchEmail] =
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject,
                          projectName.value,
                          SamBillingProjectPolicyNames.canComputeUser
      )
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))

  def getDefaultGoogleProjectPolicies(ownerGroupEmail: WorkbenchEmail,
                                      computeUserGroupEmail: WorkbenchEmail,
                                      requesterPaysRole: String
  ): Map[String, Set[String]] =
    Map(
      "roles/viewer" -> Set(s"group:${ownerGroupEmail.value}"),
      requesterPaysRole -> Set(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}"),
      "roles/bigquery.jobUser" -> Set(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}")
    )

  // TODO - once workspace migration is complete and there are no more v1 workspaces or v1 billing projects, we can remove this https://broadworkbench.atlassian.net/browse/CA-1118
  def deleteGoogleProjectIfChild(projectName: RawlsBillingProjectName,
                                 userInfoForSam: UserInfo,
                                 gcsDAO: GoogleServicesDAO,
                                 samDAO: SamDAO,
                                 ctx: RawlsRequestContext,
                                 deleteGoogleProjectWithGoogle: Boolean = true
  )(implicit ex: ExecutionContext) = {
    def rawlsCreatedGoogleProjectExists(projectId: GoogleProjectId) =
      gcsDAO.getGoogleProject(projectId) transform {
        case Success(_) => Success(true)
        case Failure(e: HttpResponseException) if e.getStatusCode == 404 || e.getStatusCode == 403 =>
          Success(
            false
          ) // Either the Google project doesn't exist, or we don't have access to it because Rawls didn't create it.
        case Failure(t) => Failure(t)
      }

    def F = Applicative[Future]

    def deleteResourcesInGoogle(projectId: GoogleProjectId) =
      for {
        _ <- deletePetsInProject(projectId, gcsDAO, samDAO, ctx)
        _ <- F.whenA(deleteGoogleProjectWithGoogle)(gcsDAO.deleteV1Project(projectId))
      } yield ()

    val projectId = GoogleProjectId(projectName.value)
    samDAO.listResourceChildren(SamResourceTypeNames.billingProject,
                                projectName.value,
                                ctx.copy(userInfo = userInfoForSam)
    ) flatMap { resourceChildren =>
      F.whenA(
        resourceChildren contains SamFullyQualifiedResourceId(projectName.value,
                                                              SamResourceTypeNames.googleProject.value
        )
      )(
        for {
          _ <- rawlsCreatedGoogleProjectExists(projectId).ifM(deleteResourcesInGoogle(projectId), F.unit)
          _ <- samDAO.deleteResource(SamResourceTypeNames.googleProject,
                                     projectName.value,
                                     ctx.copy(userInfo = userInfoForSam)
          )
        } yield ()
      )
    }
  }

  private def deletePetsInProject(projectName: GoogleProjectId,
                                  gcsDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  ctx: RawlsRequestContext
  )(implicit ex: ExecutionContext): Future[Unit] =
    for {
      projectUsers <- samDAO.listAllResourceMemberIds(SamResourceTypeNames.billingProject, projectName.value, ctx)
      _ <- projectUsers.toList.traverse(destroyPet(_, projectName, gcsDAO, samDAO, ctx))
    } yield ()

  private def destroyPet(userIdInfo: UserIdInfo,
                         projectName: GoogleProjectId,
                         gcsDAO: GoogleServicesDAO,
                         samDAO: SamDAO,
                         ctx: RawlsRequestContext
  )(implicit ex: ExecutionContext): Future[Unit] =
    for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(projectName, RawlsUserEmail(userIdInfo.userEmail))
      petUserInfo <- gcsDAO.getUserInfoUsingJson(petSAJson)
      _ <- samDAO.deleteUserPetServiceAccount(projectName, ctx.copy(userInfo = petUserInfo))
    } yield ()
}

class UserService(
  protected val ctx: RawlsRequestContext,
  val dataSource: SlickDataSource,
  protected val gcsDAO: GoogleServicesDAO,
  samDAO: SamDAO,
  bqServiceFactory: GoogleBigQueryServiceFactory,
  bigQueryCredentialJson: String,
  requesterPaysRole: String,
  protected val dmConfig: DeploymentManagerConfig,
  protected val projectTemplate: ProjectTemplate,
  servicePerimeterService: ServicePerimeterService,
  adminRegisterBillingAccountId: RawlsBillingAccountName,
  workspaceManagerDAO: WorkspaceManagerDAO,
  billingProfileManagerDAO: BillingProfileManagerDAO,
  val billingRepository: BillingRepository,
  val workspaceResourceRecordDao: WorkspaceManagerResourceMonitorRecordDao
)(implicit protected val executionContext: ExecutionContext)
    extends RoleSupport
    with FutureSupport
    with UserWiths
    with LazyLogging
    with StringValidationUtils {

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  import dataSource.dataAccess.driver.api._

  def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(
    op: => Future[T]
  ): Future[T] =
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, ctx).flatMap {
      case true => op
      case false =>
        Future.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a project owner.")
          )
        )
    }

  def requireServicePerimeterAction[T](servicePerimeterName: ServicePerimeterName, action: SamResourceAction)(
    op: => Future[T]
  ): Future[T] =
    samDAO
      .userHasAction(SamResourceTypeNames.servicePerimeter,
                     URLEncoder.encode(servicePerimeterName.value, UTF_8.name),
                     action,
                     ctx
      )
      .flatMap {
        case true => op
        case false =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              errorReport =
                ErrorReport(StatusCodes.NotFound, "Service Perimeter does not exist or you do not have access")
            )
          )
      }

  def isAdmin(userEmail: RawlsUserEmail): Future[Boolean] =
    toFutureTry(tryIsFCAdmin(userEmail)) map {
      case Failure(t) =>
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(b) => b
    }

  def isLibraryCurator(userEmail: RawlsUserEmail): Future[Boolean] =
    toFutureTry(gcsDAO.isLibraryCurator(userEmail.value)) map {
      case Failure(t) =>
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(b) => b
    }

  def adminAddLibraryCurator(userEmail: RawlsUserEmail): Future[Unit] =
    asFCAdmin {
      toFutureTry(gcsDAO.addLibraryCurator(userEmail.value)) map {
        case Failure(t) =>
          throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
        case Success(result) => result
      }
    }

  def adminRemoveLibraryCurator(userEmail: RawlsUserEmail): Future[Unit] =
    asFCAdmin {
      toFutureTry(gcsDAO.removeLibraryCurator(userEmail.value)) map {
        case Failure(t) =>
          throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
        case Success(result) => result
      }
    }

  def listBillingAccounts(firecloudHasAccess: Option[Boolean] = None): Future[Seq[RawlsBillingAccount]] =
    gcsDAO.listBillingAccounts(ctx.userInfo, firecloudHasAccess)

  def getBillingProjectStatus(projectName: RawlsBillingProjectName): Future[Option[RawlsBillingProjectStatus]] = {
    val statusFuture: Future[Option[RawlsBillingProjectStatus]] = for {
      policies <- samDAO.listUserResources(SamResourceTypeNames.billingProject, ctx)
      projectDetail <- dataSource.inTransaction(dataAccess => dataAccess.rawlsBillingProjectQuery.load(projectName))
    } yield policies
      .find { policy =>
        projectDetail.isDefined &&
        policy.resourceId.equals(projectDetail.get.projectName.value)
      }
      .flatMap { policy =>
        Some(RawlsBillingProjectStatus(RawlsBillingProjectName(policy.resourceId), projectDetail.get.status))
      }
    statusFuture
  }

  def getBillingProject(projectName: RawlsBillingProjectName): Future[Option[RawlsBillingProjectResponse]] = for {
    roles <- samDAO
      .listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, ctx)
      .map(resourceRoles => samRolesToProjectRoles(resourceRoles))
    billingProject <- billingRepository.getBillingProject(projectName)
    billingProfile = billingProject.flatMap {
      _.billingProfileId.flatMap(id => billingProfileManagerDAO.getBillingProfile(UUID.fromString(id), ctx))
    }
    lzUpdatedProject: Option[RawlsBillingProject] <- updateLandingZoneStatus(billingProject)
  } yield lzUpdatedProject.flatMap(p => if (roles.nonEmpty) Some(mapCloudPlatform(p, billingProfile, roles)) else None)

  def listBillingProjectsV2(): Future[List[RawlsBillingProjectResponse]] = for {
    samUserResources <- samDAO.listUserResources(SamResourceTypeNames.billingProject, ctx)
    rolesByResourceId: Map[String, Set[ProjectRole]] = samUserResources
      .groupBy(_.resourceId)
      .view
      .mapValues(resources => samRolesToProjectRoles(resources.flatMap(r => r.direct.roles ++ r.inherited.roles).toSet))
      .toMap
    resourceIds = rolesByResourceId.keySet
    billingProfiles <- billingProfileManagerDAO.getAllBillingProfiles(ctx)
    projectsInDB <- billingRepository.getBillingProjects(resourceIds.map(RawlsBillingProjectName))
    lzUpdatedProjects <- Future.sequence(projectsInDB.map(updateLandingZoneStatus))
  } yield lzUpdatedProjects.toList.map { p =>
    val roles = rolesByResourceId.getOrElse(p.projectName.value, Set())
    val billingProfile = p.billingProfileId.flatMap(id => billingProfiles.find(_.getId == UUID.fromString(id)))
    mapCloudPlatform(p, billingProfile, roles)
  }

  /**
    * Map the cloud platform to a billing project.
    * if no BPM id is set it's a GCP project
    * if a BPM id is set and a billing profile was found, use cloud platform from bpm and add the the coordinates if it's Azure
    * if a BPM id is set and no billing profile was found, mark as UNKNOWN
    */
  def mapCloudPlatform(
    project: RawlsBillingProject,
    billingProfile: Option[ProfileModel],
    roles: Set[ProjectRole]
  ): RawlsBillingProjectResponse = (project.billingProfileId, billingProfile) match {
    case (None, _) => RawlsBillingProjectResponse(roles, project, CloudPlatform.GCP)
    case (Some(_), Some(p)) =>
      val platform = CloudPlatform(p)
      val responseProject = if (platform == CloudPlatform.AZURE) {
        val c = AzureManagedAppCoordinates(p.getTenantId, p.getSubscriptionId, p.getManagedResourceGroupId)
        project.copy(azureManagedAppCoordinates = Some(c))
      } else project
      RawlsBillingProjectResponse(roles, responseProject, platform)
    case (Some(id), None) =>
      val message = Some(s"Unable to find billing profile in Billing Profile Manager for billing profile id: $id")
      RawlsBillingProjectResponse(roles, project.copy(message = message, status = CreationStatuses.Error))
  }

  def updateLandingZoneStatus(billingProject: Option[RawlsBillingProject]): Future[Option[RawlsBillingProject]] =
    billingProject match {
      case None    => Future.successful(billingProject)
      case Some(p) => updateLandingZoneStatus(p).map(Some(_))
    }

  /**
    * For billing projects in the status of CreationStatuses.CreatingLandingZone,
    * retrieve the landing zone status and update the project appropriately.
    * Deletes the record tracking the landing zone job, if the job is completed.
    */
  def updateLandingZoneStatus(billingProject: RawlsBillingProject): Future[RawlsBillingProject] = {
    // if there's no landing zone creation in progress, this is a no-op
    if (billingProject.status != CreationStatuses.CreatingLandingZone) return Future.successful(billingProject)

    for {
      record <- workspaceResourceRecordDao.selectByBillingProject(billingProject.projectName).map(_.headOption)
    } yield {
      def updateLandingZoneSuccess(lzId: UUID): RawlsBillingProject = {
        billingRepository.updateCreationStatus(billingProject.projectName, CreationStatuses.Ready, None)
        record.foreach(workspaceResourceRecordDao.delete)
        billingProject.copy(status = CreationStatuses.Ready, landingZoneId = Some(lzId.toString))
      }

      def updateLandingZoneFailure(msg: String): RawlsBillingProject = {
        val message = Some(s"Landing Zone creation failed: $msg")
        billingRepository.updateCreationStatus(billingProject.projectName, CreationStatuses.Error, message)
        record.foreach(workspaceResourceRecordDao.delete)
        billingProject.copy(status = CreationStatuses.Error, message = message)
      }
      try
        record.map(r => workspaceManagerDAO.getCreateAzureLandingZoneResult(r.jobControlId.toString, ctx)) match {
          case None => updateLandingZoneFailure("No monitoring record available")
          // we have a status - this should be easy
          case Some(result) if result.getJobReport != null && result.getJobReport.getStatus != null =>
            result.getJobReport.getStatus match {
              // the job just isn't done yet - return the billing project unchanged
              case JobReport.StatusEnum.RUNNING => billingProject
              case JobReport.StatusEnum.FAILED =>
                updateLandingZoneFailure(
                  Option(result.getErrorReport).map(_.getMessage).getOrElse("Failure Reported, but no errors returned")
                )
              case JobReport.StatusEnum.SUCCEEDED if result.getLandingZone == null =>
                updateLandingZoneFailure("Landing Zone result marked as successful, but no landing zone returned")
              case JobReport.StatusEnum.SUCCEEDED => updateLandingZoneSuccess(result.getLandingZone.getId)
            }
          case Some(result) if result.getLandingZone != null && result.getLandingZone.getId != null =>
            updateLandingZoneSuccess(result.getLandingZone.getId)
          case Some(result) if result.getErrorReport != null =>
            updateLandingZoneFailure(result.getErrorReport.getMessage)
          // no job report, no landing zone, and no error report
          // return project with status as error, but don't update anything in the database,
          // so it retries in the future, and no data is lost for now
          case Some(_) =>
            billingProject.copy(status = CreationStatuses.Error,
                                message = Some("Unable to retrieve landing zone results")
            )
        }
      catch {
        case e: ApiException =>
          val error =
            s"Unable to retrieve landing zone creation job report ${record.map(_.jobControlId).getOrElse("No job control id")}"
          logger.error(error, e)
          val message = s"Api call to get landing zone from workspace manager failed: ${e.getMessage}"
          billingProject.copy(status = CreationStatuses.Error, message = Some(message))
      }
    }
  }

  def listBillingProjects(): Future[List[RawlsBillingProjectMembership]] = for {
    samUserResources <- samDAO.listUserResources(SamResourceTypeNames.billingProject, ctx)
    projectDetailsByName <- dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.getBillingProjectDetails(
        samUserResources.map(resource => RawlsBillingProjectName(resource.resourceId))
      )
    }
  } yield determineProjectRoles(samUserResources)
    .flatMap { case (resourceId, role) =>
      projectDetailsByName.get(resourceId).map { case (projectStatus, message) =>
        RawlsBillingProjectMembership(RawlsBillingProjectName(resourceId), role, projectStatus, message)
      }
    }
    .toList
    .sortBy(_.projectName.value)

  private def samRolesToProjectRoles(samRoles: Set[SamResourceRole]): Set[ProjectRole] = samRoles.collect {
    case SamResourceRole(SamBillingProjectRoles.owner.value)            => ProjectRoles.Owner
    case SamResourceRole(SamBillingProjectRoles.workspaceCreator.value) => ProjectRoles.User
  }

  private def determineProjectRoles(samUserResources: Seq[SamUserResource]) =
    samUserResources.collect {
      case r if r.hasRole(SamBillingProjectRoles.owner) =>
        (r.resourceId, ProjectRoles.Owner)
      case r if r.hasRole(SamBillingProjectRoles.workspaceCreator) =>
        (r.resourceId, ProjectRoles.User)
    }

  def getBillingProjectMembers(projectName: RawlsBillingProjectName): Future[Set[RawlsBillingProjectMember]] =
    samDAO
      .listUserActionsForResource(SamResourceTypeNames.billingProject, projectName.value, ctx)
      .flatMap {
        // the JSON responses for listPoliciesForResource and getPolicy are shaped slightly differently.
        // the initial 2 cases will coerce the data into the same shape so the final yield can be re-used for both cases.
        // only project owners can call listPoliciesForResource, whereas project users must call getPolicy directly on the owner policy
        case actions if actions.contains(SamBillingProjectActions.readPolicies) =>
          samDAO.listPoliciesForResource(SamResourceTypeNames.billingProject, projectName.value, ctx).map {
            policiesWithNameAndEmail =>
              policiesWithNameAndEmail
                .map(policyWithNameAndEmail => policyWithNameAndEmail.policyName -> policyWithNameAndEmail.policy)
          }
        case actions if actions.contains(SamBillingProjectActions.readPolicy(SamBillingProjectPolicyNames.owner)) =>
          samDAO
            .getPolicy(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectPolicyNames.owner, ctx)
            .map { policy =>
              Set(SamBillingProjectPolicyNames.owner -> policy)
            }
        case _ =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.Forbidden, "You do not have the required actions to perform this.")
            )
          )
      }
      .map { policies =>
        for {
          (role, policy) <- policies.collect {
            case (SamBillingProjectPolicyNames.owner, policy)            => (ProjectRoles.Owner, policy)
            case (SamBillingProjectPolicyNames.workspaceCreator, policy) => (ProjectRoles.User, policy)
          }
          email <- policy.memberEmails
        } yield RawlsBillingProjectMember(RawlsUserEmail(email.value), role)
      }

  /**
    * Unregisters a billing project with OwnerInfo provided in the request body.
    *
    * The admin unregister endpoint does not delete the Google project in Google when we unregister it. Project
    * registration allows tests to use existing Google projects (like GPAlloc) as if Rawls had created it,
    * so we should not delete those pre-existing Google projects when we unregister them.
    *
    * @param projectName The project name to be unregistered.
    * @param ownerInfo   A map parsed from request body contains the project's owner info.
    * */
  def adminUnregisterBillingProjectWithOwnerInfo(projectName: RawlsBillingProjectName,
                                                 ownerInfo: Map[String, String]
  ): Future[Unit] =
    asFCAdmin {
      val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfo("newOwnerEmail")),
                                   OAuth2BearerToken(ownerInfo("newOwnerToken")),
                                   3600,
                                   RawlsUserSubjectId("0")
      )
      for {
        _ <- deleteGoogleProjectIfChild(projectName,
                                        ownerUserInfo,
                                        gcsDAO,
                                        samDAO,
                                        ctx,
                                        deleteGoogleProjectWithGoogle = false
        )
        result <- unregisterBillingProjectWithUserInfo(projectName, ownerUserInfo)
      } yield result
    }

  /**
    * Unregisters a billing project with UserInfo provided in parameter
    *
    * @param projectName   The project name to be unregistered.
    * @param ownerUserInfo The project's owner user info with {@code UserInfo} format.
    * */
  def unregisterBillingProjectWithUserInfo(projectName: RawlsBillingProjectName,
                                           ownerUserInfo: UserInfo
  ): Future[Unit] =
    for {
      _ <- billingRepository.deleteBillingProject(projectName)
      _ <- samDAO
        .deleteResource(SamResourceTypeNames.billingProject,
                        projectName.value,
                        ctx.copy(userInfo = ownerUserInfo)
        ) recoverWith { // Moving this to the end so that the rawls record is cleared even if there are issues clearing the Sam resource (theoretical workaround for https://broadworkbench.atlassian.net/browse/CA-1206)
        case t: Throwable =>
          logger.warn(
            s"Unexpected failure deleting billing project (while deleting billing project in Sam) for billing project `${projectName.value}`",
            t
          )
          throw t
      }
    } yield {}

  def adminDeleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[Unit] =
    asFCAdmin {
      val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfo("newOwnerEmail")),
                                   OAuth2BearerToken(ownerInfo("newOwnerToken")),
                                   3600,
                                   RawlsUserSubjectId("0")
      )
      for {
        _ <- deleteGoogleProjectIfChild(projectName, ownerUserInfo, gcsDAO, samDAO, ctx)
        _ <- unregisterBillingProjectWithUserInfo(projectName, ownerUserInfo)
      } yield {}
    }

  def deleteBillingProject(projectName: RawlsBillingProjectName): Future[Unit] =
    requireProjectAction(projectName, SamBillingProjectActions.deleteBillingProject) {
      for {
        _ <- billingRepository.failUnlessHasNoWorkspaces(projectName)
        _ <- deleteGoogleProjectIfChild(projectName, ctx.userInfo, gcsDAO, samDAO, ctx)
        _ <- unregisterBillingProjectWithUserInfo(projectName, ctx.userInfo)
      } yield {}
    }

  def setBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName,
                                          spendReportConfiguration: BillingProjectSpendConfiguration
  ): Future[Int] = {

    val datasetName = spendReportConfiguration.datasetName
    val datasetGoogleProject = spendReportConfiguration.datasetGoogleProject

    validateBigQueryDatasetName(datasetName)
    validateGoogleProjectName(datasetGoogleProject.value)

    requireProjectAction(billingProjectName, SamBillingProjectActions.alterSpendReportConfiguration) {
      val bqService =
        bqServiceFactory.getServiceFromJson(bigQueryCredentialJson, GoogleProject(billingProjectName.value))

      for {
        // Get the dataset to validate that it exists and that we have permission to see it
        _ <- bqService.use(_.getDataset(datasetGoogleProject, datasetName)).unsafeToFuture().map {
          case None =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, s"The dataset $datasetName could not be found.")
            )
          case dataset => dataset
        }

        billingAccountId <- dataSource.inTransaction { dataAccess =>
          dataAccess.rawlsBillingProjectQuery.load(billingProjectName).map {
            case Some(RawlsBillingProject(_, _, Some(billingAccountName), _, _, _, _, false, _, _, _, _, _, _)) =>
              billingAccountName.withoutPrefix()
            case _ =>
              throw new RawlsExceptionWithErrorReport(
                ErrorReport(
                  StatusCodes.BadRequest,
                  s"The Google project associated with billing project ${billingProjectName.value} is not linked to an active billing account."
                )
              )
          }
        }

        // Get the table and validate that it exists and that we have permission to see it
        // Note that the table name replaces all dashes in the billing account ID with underscores
        tableName = BigQueryTableName(s"gcp_billing_export_v1_${billingAccountId.replace("-", "_")}")
        table <- bqService.use(_.getTable(datasetGoogleProject, datasetName, tableName)).unsafeToFuture()

        res <-
          if (table.isDefined) {
            // Isolate the db txn so we're not running any REST calls inside of it
            dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.setBillingProjectSpendConfiguration(billingProjectName,
                                                                                      Option(datasetName),
                                                                                      Option(tableName),
                                                                                      Option(datasetGoogleProject)
              )
            }
          } else
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest,
                          s"The billing export table ${tableName} in dataset ${datasetName} could not be found."
              )
            )
      } yield res
    }
  }

  def clearBillingProjectSpendConfiguration(billingProjectName: RawlsBillingProjectName): Future[Int] =
    requireProjectAction(billingProjectName, SamBillingProjectActions.alterSpendReportConfiguration) {
      dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.clearBillingProjectSpendConfiguration(billingProjectName)
      }
    }

  def getBillingProjectSpendConfiguration(
    billingProjectName: RawlsBillingProjectName
  ): Future[Option[BillingProjectSpendConfiguration]] =
    requireProjectAction(billingProjectName, SamBillingProjectActions.readSpendReportConfiguration) {
      dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(billingProjectName).map {
          case Some(
                RawlsBillingProject(_,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                    Some(spendReportDataset),
                                    Some(spendReportTable),
                                    Some(spendReportDatasetGoogleProject),
                                    _,
                                    _,
                                    _
                )
              ) =>
            Option(BillingProjectSpendConfiguration(spendReportDatasetGoogleProject, spendReportDataset))
          case Some(_) => None
          case None =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.NotFound, s"Billing project ${billingProjectName.value} could not be found")
            )
        }
      }
    }

  // very sad: have to pass the new owner's token in the POST body (oh no!)
  // we could instead exploit the fact that Sam will let you create pets in projects you're not in (!!!),
  // but that seems extremely shady
  // We believe this is mostly used by gpalloc/only used by gpalloc, which is why the billing account
  // is hard coded.
  def adminRegisterBillingProject(xfer: RawlsBillingProjectTransfer): Future[Unit] =
    asFCAdmin {
      val billingProjectName = RawlsBillingProjectName(xfer.project)
      val project =
        RawlsBillingProject(billingProjectName, CreationStatuses.Ready, Option(adminRegisterBillingAccountId), None)
      val ownerUserInfo = UserInfo(RawlsUserEmail(xfer.newOwnerEmail),
                                   OAuth2BearerToken(xfer.newOwnerToken),
                                   3600,
                                   RawlsUserSubjectId("0")
      )

      (for {
        _ <- dataSource.inTransaction(dataAccess => dataAccess.rawlsBillingProjectQuery.create(project))

        _ <- samDAO.createResource(SamResourceTypeNames.billingProject, billingProjectName.value, ctx)
        _ <- samDAO.createResourceFull(
          SamResourceTypeNames.googleProject,
          project.projectName.value,
          Map.empty,
          Set.empty,
          ctx.copy(userInfo = ownerUserInfo),
          Option(SamFullyQualifiedResourceId(project.projectName.value, SamResourceTypeNames.billingProject.value))
        )
        _ <- samDAO.overwritePolicy(
          SamResourceTypeNames.billingProject,
          billingProjectName.value,
          SamBillingProjectPolicyNames.workspaceCreator,
          SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator)),
          ctx.copy(userInfo = ownerUserInfo)
        )
        _ <- samDAO.overwritePolicy(
          SamResourceTypeNames.billingProject,
          billingProjectName.value,
          SamBillingProjectPolicyNames.canComputeUser,
          SamPolicy(Set.empty,
                    Set.empty,
                    Set(SamBillingProjectRoles.batchComputeUser, SamBillingProjectRoles.notebookUser)
          ),
          ctx.copy(userInfo = ownerUserInfo)
        )
        ownerGroupEmail <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, project.projectName)
        computeUserGroupEmail <- syncBillingProjectComputeUserPolicyToGoogleAndGetEmail(samDAO, project.projectName)

        policiesToAdd = getDefaultGoogleProjectPolicies(ownerGroupEmail, computeUserGroupEmail, requesterPaysRole)

        _ <- gcsDAO.addPolicyBindings(project.googleProjectId, policiesToAdd)
        _ <- gcsDAO.grantReadAccess(xfer.bucket, Set(ownerGroupEmail, computeUserGroupEmail))
      } yield {}).recoverWith { case t: Throwable =>
        // attempt cleanup then rethrow
        for {
          _ <- samDAO
            .deleteResource(SamResourceTypeNames.googleProject,
                            project.projectName.value,
                            ctx.copy(userInfo = ownerUserInfo)
            )
            .recover { case x =>
              logger.debug(
                s"failure deleting google project ${project.projectName.value} from sam during error recovery cleanup.",
                x
              )
            }
          _ <- samDAO
            .deleteResource(SamResourceTypeNames.billingProject,
                            project.projectName.value,
                            ctx.copy(userInfo = ownerUserInfo)
            )
            .recover { case x =>
              logger.debug(
                s"failure deleting billing project ${project.projectName.value} from sam during error recovery cleanup.",
                x
              )
            }
          _ <- dataSource
            .inTransaction(dataAccess => dataAccess.rawlsBillingProjectQuery.delete(project.projectName))
            .recover { case x =>
              logger.debug(
                s"failure deleting billing project ${project.projectName.value} from rawls db during error recovery cleanup.",
                x
              )
            }
        } yield throw t
      }
    }

  private def getLegacyBillingPolicies(samRole: ProjectRole): Seq[SamResourcePolicyName] =
    samRole match {
      case ProjectRoles.Owner => Seq(SamBillingProjectPolicyNames.owner)
      case ProjectRoles.User =>
        Seq(SamBillingProjectPolicyNames.workspaceCreator, SamBillingProjectPolicyNames.canComputeUser)
    }
  private def getV2BillingPolicy(samRole: ProjectRole): SamResourcePolicyName =
    samRole match {
      case ProjectRoles.Owner => SamBillingProjectPolicyNames.owner
      case ProjectRoles.User  => SamBillingProjectPolicyNames.workspaceCreator
    }

  def addUserToBillingProject(projectName: RawlsBillingProjectName,
                              projectAccessUpdate: ProjectAccessUpdate
  ): Future[Unit] =
    requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) {
      val policies = getLegacyBillingPolicies(projectAccessUpdate.role)
      addUserToBillingProjectInner(projectName, projectAccessUpdate, policies)
    }

  private def addUserToBillingProjectInner(projectName: RawlsBillingProjectName,
                                           projectAccessUpdate: ProjectAccessUpdate,
                                           policies: Seq[SamResourcePolicyName]
  ): Future[Unit] =
    for {
      _ <- Future.traverse(policies) { policy =>
        samDAO
          .addUserToPolicy(SamResourceTypeNames.billingProject,
                           projectName.value,
                           policy,
                           projectAccessUpdate.email,
                           ctx
          )
          .recoverWith { case regrets: Throwable =>
            if (policy == SamBillingProjectPolicyNames.canComputeUser) {
              logger.info(
                s"error adding user to canComputeUser policy for $projectName likely because it is a v2 billing project which does not have a canComputeUser policy. regrets: ${regrets.getMessage}"
              )
              Future.successful(())
            } else {
              Future.failed(regrets)
            }
          }
      }
    } yield {}

  def addUserToBillingProjectV2(projectName: RawlsBillingProjectName,
                                projectAccessUpdate: ProjectAccessUpdate
  ): Future[Unit] =
    requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) {
      for {
        billingProfileId <- billingRepository.getBillingProfileId(projectName)
        policies = billingProfileId match {
          case None => getLegacyBillingPolicies(projectAccessUpdate.role)
          case Some(billingProfileId) =>
            billingProfileManagerDAO.addProfilePolicyMember(
              UUID.fromString(billingProfileId),
              projectAccessUpdate.role,
              projectAccessUpdate.email,
              ctx
            )
            Seq(getV2BillingPolicy(projectAccessUpdate.role))
        }
        _ <- addUserToBillingProjectInner(projectName, projectAccessUpdate, policies)
      } yield {}
    }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName,
                                   projectAccessUpdate: ProjectAccessUpdate
  ): Future[Unit] =
    requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) {
      removeUserFromBillingProjectInner(projectName, projectAccessUpdate)
    }

  private def removeUserFromBillingProjectInner(projectName: RawlsBillingProjectName,
                                                projectAccessUpdate: ProjectAccessUpdate
  ): Future[Unit] =
    samDAO
      .removeUserFromPolicy(SamResourceTypeNames.billingProject,
                            projectName.value,
                            getV2BillingPolicy(projectAccessUpdate.role),
                            projectAccessUpdate.email,
                            ctx
      )
      .recover {
        case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode.contains(StatusCodes.BadRequest) =>
          throw new RawlsExceptionWithErrorReport(e.errorReport.copy(statusCode = Some(StatusCodes.NotFound)))
      }

  def removeUserFromBillingProjectV2(projectName: RawlsBillingProjectName,
                                     projectAccessUpdate: ProjectAccessUpdate
  ): Future[Unit] =
    requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) {
      for {
        billingProfileId <- billingRepository.getBillingProfileId(projectName)
        _ <- billingProfileId match {
          case Some(billingProfileId) =>
            billingProfileManagerDAO.deleteProfilePolicyMember(
              UUID.fromString(billingProfileId),
              projectAccessUpdate.role,
              projectAccessUpdate.email,
              ctx
            )
            Future.successful()
          case None => Future.successful()
        }
        _ <- removeUserFromBillingProjectInner(projectName, projectAccessUpdate)
      } yield {}
    }

  def updateBillingProjectBillingAccount(billingProjectName: RawlsBillingProjectName,
                                         updateAccountRequest: UpdateRawlsBillingAccountRequest
  ): Future[Option[RawlsBillingProjectResponse]] = {
    validateBillingAccountName(updateAccountRequest.billingAccount.value)

    requireProjectAction(billingProjectName, SamBillingProjectActions.updateBillingAccount) {
      for {
        hasAccess <- gcsDAO.testBillingAccountAccess(updateAccountRequest.billingAccount, ctx.userInfo)
        _ = if (!hasAccess) {
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest,
                        "Billing account does not exist, user does not have access, or Terra does not have access"
            )
          )
        }
        result <- updateBillingAccountInternal(billingProjectName, Option(updateAccountRequest.billingAccount))
      } yield result
    }
  }

  def deleteBillingAccount(billingProjectName: RawlsBillingProjectName): Future[Option[RawlsBillingProjectResponse]] =
    requireProjectAction(billingProjectName, SamBillingProjectActions.updateBillingAccount) {
      updateBillingAccountInternal(billingProjectName, None)
    }

  def startBillingProjectCreation(createProjectRequest: CreateRawlsBillingProjectFullRequest): Future[Unit] =
    for {
      _ <- validateV1CreateProjectRequest(createProjectRequest)
      _ <- ServicePerimeterService.checkServicePerimeterAccess(samDAO, createProjectRequest.servicePerimeter, ctx)
      billingAccount <- checkBillingAccountAccess(createProjectRequest.billingAccount)
      result <- internalStartBillingProjectCreation(createProjectRequest, billingAccount)
    } yield result

  private def validateV1CreateProjectRequest(createProjectRequest: CreateRawlsBillingProjectFullRequest): Future[Unit] =
    for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)
      _ <-
        if (
          (createProjectRequest.enableFlowLogs.getOrElse(false) || createProjectRequest.privateIpGoogleAccess.getOrElse(
            false
          )) && !createProjectRequest.highSecurityNetwork.getOrElse(false)
        ) {
          // flow logs and private google access both require HSN, so error if someone asks for either of the former without the latter
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest,
                          "enableFlowLogs or privateIpGoogleAccess both require highSecurityNetwork = true"
              )
            )
          )
        } else {
          Future.successful(())
        }
    } yield ()

  private def checkBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[RawlsBillingAccount] = {
    def createForbiddenErrorMessage(who: String, billingAccountName: RawlsBillingAccountName) =
      s"""${who} must have the permission "Billing Account User" on ${billingAccountName.value} to create a project with it."""

    gcsDAO.listBillingAccounts(ctx.userInfo) flatMap { billingAccountNames =>
      billingAccountNames.find(_.accountName == billingAccountName) match {
        case None =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Forbidden, createForbiddenErrorMessage("You", billingAccountName))
            )
          )
        case Some(billingAccount) =>
          if (billingAccount.firecloudHasAccess)
            Future.successful(billingAccount)
          else
            Future.failed(
              new RawlsExceptionWithErrorReport(
                ErrorReport(StatusCodes.BadRequest,
                            createForbiddenErrorMessage(gcsDAO.billingEmail, billingAccountName)
                )
              )
            )
      }
    }
  }

  private def internalStartBillingProjectCreation(createProjectRequest: CreateRawlsBillingProjectFullRequest,
                                                  billingAccount: RawlsBillingAccount
  ): Future[Unit] =
    for {
      project <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(createProjectRequest.projectName) flatMap {
          case None =>
            for {
              _ <- DBIO.from(
                samDAO.createResource(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, ctx)
              )
              _ <- DBIO.from(
                samDAO.createResourceFull(
                  SamResourceTypeNames.googleProject,
                  createProjectRequest.projectName.value,
                  Map.empty,
                  Set.empty,
                  ctx,
                  Option(
                    SamFullyQualifiedResourceId(createProjectRequest.projectName.value,
                                                SamResourceTypeNames.billingProject.value
                    )
                  )
                )
              )
              _ <- DBIO.from(
                samDAO.overwritePolicy(
                  SamResourceTypeNames.billingProject,
                  createProjectRequest.projectName.value,
                  SamBillingProjectPolicyNames.workspaceCreator,
                  SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator)),
                  ctx
                )
              )
              _ <- DBIO.from(
                samDAO.overwritePolicy(
                  SamResourceTypeNames.billingProject,
                  createProjectRequest.projectName.value,
                  SamBillingProjectPolicyNames.canComputeUser,
                  SamPolicy(Set.empty,
                            Set.empty,
                            Set(SamBillingProjectRoles.batchComputeUser, SamBillingProjectRoles.notebookUser)
                  ),
                  ctx
                )
              )
              project <- dataAccess.rawlsBillingProjectQuery.create(
                RawlsBillingProject(
                  createProjectRequest.projectName,
                  CreationStatuses.Creating,
                  Option(createProjectRequest.billingAccount),
                  None,
                  None,
                  createProjectRequest.servicePerimeter
                )
              )
            } yield project

          case Some(_) =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Conflict, "project by that name already exists")
            )
        }
      }

      // NOTE: we're syncing this to Sam ahead of the resource actually existing. is this fine? (ps these are sam calls)
      ownerGroupEmail <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
      computeUserGroupEmail <- syncBillingProjectComputeUserPolicyToGoogleAndGetEmail(samDAO,
                                                                                      createProjectRequest.projectName
      )

      // each service perimeter should have a folder which is used to make an aggregate log sink for flow logs
      parentFolderId <- createProjectRequest.servicePerimeter.traverse(lookupFolderIdFromServicePerimeterName)

      createProjectOperation <- gcsDAO
        .createProject(
          project.googleProjectId,
          billingAccount,
          dmConfig.templatePath,
          createProjectRequest.highSecurityNetwork.getOrElse(false),
          createProjectRequest.enableFlowLogs.getOrElse(false),
          createProjectRequest.privateIpGoogleAccess.getOrElse(false),
          requesterPaysRole,
          ownerGroupEmail,
          computeUserGroupEmail,
          projectTemplate,
          parentFolderId
        )
        .recoverWith { case t: Throwable =>
          // failed to create project in google land, rollback inserts above
          dataSource.inTransaction { dataAccess =>
            dataAccess.rawlsBillingProjectQuery.delete(createProjectRequest.projectName)
          } map (_ => throw t)
        }

      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.insertOperations(Seq(createProjectOperation))
      }
    } yield {}

  private def updateBillingAccountInternal(
    projectName: RawlsBillingProjectName,
    billingAccount: Option[RawlsBillingAccountName]
  ): Future[Option[RawlsBillingProjectResponse]] = for {
    project <- updateBillingAccountInDatabase(projectName, billingAccount)
    projectRoles <- samDAO
      .listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, ctx)
      .map(resourceRoles => samRolesToProjectRoles(resourceRoles))
  } yield project.flatMap { p =>
    if (projectRoles.nonEmpty) Some(RawlsBillingProjectResponse(projectRoles, p)) else None
  }

  private def updateBillingAccountInDatabase(billingProjectName: RawlsBillingProjectName,
                                             billingAccountName: Option[RawlsBillingAccountName]
  ): Future[Option[RawlsBillingProject]] =
    dataSource.inTransaction { dataAccess =>
      val F = Applicative[ReadWriteAction]
      dataAccess.rawlsBillingProjectQuery
        .load(billingProjectName)
        .flatMap(_.traverse { project =>
          F.pure(project.copy(billingAccount = billingAccountName)) <* F
            .whenA(project.billingAccount != billingAccountName) {
              for {
                _ <- dataAccess.rawlsBillingProjectQuery.updateBillingAccount(billingProjectName,
                                                                              billingAccountName,
                                                                              ctx.userInfo.userSubjectId
                )
                // Since the billing account has been updated, any existing spend configuration is now out of date
                _ <- dataAccess.rawlsBillingProjectQuery.clearBillingProjectSpendConfiguration(billingProjectName)
                // if any workspaces failed to be updated last time, clear out the error message so the monitor will pick them up and try to update them again
                _ <- dataAccess.workspaceQuery
                  .deleteAllWorkspaceBillingAccountErrorMessagesInBillingProject(billingProjectName)
              } yield ()
            }
        })
    }

  private def lookupFolderIdFromServicePerimeterName(perimeterName: ServicePerimeterName): Future[String] = {
    val folderName = perimeterName.value.split("/").last
    gcsDAO.getFolderId(folderName).flatMap {
      case None =>
        Future
          .failed(new RawlsException(s"folder named $folderName corresponding to perimeter $perimeterName not found"))
      case Some(folderId) => Future.successful(folderId)
    }
  }

  // User needs to be an owner of the billing project and have the AddProject action on the service perimeter
  private def requirePermissionsToAddToServicePerimeter[T](servicePerimeterName: ServicePerimeterName,
                                                           projectName: RawlsBillingProjectName
  )(op: => Future[T]): Future[T] =
    requireServicePerimeterAction(servicePerimeterName, SamServicePerimeterActions.addProject) {
      requireProjectAction[T](projectName, SamBillingProjectActions.addToServicePerimeter) {
        op
      }
    }

  def addProjectToServicePerimeter(servicePerimeterName: ServicePerimeterName,
                                   projectName: RawlsBillingProjectName
  ): Future[Unit] =
    requirePermissionsToAddToServicePerimeter(servicePerimeterName, projectName) {
      for {
        billingProject <- dataSource.inTransaction { dataAccess =>
          dataAccess.rawlsBillingProjectQuery.load(projectName).map { billingProjectOpt =>
            billingProjectOpt.getOrElse(
              throw new RawlsException(
                s"Sam thinks user has access to project ${projectName.value} but project not found in database"
              )
            )
          }
        }

        _ <- billingProject.servicePerimeter match {
          case Some(existingServicePerimeter) =>
            Future.failed(
              new RawlsExceptionWithErrorReport(
                ErrorReport(
                  StatusCodes.BadRequest,
                  s"project ${billingProject.projectName.value} is already in service perimeter $existingServicePerimeter"
                )
              )
            )
          case None => Future.successful(())
        }

        // Even if the project's status is 'Creating' and could possibly still have a perimeter added to it, we throw an exception to avoid a race condition
        _ <- billingProject.status match {
          case CreationStatuses.Ready => Future.successful(())
          case status =>
            Future.failed(
              new RawlsExceptionWithErrorReport(
                ErrorReport(StatusCodes.BadRequest,
                            s"project ${billingProject.projectName.value} should be Ready but is $status"
                )
              )
            )
        }

        // each service perimeter should have a folder which is used to make an aggregate log sink for flow logs
        _ <- moveGoogleProjectToServicePerimeterFolder(servicePerimeterName, billingProject.googleProjectId)

        googleProjectNumber <- billingProject.googleProjectNumber match {
          case Some(existingGoogleProjectNumber) => Future.successful(existingGoogleProjectNumber)
          case None =>
            gcsDAO
              .getGoogleProject(billingProject.googleProjectId)
              .map(googleProject => gcsDAO.getGoogleProjectNumber(googleProject))
        }

        _ <- dataSource.inTransaction { dataAccess =>
          for {
            workspaces <- dataAccess.workspaceQuery.listWithBillingProject(projectName)
            // all v2 workspaces in the specified Terra billing project will already have their own
            // Google project number, but any v1 workspaces should store the Terra billing project's
            // Google project number
            v1Workspaces = workspaces.filterNot(_.googleProjectNumber.isDefined)
            _ <- dataAccess.workspaceQuery.updateGoogleProjectNumber(v1Workspaces.map(_.workspaceIdAsUUID),
                                                                     googleProjectNumber
            )
            _ <- dataAccess.rawlsBillingProjectQuery.updateServicePerimeter(billingProject.projectName,
                                                                            servicePerimeterName.some
            )
            _ <- dataAccess.rawlsBillingProjectQuery.updateGoogleProjectNumber(billingProject.projectName,
                                                                               googleProjectNumber.some
            )
          } yield ()
        }

        // not combining into the above transaction because it calls google within a transaction. fml.
        _ <- dataSource.inTransaction { dataAccess =>
          servicePerimeterService.overwriteGoogleProjectsInPerimeter(servicePerimeterName, dataAccess)
        }
      } yield {}
    }

  def moveGoogleProjectToServicePerimeterFolder(servicePerimeterName: ServicePerimeterName,
                                                googleProjectId: GoogleProjectId
  ): Future[Unit] =
    for {
      folderId <- lookupFolderIdFromServicePerimeterName(servicePerimeterName)
      _ <- gcsDAO.addProjectToFolder(googleProjectId, folderId)
    } yield ()
}
