package org.broadinstitute.dsde.rawls.user

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.Applicative
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.monadThrowDBIOAction
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserUtils, UserWiths}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.model.google.{BigQueryTableName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchUserId}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {

  val allUsersGroupRef: RawlsGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))

  def constructor(
    dataSource: SlickDataSource,
    googleServicesDAO: GoogleServicesDAO,
    samDAO: SamDAO,
    bqServiceFactory: GoogleBigQueryServiceFactory,
    bigQueryCredentialJson: String,
    servicePerimeterService: ServicePerimeterService,
    workspaceManagerDAO: WorkspaceManagerDAO,
    notificationDAO: NotificationDAO
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext) =
    new UserService(
      ctx,
      dataSource,
      googleServicesDAO,
      samDAO,
      bqServiceFactory,
      bigQueryCredentialJson,
      servicePerimeterService,
      workspaceManagerDAO,
      new BillingRepository(dataSource),
      notificationDAO
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
  )(implicit ex: ExecutionContext): Future[Unit] = {
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
  val samDAO: SamDAO,
  bqServiceFactory: GoogleBigQueryServiceFactory,
  bigQueryCredentialJson: String,
  servicePerimeterService: ServicePerimeterService,
  val workspaceManagerDAO: WorkspaceManagerDAO,
  val billingRepository: BillingRepository,
  notificationDAO: NotificationDAO
)(implicit protected val executionContext: ExecutionContext)
    extends RoleSupport
    with FutureSupport
    with UserWiths
    with UserUtils
    with LazyLogging
    with StringValidationUtils {

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  private def requireProjectAction[T](projectName: RawlsBillingProjectName, action: SamResourceAction)(
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

  private def requireServicePerimeterAction[T](servicePerimeterName: ServicePerimeterName, action: SamResourceAction)(
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

  private def getBillingProjectResponse(
    billingProjectFuture: Future[Option[RawlsBillingProject]]
  ): Future[Option[RawlsBillingProjectResponse]] =
    billingProjectFuture.flatMap {
      case Some(project) =>
        samDAO
          .listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, ctx)
          .map(samRolesToProjectRoles) map { roles =>
          if (roles.nonEmpty) {
            Some(mapCloudPlatformAndPolicies(project, roles))
          } else {
            None
          }
        }
      case None => Future.successful(None)
    }

  def getBillingProject(projectName: RawlsBillingProjectName): Future[Option[RawlsBillingProjectResponse]] =
    getBillingProjectResponse(billingRepository.getBillingProject(projectName))

  def getBillingProjectById(id: UUID): Future[Option[RawlsBillingProjectResponse]] =
    getBillingProjectResponse(billingRepository.getBillingProjectById(id))

  def listBillingProjectsV2(): Future[List[RawlsBillingProjectResponse]] = for {
    samUserResources <- samDAO.listUserResources(SamResourceTypeNames.billingProject, ctx)
    rolesByResourceId: Map[String, Set[ProjectRole]] = samUserResources
      .groupBy(_.resourceId)
      .view
      .mapValues(resources => samRolesToProjectRoles(resources.flatMap(r => r.direct.roles ++ r.inherited.roles).toSet))
      .toMap
    resourceIds = rolesByResourceId.keySet
    projectsInDB <- billingRepository.getBillingProjects(resourceIds.map(RawlsBillingProjectName))
  } yield projectsInDB.toList
    .map { p =>
      val roles = rolesByResourceId.getOrElse(p.projectName.value, Set())
      mapCloudPlatformAndPolicies(p, roles)
    }
    .filter(p => p.roles.nonEmpty)

  private def mapCloudPlatformAndPolicies(
    project: RawlsBillingProject,
    roles: Set[ProjectRole]
  ): RawlsBillingProjectResponse = RawlsBillingProjectResponse(roles, project, CloudPlatform.GCP, protectedData = None)

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
    * Unregisters a billing project with UserInfo provided in parameter
    *
    * @param projectName   The project name to be unregistered.
    * @param ownerUserInfo The project's owner user info with  <pre>UserInfo</pre>  format.
    * */
  private def unregisterBillingProjectWithUserInfo(projectName: RawlsBillingProjectName,
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
            case Some(RawlsBillingProject(_, _, _, Some(billingAccountName), _, _, _, false, _, _, _, _, _, _)) =>
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
            // Isolate the db txn so we're not running any REST calls inside it
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
                          s"The billing export table $tableName in dataset $datasetName could not be found."
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

  def verifyBillingProjectAccess(projectUuid: UUID, action: SamResourceAction): Future[Option[Boolean]] =
    for {
      billingProject <- billingRepository.getBillingProjectById(projectUuid)
      userActions <- billingProject match {
        case Some(bp) =>
          samDAO
            .listUserActionsForResource(SamResourceTypeNames.billingProject, bp.projectName.value, ctx)
            .map(Option(_))
        case None => Future.successful(None)
      }
    } yield userActions.filter(_.nonEmpty).map(_.contains(action))

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
          case None                   => getLegacyBillingPolicies(projectAccessUpdate.role)
          case Some(billingProfileId) => Seq(getV2BillingPolicy(projectAccessUpdate.role))
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
        _ <- removeUserFromBillingProjectInner(projectName, projectAccessUpdate)
      } yield {}
    }

  def batchUpdateBillingProjectMembers(projectName: RawlsBillingProjectName,
                                       batchProjectAccessUpdate: BatchProjectAccessUpdate,
                                       inviteUsersNotFound: Boolean
  ): Future[Unit] =
    requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) {
      val membersToAdd = batchProjectAccessUpdate.membersToAdd
      val membersToRemove = batchProjectAccessUpdate.membersToRemove

      collectMissingUsers(membersToAdd.map(_.email), ctx) flatMap { missingUsers =>
        if (missingUsers.isEmpty || inviteUsersNotFound) {
          for {
            invites <- Future.traverse(missingUsers) { invite =>
              samDAO.inviteUser(invite, ctx).map { _ =>
                Notifications.BillingProjectInvitedNotification(
                  WorkbenchEmail(invite),
                  WorkbenchUserId(ctx.userInfo.userSubjectId.value),
                  projectName.value
                )
              }
            }
            additions <- Future.traverse(membersToAdd) { projectAccessUpdate =>
              addUserToBillingProjectV2(projectName, projectAccessUpdate)
            }
            removals <- Future.traverse(membersToRemove) { projectAccessUpdate =>
              removeUserFromBillingProjectV2(projectName, projectAccessUpdate)
            }
          } yield notificationDAO.fireAndForgetNotifications(invites)
        } else
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Conflict, s"Users ${missingUsers.mkString(",")} have not signed up for Terra")
            )
          )
      }
    }

  def updateBillingProjectBillingAccount(billingProjectName: RawlsBillingProjectName,
                                         updateAccountRequest: UpdateRawlsBillingAccountRequest
  ): Future[Option[RawlsBillingProjectResponse]] = {
    validateBillingAccountName(updateAccountRequest.billingAccount.value)

    requireProjectAction(billingProjectName, SamBillingProjectActions.updateBillingAccount) {
      for {
        hasAccess <- gcsDAO.testTerraAndUserBillingAccountAccess(updateAccountRequest.billingAccount, ctx.userInfo)
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

  private def updateBillingAccountInternal(
    projectName: RawlsBillingProjectName,
    billingAccount: Option[RawlsBillingAccountName]
  ): Future[Option[RawlsBillingProjectResponse]] = for {
    project <- updateBillingAccountInDatabase(projectName, billingAccount)
    projectRoles <- samDAO
      .listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, ctx)
      .map(resourceRoles => samRolesToProjectRoles(resourceRoles))
  } yield project.flatMap { p =>
    if (projectRoles.nonEmpty) Some(RawlsBillingProjectResponse(projectRoles, p, platform = CloudPlatform.GCP))
    else None
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
                // Since the billing account has been updated, any existing "spend" configuration is now out of date
                _ <- dataAccess.rawlsBillingProjectQuery.clearBillingProjectSpendConfiguration(billingProjectName)
                // if any workspaces failed to be updated last time, clear out the error message so the monitor will pick them up and try to update them again
                _ <- dataAccess.workspaceQuery
                  .deleteAllWorkspaceErrorMessagesInBillingProject(billingProjectName)
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

        // not combining into the above transaction because it calls Google within a transaction. fml.
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
