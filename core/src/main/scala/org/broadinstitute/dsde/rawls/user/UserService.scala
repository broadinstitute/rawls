package org.broadinstitute.dsde.rawls.user

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.implicits._
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.DeploymentManagerConfig
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  val allUsersGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, notificationDAO: NotificationDAO, samDAO: SamDAO, requesterPaysRole: String, dmConfig: DeploymentManagerConfig, projectTemplate: ProjectTemplate, servicePerimeterService: ServicePerimeterService)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, notificationDAO, samDAO, requesterPaysRole, dmConfig, projectTemplate, servicePerimeterService)

  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList)

  def syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO: SamDAO, projectName: RawlsBillingProjectName)(implicit ec: ExecutionContext): Future[WorkbenchEmail] = {
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectPolicyNames.owner)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
  }

  // this will no longer be used after v1 compute permissions are removed from billing projects (https://broadworkbench.atlassian.net/browse/CA-913)
  def syncBillingProjectComputeUserPolicyToGoogleAndGetEmail(samDAO: SamDAO, projectName: RawlsBillingProjectName)(implicit ec: ExecutionContext): Future[WorkbenchEmail] = {
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectPolicyNames.canComputeUser)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))
  }

  def getDefaultGoogleProjectPolicies(ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, requesterPaysRole: String) = {
    Map(
      "roles/viewer" -> Set(s"group:${ownerGroupEmail.value}"),
      "roles/billing.projectManager" -> Set(s"group:${ownerGroupEmail.value}"),
      requesterPaysRole -> Set(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}"),
      "roles/bigquery.jobUser" -> Set(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}")
    )
  }
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, notificationDAO: NotificationDAO, samDAO: SamDAO, requesterPaysRole: String, protected val dmConfig: DeploymentManagerConfig, protected val projectTemplate: ProjectTemplate, servicePerimeterService: ServicePerimeterService)(implicit protected val executionContext: ExecutionContext) extends RoleSupport with FutureSupport with UserWiths with LazyLogging with StringValidationUtils {
  implicit val errorReportSource = ErrorReportSource("rawls")

  import dataSource.dataAccess.driver.api._

  def SetRefreshToken(token: UserRefreshToken) = setRefreshToken(token)
  def GetRefreshTokenDate = getRefreshTokenDate()

  def GetBillingProjectStatus(projectName: RawlsBillingProjectName) = getBillingProjectStatus(projectName)
  def GetBillingProject(projectName: RawlsBillingProjectName) = getBillingProject(projectName)
  def ListBillingProjects = listBillingProjects
  def ListBillingProjectsV2 = listBillingProjectsV2
  def AdminDeleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { adminDeleteBillingProject(projectName, ownerInfo) }
  def AdminRegisterBillingProject(xfer: RawlsBillingProjectTransfer) = asFCAdmin { registerBillingProject(xfer) }
  def AdminUnregisterBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { unregisterBillingProjectWithOwnerInfo(projectName, ownerInfo) }
  def DeleteBillingProject(projectName: RawlsBillingProjectName) = requireProjectAction(projectName, SamBillingProjectActions.deleteBillingProject) { deleteBillingProject(projectName) }

  def AddUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) { addUserToBillingProject(projectName, projectAccessUpdate) }
  def RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) { removeUserFromBillingProject(projectName, projectAccessUpdate) }
  def ListBillingAccounts = listBillingAccounts()

  def UpdateBillingAccount(projectName: RawlsBillingProjectName, updateProjectRequest: UpdateRawlsBillingAccountRequest) = requireProjectAction(projectName, SamBillingProjectActions.updateBillingAccount) { updateBillingAccount(projectName, updateProjectRequest) }
  def DeleteBillingAccount(projectName: RawlsBillingProjectName) = requireProjectAction(projectName, SamBillingProjectActions.updateBillingAccount) { deleteBillingAccount(projectName) }

  def CreateBillingProjectFull(createProjectRequest: CreateRawlsBillingProjectFullRequest) = startBillingProjectCreation(createProjectRequest)
  def CreateBillingProjectFullV2(createProjectRequest: CreateRawlsBillingProjectFullRequest) = createBillingProjectV2(createProjectRequest)
  def GetBillingProjectMembers(projectName: RawlsBillingProjectName) = requireProjectAction(projectName, SamBillingProjectActions.readPolicies) { getBillingProjectMembers(projectName) }

  def AdminDeleteRefreshToken(userRef: RawlsUserRef) = asFCAdmin { deleteRefreshToken(userRef) }

  def IsAdmin(userEmail: RawlsUserEmail) = { isAdmin(userEmail) }
  def IsLibraryCurator(userEmail: RawlsUserEmail) = { isLibraryCurator(userEmail) }
  def AdminAddLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { addLibraryCurator(userEmail) }
  def AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { removeLibraryCurator(userEmail) }

  def AddProjectToServicePerimeter(servicePerimeterName: ServicePerimeterName, projectName: RawlsBillingProjectName) = requirePermissionsToAddToServicePerimeter(servicePerimeterName, projectName) { addProjectToServicePerimeter(servicePerimeterName, projectName) }

  def requireProjectAction(projectName: RawlsBillingProjectName, action: SamResourceAction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a project owner.")))
    }
  }

  def requireServicePerimeterAction(servicePerimeterName: ServicePerimeterName, action: SamResourceAction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeterName.value, UTF_8.name), action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, "Service Perimeter does not exist or you do not have access")))
    }
  }

  def setRefreshToken(userRefreshToken: UserRefreshToken): Future[PerRequestMessage] = {
    gcsDAO.storeToken(userInfo, userRefreshToken.refreshToken).map(_ => RequestComplete(StatusCodes.Created))
  }

  def getRefreshTokenDate(): Future[PerRequestMessage] = {
    gcsDAO.getTokenDate(RawlsUser(userInfo)).map(_ match {
      case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"no refresh token stored for ${userInfo.userEmail}"))
      case Some(date) => RequestComplete(UserRefreshTokenDate(date))
    }).recover {
      case t: TokenResponseException =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(t.getStatusCode, t))
    }
  }

  def isAdmin(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(tryIsFCAdmin(userEmail)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(b) => b match {
        case true => RequestComplete(StatusCodes.OK)
        case false => RequestComplete(StatusCodes.NotFound)
      }
    }
  }

  def isLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.isLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(b) => b match {
        case true => RequestComplete(StatusCodes.OK)
        case false => RequestComplete(StatusCodes.NotFound)
      }
    }
  }

  def addLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.addLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(_) => RequestComplete(StatusCodes.OK)
    }
  }

  def removeLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.removeLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, t))
      case Success(_) => RequestComplete(StatusCodes.OK)
    }
  }

  import spray.json.DefaultJsonProtocol._

  def listBillingAccounts(): Future[PerRequestMessage] =
    gcsDAO.listBillingAccounts(userInfo) map(RequestComplete(_))

  def getBillingProjectStatus(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    val statusFuture: Future[Option[RawlsBillingProjectStatus]] = for {
      policies <- samDAO.getPoliciesForType(SamResourceTypeNames.billingProject, userInfo)
      projectDetail <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.load(projectName) }
    } yield {
      policies.find { policy =>
        projectDetail.isDefined &&
        policy.resourceId.equals(projectDetail.get.projectName.value)
      }.flatMap { policy =>
        Some(RawlsBillingProjectStatus(RawlsBillingProjectName(policy.resourceId), projectDetail.get.status))
      }
    }
    statusFuture.map {
      case Some(status) => RequestComplete(StatusCodes.OK, status)
      case _ => RequestComplete(StatusCodes.NotFound)
    }
  }

  def getBillingProject(billingProjectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    for {
      projectRoles <- samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, billingProjectName.value, userInfo)
        .map(resourceRoles => samRolesToProjectRoles(resourceRoles))
      maybeBillingProject <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.load(billingProjectName) }
    } yield {
      constructBillingProjectResponseFromOptionalAndRoles(maybeBillingProject, projectRoles)
    }
  }

  private def constructBillingProjectResponseWithUserRoles(projectRoles: Set[ProjectRole], billingProject: RawlsBillingProject): Future[RawlsBillingProjectResponse] = {
    for {
      (workspacesWithCorrectBillingAccount, workspacesWithIncorrectBillingAccount) <- loadAndPartitionWorkspacesByMatchingBillingProjectBillingAccount(billingProject)
    } yield RawlsBillingProjectResponse(
      billingProject.projectName,
      billingProject.billingAccount,
      billingProject.servicePerimeter,
      billingProject.invalidBillingAccount,
      projectRoles,
      workspacesWithCorrectBillingAccount.map(_.toWorkspaceName).toSet,
      workspacesWithIncorrectBillingAccount.map(workspace => WorkspaceBillingAccount(workspace.toWorkspaceName, workspace.currentBillingAccountOnGoogleProject)).toSet)
  }

  // This returns two seqs of Workspace. The first seq is the ones with the correct billing account, the second seq is the ones with incorrect billing accounts
  private def loadAndPartitionWorkspacesByMatchingBillingProjectBillingAccount(billingProject: RawlsBillingProject): Future[(Seq[Workspace], Seq[Workspace])] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.listWithBillingProject(billingProject.projectName).map(workspaces => {
        workspaces.partition(workspace => workspace.currentBillingAccountOnGoogleProject == billingProject.billingAccount)
      })
    }
  }

  def listBillingProjectsV2(): Future[PerRequestMessage] = {
    for {
      samUserResources <- samDAO.listUserResources(SamResourceTypeNames.billingProject, userInfo)
      projectsInDB <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.getBillingProjects(samUserResources.map(resource => RawlsBillingProjectName(resource.resourceId)).toSet) }
    } yield {
      val projectResponses = constructBillingProjectResponses(samUserResources, projectsInDB)
      RequestComplete(projectResponses)
    }
  }

  private def constructBillingProjectResponses(samUserResources: Seq[SamUserResource], billingProjectsInRawlsDB: Seq[RawlsBillingProject]): Future[List[RawlsBillingProjectResponse]] = {
    val projectsByName = billingProjectsInRawlsDB.map(p => p.projectName -> p).toMap
    for {
      billingProjectResponses <- samUserResources.toList.traverse { samUserResource =>
        val allSamRoles = samUserResource.direct.roles ++ samUserResource.inherited.roles
        val projectRoles = samRolesToProjectRoles(allSamRoles)
        projectsByName.get(RawlsBillingProjectName(samUserResource.resourceId))
          .traverse(billingProject => constructBillingProjectResponseWithUserRoles(projectRoles, billingProject))
      }
    } yield {
      billingProjectResponses.flatten.sortBy(value => value.projectName.value)
    }
  }

  def listBillingProjects(): Future[PerRequestMessage] = {
    val membershipsFuture = for {
      resourceIdsWithPolicyNames <- samDAO.getPoliciesForType(SamResourceTypeNames.billingProject, userInfo)
      projectDetailsByName <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.getBillingProjectDetails(resourceIdsWithPolicyNames.map(idWithPolicyName => RawlsBillingProjectName(idWithPolicyName.resourceId))) }
    } yield {
      projectPoliciesToRoles(resourceIdsWithPolicyNames).flatMap { case (resourceId, role) =>
        projectDetailsByName.get(resourceId).map { case (projectStatus, message) =>
          RawlsBillingProjectMembership(RawlsBillingProjectName(resourceId), role, projectStatus, message)
        }
      }.toList.sortBy(_.projectName.value)
    }

    membershipsFuture.map(RequestComplete(_))
  }

  private def samRolesToProjectRoles(samRoles: Set[SamResourceRole]): Set[ProjectRole] = {
    samRoles.collect {
      case SamResourceRole(SamBillingProjectRoles.owner.value) => ProjectRoles.Owner
      case SamResourceRole(SamBillingProjectRoles.workspaceCreator.value) => ProjectRoles.User
    }
  }

  private def projectPoliciesToRoles(resourceIdsWithPolicyNames: Set[SamResourceIdWithPolicyName]) = {
    resourceIdsWithPolicyNames.collect {
      case SamResourceIdWithPolicyName(resourceId, SamBillingProjectPolicyNames.owner, _, _, _) => (resourceId, ProjectRoles.Owner)
      case SamResourceIdWithPolicyName(resourceId, SamBillingProjectPolicyNames.workspaceCreator, _, _, _) => (resourceId, ProjectRoles.User)
    }
  }

  def getBillingProjectMembers(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    samDAO.listPoliciesForResource(SamResourceTypeNames.billingProject, projectName.value, userInfo).map { policies =>
      for {
        (role, policy) <- policies.collect {
          case SamPolicyWithNameAndEmail(SamBillingProjectPolicyNames.owner, policy, _) => (ProjectRoles.Owner, policy)
          case SamPolicyWithNameAndEmail(SamBillingProjectPolicyNames.workspaceCreator, policy, _) => (ProjectRoles.User, policy)
        }
        email <- policy.memberEmails
      } yield RawlsBillingProjectMember(RawlsUserEmail(email.value), role)
    }.map(RequestComplete(_))
  }

  /**
   * Unregisters a billing project with OwnerInfo provided in the request body.
   *
   * @param projectName The project name to be unregistered.
   * @param ownerInfo A map parsed from request body contains the project's owner info.
   * */
  def unregisterBillingProjectWithOwnerInfo(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[PerRequestMessage] = {
    val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfo("newOwnerEmail")), OAuth2BearerToken(ownerInfo("newOwnerToken")), 3600, RawlsUserSubjectId("0"))
    for {
      _ <- samDAO.deleteResource(SamResourceTypeNames.googleProject, projectName.value, ownerUserInfo)
      result <- unregisterBillingProjectWithUserInfo(projectName, ownerUserInfo)
    } yield result
  }

  /**
   * Unregisters a billing project with UserInfo provided in parameter
   *
   * @param projectName The project name to be unregistered.
   * @param ownerUserInfo The project's owner user info with {@code UserInfo} format.
   * */
  def unregisterBillingProjectWithUserInfo(projectName: RawlsBillingProjectName, ownerUserInfo: UserInfo): Future[PerRequestMessage] = {
    for {
      _ <- samDAO.deleteResource(SamResourceTypeNames.billingProject, projectName.value, ownerUserInfo)
      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.delete(projectName)
      }
    } yield {
      RequestComplete(StatusCodes.NoContent)
    }
  }

  private def deletePetsInProject(projectName: GoogleProjectId, userInfo: UserInfo): Future[Unit] = {
    for {
      projectUsers <- samDAO.listAllResourceMemberIds(SamResourceTypeNames.billingProject, projectName.value, userInfo)
      _ <- projectUsers.toList.traverse(destroyPet(_, projectName))
    } yield ()
  }

  private def destroyPet(userIdInfo: UserIdInfo, projectName: GoogleProjectId): Future[Unit] = {
    for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(projectName, RawlsUserEmail(userIdInfo.userEmail))
      petUserInfo <- gcsDAO.getUserInfoUsingJson(petSAJson)
      _ <- samDAO.deleteUserPetServiceAccount(projectName, petUserInfo)
    } yield ()
  }

  def adminDeleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[PerRequestMessage] = {
    val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfo("newOwnerEmail")), OAuth2BearerToken(ownerInfo("newOwnerToken")), 3600, RawlsUserSubjectId("0"))
    for {
      _ <- deleteGoogleProjectIfChild(projectName, ownerUserInfo)
      _ <- unregisterBillingProjectWithUserInfo(projectName, ownerUserInfo)
    } yield RequestComplete(StatusCodes.NoContent)
  }

  def deleteBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    for {
      _ <- validateNoWorkspaces(projectName)
      _ <- deleteGoogleProjectIfChild(projectName, userInfo)
      _ <- unregisterBillingProjectWithUserInfo(projectName, userInfo)
    } yield RequestComplete(StatusCodes.NoContent)
  }

  private def validateNoWorkspaces(projectName: RawlsBillingProjectName): Future[Unit] = {
    for {
      workspaceCount <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.countByNamespace(projectName)
      }
      _ <- if (workspaceCount > 0) {
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Project cannot be deleted because it contains workspaces.")))
      } else {
        Future.successful(())
      }
    } yield ()
  }

  // TODO - once workspace migration is complete and there are no more v1 workspaces or v1 billing projects, we can remove this https://broadworkbench.atlassian.net/browse/CA-1118
  private def deleteGoogleProjectIfChild(projectName: RawlsBillingProjectName, userInfoForSam: UserInfo) = {
    samDAO.listResourceChildren(SamResourceTypeNames.billingProject, projectName.value, userInfoForSam).flatMap { projectChildren =>
      if (projectChildren.contains(SamFullyQualifiedResourceId(projectName.value, SamResourceTypeNames.googleProject.value))) {
        for {
          _ <- deletePetsInProject(GoogleProjectId(projectName.value), userInfoForSam)
          _ <- gcsDAO.deleteV1Project(GoogleProjectId(projectName.value))
          _ <- samDAO.deleteResource(SamResourceTypeNames.googleProject, projectName.value, userInfoForSam)
        } yield ()
      } else {
        Future.successful(())
      }
    }
  }

  //very sad: have to pass the new owner's token in the POST body (oh no!)
  //we could instead exploit the fact that Sam will let you create pets in projects you're not in (!!!),
  //but that seems extremely shady
  def registerBillingProject(xfer: RawlsBillingProjectTransfer): Future[PerRequestMessage] = {
    val billingProjectName = RawlsBillingProjectName(xfer.project)
    val project = RawlsBillingProject(billingProjectName, CreationStatuses.Ready, None, None)
    val ownerUserInfo = UserInfo(RawlsUserEmail(xfer.newOwnerEmail), OAuth2BearerToken(xfer.newOwnerToken), 3600, RawlsUserSubjectId("0"))


    (for {
      _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.create(project) }

      _ <- samDAO.createResource(SamResourceTypeNames.billingProject, billingProjectName.value, ownerUserInfo)
      _ <- samDAO.createResourceFull(SamResourceTypeNames.googleProject, project.projectName.value, Map.empty, Set.empty, ownerUserInfo, Option(SamFullyQualifiedResourceId(project.projectName.value, SamResourceTypeNames.billingProject.value)))
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, SamBillingProjectPolicyNames.workspaceCreator, SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator)), ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, SamBillingProjectPolicyNames.canComputeUser, SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.batchComputeUser, SamBillingProjectRoles.notebookUser)), ownerUserInfo)
      ownerGroupEmail <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, project.projectName)
      computeUserGroupEmail <- syncBillingProjectComputeUserPolicyToGoogleAndGetEmail(samDAO, project.projectName)

      policiesToAdd = getDefaultGoogleProjectPolicies(ownerGroupEmail, computeUserGroupEmail, requesterPaysRole)

      _ <- gcsDAO.addPolicyBindings(project.googleProjectId, policiesToAdd)
      _ <- gcsDAO.grantReadAccess(xfer.bucket, Set(ownerGroupEmail, computeUserGroupEmail))
    } yield {
      RequestComplete(StatusCodes.Created)
    }).recoverWith {
      case t: Throwable =>
        // attempt cleanup then rethrow
        for {
          _ <- samDAO.deleteResource(SamResourceTypeNames.googleProject, project.projectName.value, ownerUserInfo).recover {
            case x => logger.debug(s"failure deleting google project ${project.projectName.value} from sam during error recovery cleanup.", x)
          }
          _ <- samDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, ownerUserInfo).recover {
            case x => logger.debug(s"failure deleting billing project ${project.projectName.value} from sam during error recovery cleanup.", x)
          }
          _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(project.projectName) }.recover {
            case x => logger.debug(s"failure deleting billing project ${project.projectName.value} from rawls db during error recovery cleanup.", x)
          }
        } yield throw t
    }
  }

  def addUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    val policies = projectAccessUpdate.role match {
      case ProjectRoles.Owner => Seq(SamBillingProjectPolicyNames.owner)
      case ProjectRoles.User => Seq(SamBillingProjectPolicyNames.workspaceCreator, SamBillingProjectPolicyNames.canComputeUser)
    }

    for {
      _ <- Future.traverse(policies) { policy =>
        samDAO.addUserToPolicy(SamResourceTypeNames.billingProject, projectName.value, policy, projectAccessUpdate.email, userInfo)}
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    val policy = projectAccessUpdate.role match {
      case ProjectRoles.Owner => SamBillingProjectPolicyNames.owner
      case ProjectRoles.User => SamBillingProjectPolicyNames.workspaceCreator
    }

    for {
      _ <- samDAO.removeUserFromPolicy(SamResourceTypeNames.billingProject, projectName.value, policy, projectAccessUpdate.email, userInfo).recover {
        case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode.contains(StatusCodes.BadRequest) => throw new RawlsExceptionWithErrorReport(e.errorReport.copy(statusCode = Some(StatusCodes.NotFound)))}
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def deleteRefreshToken(rawlsUserRef: RawlsUserRef): Future[PerRequestMessage] = {
    deleteRefreshTokenInternal(rawlsUserRef).map(_ => RequestComplete(StatusCodes.OK))

  }

  def startBillingProjectCreation(createProjectRequest: CreateRawlsBillingProjectFullRequest): Future[PerRequestMessage] = {
    for {
      _ <- validateCreateProjectRequest(createProjectRequest)
      _ <- checkServicePerimeterAccess(createProjectRequest.servicePerimeter)
      billingAccount <- checkBillingAccountAccess(createProjectRequest.billingAccount)
      result <- internalStartBillingProjectCreation(createProjectRequest, billingAccount)
    } yield result
  }

  def updateBillingAccount(projectName: RawlsBillingProjectName, updateAccountRequest: UpdateRawlsBillingAccountRequest): Future[PerRequestMessage] = {
    for {
      hasAccess <- gcsDAO.testBillingAccountAccess(updateAccountRequest.billingAccount, userInfo)
      _ = if (!hasAccess) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Billing account does not exist, user does not have access, or Terra does not have access"))
      }
      result <- updateBillingAccountInternal(projectName, Option(updateAccountRequest.billingAccount))
    } yield result
  }

  def deleteBillingAccount(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    for {
      result <- updateBillingAccountInternal(projectName, None)
    } yield result
  }

  def createBillingProjectV2(createProjectRequest: CreateRawlsBillingProjectFullRequest): Future[PerRequestMessage] = {
    for {
      _ <- validateCreateProjectRequest(createProjectRequest)
      _ <- checkServicePerimeterAccess(createProjectRequest.servicePerimeter)
      hasAccess <- gcsDAO.testBillingAccountAccess(createProjectRequest.billingAccount, userInfo)
      _ = if (!hasAccess) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Billing account does not exist, user does not have access, or Terra does not have access"))
      }
      result <- createBillingProjectInternal(createProjectRequest)
    } yield result
  }

  private def validateCreateProjectRequest(createProjectRequest: CreateRawlsBillingProjectFullRequest): Future[Unit] = {
    for {
      _ <- validateBillingProjectName(createProjectRequest.projectName.value)
      _ <- if ((createProjectRequest.enableFlowLogs.getOrElse(false) || createProjectRequest.privateIpGoogleAccess.getOrElse(false)) && !createProjectRequest.highSecurityNetwork.getOrElse(false)) {
        //flow logs and private google access both require HSN, so error if someone asks for either of the former without the latter
        Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "enableFlowLogs or privateIpGoogleAccess both require highSecurityNetwork = true")))
      } else {
        Future.successful(())
      }
    } yield ()
  }

  private def checkBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[RawlsBillingAccount] = {
    def createForbiddenErrorMessage(who: String, billingAccountName: RawlsBillingAccountName) = {
      s"""${who} must have the permission "Billing Account User" on ${billingAccountName.value} to create a project with it."""
    }

    gcsDAO.listBillingAccounts(userInfo) flatMap { billingAccountNames =>
      billingAccountNames.find(_.accountName == billingAccountName) match {
        case Some(billingAccount) if billingAccount.firecloudHasAccess => Future.successful(billingAccount)
        case None => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, createForbiddenErrorMessage("You", billingAccountName))))
        case Some(billingAccount) if !billingAccount.firecloudHasAccess => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, createForbiddenErrorMessage(gcsDAO.billingEmail, billingAccountName))))
      }
    }
  }

  private def checkServicePerimeterAccess(servicePerimeterOption: Option[ServicePerimeterName]): Future[Unit] = {
    servicePerimeterOption.map { servicePerimeter =>
      samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, URLEncoder.encode(servicePerimeter.value, UTF_8.name), SamServicePerimeterActions.addProject, userInfo).flatMap {
        case true => Future.successful(())
        case false => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, s"You do not have the action ${SamServicePerimeterActions.addProject.value} for $servicePerimeter")))
      }
    }.getOrElse(Future.successful(()))
  }

  private def internalStartBillingProjectCreation(createProjectRequest: CreateRawlsBillingProjectFullRequest, billingAccount: RawlsBillingAccount): Future[PerRequestMessage] = {
    for {
      project <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(createProjectRequest.projectName) flatMap {
          case None =>
            for {
              _ <- DBIO.from(samDAO.createResource(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, userInfo))
              _ <- DBIO.from(samDAO.createResourceFull(SamResourceTypeNames.googleProject, createProjectRequest.projectName.value, Map.empty, Set.empty, userInfo, Option(SamFullyQualifiedResourceId(createProjectRequest.projectName.value, SamResourceTypeNames.billingProject.value))))
              _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, SamBillingProjectPolicyNames.workspaceCreator, SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator)), userInfo))
              _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, SamBillingProjectPolicyNames.canComputeUser, SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.batchComputeUser, SamBillingProjectRoles.notebookUser)), userInfo))
              project <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(createProjectRequest.projectName, CreationStatuses.Creating, Option(createProjectRequest.billingAccount), None, None, createProjectRequest.servicePerimeter))
            } yield project

          case Some(_) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists"))
        }
      }

      //NOTE: we're syncing this to Sam ahead of the resource actually existing. is this fine? (ps these are sam calls)
      ownerGroupEmail <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
      computeUserGroupEmail <- syncBillingProjectComputeUserPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)

      // each service perimeter should have a folder which is used to make an aggregate log sink for flow logs
      parentFolderId <- createProjectRequest.servicePerimeter.traverse(lookupFolderIdFromServicePerimeterName)

      createProjectOperation <- gcsDAO.createProject(project.googleProjectId, billingAccount, dmConfig.templatePath, createProjectRequest.highSecurityNetwork.getOrElse(false), createProjectRequest.enableFlowLogs.getOrElse(false), createProjectRequest.privateIpGoogleAccess.getOrElse(false), requesterPaysRole, ownerGroupEmail, computeUserGroupEmail, projectTemplate, parentFolderId).recoverWith {
        case t: Throwable =>
          // failed to create project in google land, rollback inserts above
          dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(createProjectRequest.projectName) } map(_ => throw t)
      }

      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.insertOperations(Seq(createProjectOperation))
      }
    } yield {
      RequestComplete(StatusCodes.Created)
    }
  }

  def defaultBillingProjectPolicies: Map[SamResourcePolicyName, SamPolicy] = {
    Map(
      SamBillingProjectPolicyNames.owner -> SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set(SamBillingProjectRoles.owner)),
      SamBillingProjectPolicyNames.workspaceCreator -> SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator))
    )
  }

  private def createBillingProjectInternal(createProjectRequest: CreateRawlsBillingProjectFullRequest): Future[PerRequestMessage] = {
    for {
      maybeProject <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(createProjectRequest.projectName)
      }
      _ <- maybeProject match {
        case Some(_) => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists")))
        case None => Future.successful(())
      }

      _ <- samDAO.createResourceFull(SamResourceTypeNames.billingProject, createProjectRequest.projectName.value, defaultBillingProjectPolicies, Set.empty, userInfo, None)

      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(createProjectRequest.projectName, CreationStatuses.Ready, Option(createProjectRequest.billingAccount), None, None, createProjectRequest.servicePerimeter))
      }

      _ <- syncBillingProjectOwnerPolicyToGoogleAndGetEmail(samDAO, createProjectRequest.projectName)
    } yield {
      RequestComplete(StatusCodes.Created)
    }
  }

  private def updateBillingAccountInternal(projectName: RawlsBillingProjectName, billingAccount: Option[RawlsBillingAccountName]): Future[PerRequestMessage] = {
    for {
      maybeBillingProject <- updateBillingAccountInDatabase(projectName, billingAccount)
      projectRoles <- samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, userInfo)
        .map(resourceRoles => samRolesToProjectRoles(resourceRoles))
    } yield {
      constructBillingProjectResponseFromOptionalAndRoles(maybeBillingProject, projectRoles)
    }

  }

  private def updateBillingAccountInDatabase(billingProjectName: RawlsBillingProjectName, maybeBillingAccountName: Option[RawlsBillingAccountName]): Future[Option[RawlsBillingProject]] = {
    dataSource.inTransaction { dataAccess =>
      for {
        _ <- dataAccess.rawlsBillingProjectQuery.updateBillingAccount(billingProjectName, maybeBillingAccountName)
        // if any workspaces failed to be updated last time, clear out the error message so the monitor will pick them up and try to update them again
        _ <- dataAccess.workspaceQuery.deleteAllWorkspaceBillingAccountErrorMessagesInBillingProject(billingProjectName)
        maybeBillingProject <- dataAccess.rawlsBillingProjectQuery.load(billingProjectName)
      } yield maybeBillingProject
    }
  }

  private def constructBillingProjectResponseFromOptionalAndRoles(maybeBillingProject: Option[RawlsBillingProject], projectRoles: Set[ProjectRole]) = {
    maybeBillingProject match {
      case Some(billingProject) if projectRoles.nonEmpty => RequestComplete(StatusCodes.OK, constructBillingProjectResponseWithUserRoles(projectRoles, billingProject))
      case _ => RequestComplete(StatusCodes.NotFound)
    }
  }

  private def lookupFolderIdFromServicePerimeterName(perimeterName: ServicePerimeterName): Future[String] = {
    val folderName = perimeterName.value.split("/").last
    gcsDAO.getFolderId(folderName).flatMap {
      case None => Future.failed(new RawlsException(s"folder named $folderName corresponding to perimeter $perimeterName not found"))
      case Some(folderId) => Future.successful(folderId)
    }
  }

  private def deleteRefreshTokenInternal(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    for {
      _ <- gcsDAO.revokeToken(rawlsUserRef)
      _ <- gcsDAO.deleteToken(rawlsUserRef).recover { case e: HttpResponseException if e.getStatusCode == 404 => Unit }
    } yield { Unit }
  }


  // User needs to be an owner of the billing project and have the AddProject action on the service perimeter
  private def requirePermissionsToAddToServicePerimeter(servicePerimeterName: ServicePerimeterName, projectName: RawlsBillingProjectName)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    requireServicePerimeterAction(servicePerimeterName, SamServicePerimeterActions.addProject) {
      requireProjectAction(projectName, SamBillingProjectActions.addToServicePerimeter) {
        op
      }
    }
  }

  def addProjectToServicePerimeter(servicePerimeterName: ServicePerimeterName, projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    for {
      billingProject <- dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(projectName).map { billingProjectOpt =>
          billingProjectOpt.getOrElse(throw new RawlsException(s"Sam thinks user has access to project ${projectName.value} but project not found in database"))
        }
      }

      _ <- billingProject.servicePerimeter match {
        case Some(existingServicePerimeter) => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"project ${billingProject.projectName.value} is already in service perimeter $existingServicePerimeter")))
        case None => Future.successful(())
      }

      // Even if the project's status is 'Creating' and could possibly still have a perimeter added to it, we throw an exception to avoid a race condition
      _ <- billingProject.status match {
        case CreationStatuses.Ready => Future.successful(())
        case status => Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"project ${billingProject.projectName.value} should be Ready but is $status")))
      }

      // each service perimeter should have a folder which is used to make an aggregate log sink for flow logs
      _ <- moveGoogleProjectToServicePerimeterFolder(servicePerimeterName, billingProject.googleProjectId)

      googleProjectNumber <- billingProject.googleProjectNumber match {
        case Some(existingGoogleProjectNumber) => Future.successful(existingGoogleProjectNumber)
        case None => gcsDAO.getGoogleProject(billingProject.googleProjectId).map(googleProject =>
          gcsDAO.getGoogleProjectNumber(googleProject))
      }

      // all v2 workspaces in the specified Terra billing project will already have their own Google project number, but any v1 workspaces should store the Terra billing project's Google project number
      workspaces <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWithBillingProject(projectName)
      }
      v1Workspaces = workspaces.filterNot(_.googleProjectNumber.isDefined)

      _ <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.updateGoogleProjectNumber(v1Workspaces.map(_.workspaceIdAsUUID), googleProjectNumber)
        dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(billingProject.copy(servicePerimeter = Option(servicePerimeterName), googleProjectNumber = Option(googleProjectNumber))))
      }

      _ <- servicePerimeterService.overwriteGoogleProjectsInPerimeter(servicePerimeterName)
    } yield RequestComplete(StatusCodes.NoContent)
  }

  def moveGoogleProjectToServicePerimeterFolder(servicePerimeterName: ServicePerimeterName, googleProjectId: GoogleProjectId): Future[Unit] = {
    for {
      folderId <- lookupFolderIdFromServicePerimeterName(servicePerimeterName)
      _ <- gcsDAO.addProjectToFolder(googleProjectId, folderId)
    } yield ()
  }
}

