package org.broadinstitute.dsde.rawls.user

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.http.HttpResponseException
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  val allUsersGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, notificationDAO: NotificationDAO, samDAO: SamDAO, projectOwnerGrantableRoles: Seq[String], requesterPaysRole: String, dmConfig: Config)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, notificationDAO, samDAO, projectOwnerGrantableRoles, requesterPaysRole, dmConfig)

  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList)

  def getGoogleProjectOwnerGroupEmail(samDAO: SamDAO, project: RawlsBillingProject)(implicit ec: ExecutionContext): Future[WorkbenchEmail] = {
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectPolicyNames.owner)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
  }

  def getComputeUserGroupEmail(samDAO: SamDAO, project: RawlsBillingProject)(implicit ec: ExecutionContext): Future[WorkbenchEmail] = {
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectPolicyNames.canComputeUser)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))
  }

  def getDefaultGoogleProjectPolicies(ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, requesterPaysRole: String) = {
    Map(
      "roles/viewer" -> List(s"group:${ownerGroupEmail.value}"),
      "roles/billing.projectManager" -> List(s"group:${ownerGroupEmail.value}"),
      "roles/genomics.pipelinesRunner" -> List(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}"),
      requesterPaysRole -> List(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}"),
      "roles/bigquery.jobUser" -> List(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}")
    )
  }
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, notificationDAO: NotificationDAO, samDAO: SamDAO, projectOwnerGrantableRoles: Seq[String], requesterPaysRole: String, protected val dmConfig: Config)(implicit protected val executionContext: ExecutionContext) extends RoleSupport with FutureSupport with UserWiths with LazyLogging {

  import dataSource.dataAccess.driver.api._

  def SetRefreshToken(token: UserRefreshToken) = setRefreshToken(token)
  def GetRefreshTokenDate = getRefreshTokenDate()

  def GetBillingProjectStatus(projectName: RawlsBillingProjectName) = getBillingProjectStatus(projectName)
  def ListBillingProjects = listBillingProjects
  def AdminDeleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { deleteBillingProject(projectName, ownerInfo) }
  def AdminRegisterBillingProject(xfer: RawlsBillingProjectTransfer) = asFCAdmin { registerBillingProject(xfer) }
  def AdminUnregisterBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { unregisterBillingProject(projectName, ownerInfo) }

  def AddUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) { addUserToBillingProject(projectName, projectAccessUpdate) }
  def RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamBillingProjectActions.alterPolicies) { removeUserFromBillingProject(projectName, projectAccessUpdate) }
  def GrantGoogleRoleToUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String) = requireProjectAction(projectName, SamBillingProjectActions.alterGoogleRole) { grantGoogleRoleToUser(projectName, targetUserEmail, role) }
  def RemoveGoogleRoleFromUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String) = requireProjectAction(projectName, SamBillingProjectActions.alterGoogleRole) { removeGoogleRoleFromUser(projectName, targetUserEmail, role) }
  def ListBillingAccounts = listBillingAccounts()

  def CreateBillingProjectFull(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName) = startBillingProjectCreation(projectName, billingAccount)
  def GetBillingProjectMembers(projectName: RawlsBillingProjectName) = requireProjectAction(projectName, SamBillingProjectActions.readPolicies) { getBillingProjectMembers(projectName) }

  def AdminDeleteRefreshToken(userRef: RawlsUserRef) = asFCAdmin { deleteRefreshToken(userRef) }

  def IsAdmin(userEmail: RawlsUserEmail) = { isAdmin(userEmail) }
  def IsLibraryCurator(userEmail: RawlsUserEmail) = { isLibraryCurator(userEmail) }
  def AdminAddLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { addLibraryCurator(userEmail) }
  def AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { removeLibraryCurator(userEmail) }

  val dmPubSubTopic = dmConfig.getString("pubSubTopic")
  val dmPubSubSubscription = dmConfig.getString("pubSubSubscription")
  val dmTemplatePath = dmConfig.getString("templatePath")
  val dmProject = dmConfig.getString("projectID")

  def requireProjectAction(projectName: RawlsBillingProjectName, action: SamResourceAction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, action, userInfo).flatMap {
      case true => op
      case false => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, "You must be a project owner.")))
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

  def listBillingProjects(): Future[PerRequestMessage] = {
    val membershipsFuture = for {
      resourceIdsWithPolicyNames <- samDAO.getPoliciesForType(SamResourceTypeNames.billingProject, userInfo)
      projectDetailsByName <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.getBillingProjectDetails(resourceIdsWithPolicyNames.map(idWithPolicyName => RawlsBillingProjectName(idWithPolicyName.resourceId))) }
    } yield {
      resourceIdsWithPolicyNames.collect {
        case SamResourceIdWithPolicyName(resourceId, SamBillingProjectPolicyNames.owner, _, _, _) => (resourceId, ProjectRoles.Owner)
        case SamResourceIdWithPolicyName(resourceId, SamBillingProjectPolicyNames.workspaceCreator, _, _, _) => (resourceId, ProjectRoles.User)
      }.flatMap { case (resourceId, role) =>
        projectDetailsByName.get(resourceId).map { case (projectStatus, message) =>
          RawlsBillingProjectMembership(RawlsBillingProjectName(resourceId), role, projectStatus, message)
        }
      }.toList.sortBy(_.projectName.value)
    }

    membershipsFuture.map(RequestComplete(_))
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

  def unregisterBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[PerRequestMessage] = {
    val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfo("newOwnerEmail")), OAuth2BearerToken(ownerInfo("newOwnerToken")), 3600, RawlsUserSubjectId("0"))
    for {
      _ <- samDAO.deleteResource(SamResourceTypeNames.billingProject, projectName.value, ownerUserInfo)
      _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(projectName) }
    } yield {
      RequestComplete(StatusCodes.NoContent)
    }
  }

  def deleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]): Future[PerRequestMessage] = {
    // unregister then delete actual project in google
    for {
      _ <- unregisterBillingProject(projectName, ownerInfo)
      _ <- gcsDAO.deleteProject(projectName)
    } yield RequestComplete(StatusCodes.NoContent)
  }

  //very sad: have to pass the new owner's token in the POST body (oh no!)
  //we could instead exploit the fact that Sam will let you create pets in projects you're not in (!!!),
  //but that seems extremely shady
  def registerBillingProject(xfer: RawlsBillingProjectTransfer): Future[PerRequestMessage] = {
    val billingProjectName = RawlsBillingProjectName(xfer.project)
    val project = RawlsBillingProject(billingProjectName, s"gs://${xfer.bucket}", CreationStatuses.Ready, None, None)
    val ownerUserInfo = UserInfo(RawlsUserEmail(xfer.newOwnerEmail), OAuth2BearerToken(xfer.newOwnerToken), 3600, RawlsUserSubjectId("0"))


    (for {
      _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.create(project) }

      _ <- samDAO.createResource(SamResourceTypeNames.billingProject, billingProjectName.value, ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, SamBillingProjectPolicyNames.workspaceCreator, SamPolicy(Set.empty, Set.empty, Set(SamProjectRoles.workspaceCreator)), ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, SamBillingProjectPolicyNames.canComputeUser, SamPolicy(Set.empty, Set.empty, Set(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser)), ownerUserInfo)
      ownerGroupEmail <- getGoogleProjectOwnerGroupEmail(samDAO, project)
      computeUserGroupEmail <- getComputeUserGroupEmail(samDAO, project)

      policiesToAdd = getDefaultGoogleProjectPolicies(ownerGroupEmail, computeUserGroupEmail, requesterPaysRole)

      _ <- gcsDAO.addPolicyBindings(billingProjectName, policiesToAdd)
      _ <- gcsDAO.grantReadAccess(billingProjectName, xfer.bucket, Set(ownerGroupEmail, computeUserGroupEmail))
    } yield {
      RequestComplete(StatusCodes.Created)
    }).recoverWith {
      case t: Throwable =>
        // attempt cleanup then rethrow
        for {
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

  private def googleRoleWhitelistCheck(role: String): Future[Unit] = {
    if (projectOwnerGrantableRoles.contains(role))
      Future.successful(())
    else
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"Cannot alter Google role $role: not in list [${projectOwnerGrantableRoles mkString ", "}]")))
  }

  def grantGoogleRoleToUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String): Future[PerRequestMessage] = {
    for {
      _ <- googleRoleWhitelistCheck(role)
      proxyGroupEmail <- samDAO.getProxyGroup(userInfo, targetUserEmail)
      _ <- gcsDAO.addRoleToGroup(projectName, proxyGroupEmail, role)
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def removeGoogleRoleFromUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String): Future[PerRequestMessage] = {
    for {
      _ <- googleRoleWhitelistCheck(role)
      proxyGroupEmail <- samDAO.getProxyGroup(userInfo, targetUserEmail)
      _ <- gcsDAO.removeRoleFromGroup(projectName, proxyGroupEmail, role)
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def deleteRefreshToken(rawlsUserRef: RawlsUserRef): Future[PerRequestMessage] = {
    deleteRefreshTokenInternal(rawlsUserRef).map(_ => RequestComplete(StatusCodes.OK))

  }

  def startBillingProjectCreation(projectName: RawlsBillingProjectName, billingAccountName: RawlsBillingAccountName): Future[PerRequestMessage] = {
    def createForbiddenErrorMessage(who: String, billingAccountName: RawlsBillingAccountName) = {
      s"""${who} must have the permission "Billing Account User" on ${billingAccountName.value} to create a project with it."""
    }
    gcsDAO.listBillingAccounts(userInfo) flatMap { billingAccountNames =>
      billingAccountNames.find(_.accountName == billingAccountName) match {
        case Some(billingAccount) if billingAccount.firecloudHasAccess =>
          for {
            _ <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
                case None =>
                  for {
                    _ <- DBIO.from(samDAO.createResource(SamResourceTypeNames.billingProject, projectName.value, userInfo))
                    _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectPolicyNames.workspaceCreator, SamPolicy(Set.empty, Set.empty, Set(SamProjectRoles.workspaceCreator)), userInfo))
                    _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, SamBillingProjectPolicyNames.canComputeUser, SamPolicy(Set.empty, Set.empty, Set(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser)), userInfo))
                    project <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, "gs://" + gcsDAO.getCromwellAuthBucketName(projectName), CreationStatuses.Creating, Option(billingAccountName), None))
                  } yield project

                case Some(_) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists"))
              }
            }

            //TODO: sub 2 da pub and process it as doned
            //TODO: some DM policy that makes deployments deletable after the fact
            //TODO: delete the deployment after the project has been created fine
            _ <- gcsDAO.createProject2(projectName, billingAccount, dmTemplatePath).recoverWith {
              case t: Throwable =>
                // failed to create project in google land, rollback inserts above
                dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(projectName) } map(_ => throw t)
            }


          } yield {
            RequestComplete(StatusCodes.Created)
          }
        case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, createForbiddenErrorMessage("You", billingAccountName))))
        case Some(billingAccount) if !billingAccount.firecloudHasAccess => Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, createForbiddenErrorMessage(gcsDAO.billingEmail, billingAccountName))))
      }
    }
  }

  private def deleteRefreshTokenInternal(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    for {
      _ <- gcsDAO.revokeToken(rawlsUserRef)
      _ <- gcsDAO.deleteToken(rawlsUserRef).recover { case e: HttpResponseException if e.getStatusCode == 404 => Unit }
    } yield { Unit }
  }
}

