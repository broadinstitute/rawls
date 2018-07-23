package org.broadinstitute.dsde.rawls.user

import _root_.slick.jdbc.TransactionIsolation
import akka.actor.{Actor, Props}
import akka.pattern._
import com.google.api.client.http.HttpResponseException
import com.google.api.client.auth.oauth2.TokenResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Notifications._
import org.broadinstitute.dsde.rawls.model.ManagedRoles.ManagedRole
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import akka.http.scaladsl.model.StatusCodes
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchException, WorkbenchGroupName}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  val allUsersGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))
  val workspaceCreatorPolicyName = "workspace-creator"
  val canComputeUserPolicyName = "can-compute-user"
  val ownerPolicyName = "owner"

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String, notificationDAO: NotificationDAO, samDAO: SamDAO, projectOwnerGrantableRoles: Seq[String])(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, gpsDAO, gpsGroupSyncTopic, notificationDAO, samDAO, projectOwnerGrantableRoles)

  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList)

  def getGoogleProjectOwnerGroupEmail(samDAO: SamDAO, project: RawlsBillingProject)(implicit ec: ExecutionContext): Future[RawlsGroupEmail] = {
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, SamProjectRoles.owner)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
  }

  def getComputeUserGroupEmail(samDAO: SamDAO, project: RawlsBillingProject)(implicit ec: ExecutionContext): Future[RawlsGroupEmail] = {
    samDAO
      .syncPolicyToGoogle(SamResourceTypeNames.billingProject, project.projectName.value, UserService.canComputeUserPolicyName)
      .map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting can compute user policy email")))
  }

  def getDefaultGoogleProjectPolicies(ownerGroupEmail: RawlsGroupEmail, computeUserGroupEmail: RawlsGroupEmail) = {
    Map(
      "roles/viewer" -> List(s"group:${ownerGroupEmail.value}"),
      "roles/billing.projectManager" -> List(s"group:${ownerGroupEmail.value}"),
      "roles/genomics.pipelinesRunner" -> List(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}"),
      "roles/bigquery.jobUser" -> List(s"group:${ownerGroupEmail.value}", s"group:${computeUserGroupEmail.value}")
    )
  }
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String, notificationDAO: NotificationDAO, samDAO: SamDAO, projectOwnerGrantableRoles: Seq[String])(implicit protected val executionContext: ExecutionContext) extends RoleSupport with FutureSupport with UserWiths with LazyLogging with DirectorySubjectNameSupport {

  import dataSource.dataAccess.driver.api._
  import spray.json.DefaultJsonProtocol._

  def SetRefreshToken(token: UserRefreshToken) = setRefreshToken(token)
  def GetRefreshTokenDate = getRefreshTokenDate()

  def CreateUser = createUser()

  def GetBillingProjectStatus(projectName: RawlsBillingProjectName) = getBillingProjectStatus(projectName)
  def ListBillingProjects = listBillingProjects
  def AdminDeleteBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { deleteBillingProject(projectName, ownerInfo) }
  def AdminRegisterBillingProject(xfer: RawlsBillingProjectTransfer) = asFCAdmin { registerBillingProject(xfer) }
  def AdminUnregisterBillingProject(projectName: RawlsBillingProjectName, ownerInfo: Map[String, String]) = asFCAdmin { unregisterBillingProject(projectName, ownerInfo) }

  def AddUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamResourceActions.alterPolicies) { addUserToBillingProject(projectName, projectAccessUpdate) }
  def RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) = requireProjectAction(projectName, SamResourceActions.alterPolicies) { removeUserFromBillingProject(projectName, projectAccessUpdate) }
  def GrantGoogleRoleToUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String) = requireProjectAction(projectName, SamResourceActions.alterGoogleRole) { grantGoogleRoleToUser(projectName, targetUserEmail, role) }
  def RemoveGoogleRoleFromUser(projectName: RawlsBillingProjectName, targetUserEmail: WorkbenchEmail, role: String) = requireProjectAction(projectName, SamResourceActions.alterGoogleRole) { removeGoogleRoleFromUser(projectName, targetUserEmail, role) }
  def ListBillingAccounts = listBillingAccounts()

  def RequestAccessToManagedGroup(groupRef: ManagedGroupRef) = requestAccessToManagedGroup(groupRef)
  def SetManagedGroupAccessInstructions(groupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions) = asFCAdmin { setManagedGroupAccessInstructions(groupRef, instructions) }

  def CreateBillingProjectFull(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName) = startBillingProjectCreation(projectName, billingAccount)
  def GetBillingProjectMembers(projectName: RawlsBillingProjectName) = requireProjectAction(projectName, SamResourceActions.readPolicies) { getBillingProjectMembers(projectName) }

  def AdminDeleteRefreshToken(userRef: RawlsUserRef) = asFCAdmin { deleteRefreshToken(userRef) }

  def IsAdmin(userEmail: RawlsUserEmail) = { isAdmin(userEmail) }
  def IsLibraryCurator(userEmail: RawlsUserEmail) = { isLibraryCurator(userEmail) }
  def AdminAddLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { addLibraryCurator(userEmail) }
  def AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) = asFCAdmin { removeLibraryCurator(userEmail) }

  def requireProjectAction(projectName: RawlsBillingProjectName, action: SamResourceActions.SamResourceAction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
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

  //Note: As of Sam Phase I, this function only fires off the welcome email and updates pending workspace access
  //The rest of user registration now takes place in Sam
  def createUser(): Future[Unit] = {
    val user = RawlsUser(userInfo)

    val response = handleFutures(Future.sequence(Seq(toFutureTry(turnInvitesIntoRealAccess(user)))))(_ => {
      notificationDAO.fireAndForgetNotification(ActivationNotification(user.userSubjectId))
    }, handleException("Errors creating user"))

    response.map(_ => ())
  }

  //TODO: this is just a stub to get it to compile for now
  def turnInvitesIntoRealAccess(user: RawlsUser) = {
    Future.successful(())
  }

//  def turnInvitesIntoRealAccess(user: RawlsUser) = {
//    val groupRefs = dataSource.inTransaction { dataAccess =>
//      dataAccess.workspaceQuery.findWorkspaceInvitesForUser(user.userEmail).flatMap { invites =>
//        DBIO.sequence(invites.map { case (workspaceName, accessLevel) =>
//          dataAccess.workspaceQuery.loadAccessGroup(workspaceName, accessLevel)
//        })
//      }
//    }
//
//    groupRefs.flatMap { refs =>
//      Future.sequence(refs.map { ref =>
//        updateGroupMembers(ref, RawlsGroupMemberList(userSubjectIds = Option(Seq(user.userSubjectId.value))), RawlsGroupMemberList())
//      })
//    } flatMap { _ =>
//      dataSource.inTransaction { dataAccess =>
//        dataAccess.workspaceQuery.deleteWorkspaceInvitesForUser(user.userEmail)
//      }
//    }
//  }

  private def loadUser(userRef: RawlsUserRef): Future[RawlsUser] = dataSource.inTransaction { dataAccess => withUser(userRef, dataAccess)(DBIO.successful) }


  private def verifyNoSubmissions(userRef: RawlsUserRef, dataAccess: DataAccess): ReadAction[Unit] = {
    dataAccess.submissionQuery.findBySubmitter(userRef.userSubjectId.value).exists.result flatMap {
      case false => DBIO.successful(())
      case _ => DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Cannot delete a user with submissions")))
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
        case SamResourceIdWithPolicyName(resourceId, "owner") => (resourceId, ProjectRoles.Owner)
        case SamResourceIdWithPolicyName(resourceId, "workspace-creator") => (resourceId, ProjectRoles.User)
      }.flatMap { case (resourceId, role) =>
        projectDetailsByName.get(resourceId).map { case (projectStatus, message) =>
          RawlsBillingProjectMembership(RawlsBillingProjectName(resourceId), role, projectStatus, message)
        }
      }.toList.sortBy(_.projectName.value)
    }

    membershipsFuture.map(RequestComplete(_))
  }

  def getBillingProjectMembers(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    samDAO.getResourcePolicies(SamResourceTypeNames.billingProject, projectName.value, userInfo).map { policies =>
      for {
        (role, policy) <- policies.collect {
          case SamPolicyWithName("owner", policy) => (ProjectRoles.Owner, policy)
          case SamPolicyWithName("workspace-creator", policy) => (ProjectRoles.User, policy)
        }
        email <- policy.memberEmails
      } yield RawlsBillingProjectMember(RawlsUserEmail(email), role)
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


    for {
      _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.create(project) }

      _ <- samDAO.createResource(SamResourceTypeNames.billingProject, billingProjectName.value, ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, workspaceCreatorPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.workspaceCreator)), ownerUserInfo)
      _ <- samDAO.overwritePolicy(SamResourceTypeNames.billingProject, billingProjectName.value, canComputeUserPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser)), ownerUserInfo)
      ownerGroupEmail <- getGoogleProjectOwnerGroupEmail(samDAO, project)
      computeUserGroupEmail <- getComputeUserGroupEmail(samDAO, project)

      policiesToAdd = getDefaultGoogleProjectPolicies(ownerGroupEmail, computeUserGroupEmail)

      _ <- gcsDAO.addPolicyBindings(billingProjectName, policiesToAdd)
      _ <- gcsDAO.grantReadAccess(billingProjectName, xfer.bucket, Set(ownerGroupEmail, computeUserGroupEmail))
    } yield {
      RequestComplete(StatusCodes.Created)
    }
  }

  def addUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    val policies = projectAccessUpdate.role match {
      case ProjectRoles.Owner => Seq(ownerPolicyName)
      case ProjectRoles.User => Seq(workspaceCreatorPolicyName, canComputeUserPolicyName)
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
      case ProjectRoles.Owner => ownerPolicyName
      case ProjectRoles.User => workspaceCreatorPolicyName
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

  def requestAccessToManagedGroup(groupRef: ManagedGroupRef): Future[PerRequestMessage] = {
    val instructionsQuery = dataSource.inTransaction { dataAccess =>
      dataAccess.managedGroupQuery.getManagedGroupAccessInstructions(Set(groupRef))
    }

    instructionsQuery.flatMap { accessInstructions =>
      if(accessInstructions.nonEmpty) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "You may not request access to this group"))
      else samDAO.requestAccessToManagedGroup(WorkbenchGroupName(groupRef.membersGroupName.value), userInfo).map(_ => RequestComplete(StatusCodes.NoContent))
    }
  }

  def setManagedGroupAccessInstructions(managedGroupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.managedGroupQuery.setManagedGroupAccessInstructions(managedGroupRef, instructions).map {
        case 0 => RequestComplete(StatusCodes.InternalServerError, "We were unable to update the access instructions")
        case _ => RequestComplete(StatusCodes.NoContent)
      }
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
                    _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, workspaceCreatorPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.workspaceCreator)), userInfo))
                    _ <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, canComputeUserPolicyName, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser)), userInfo))
                    project <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, "gs://" + gcsDAO.getCromwellAuthBucketName(projectName), CreationStatuses.Creating, Option(billingAccountName), None))
                  } yield project

                case Some(_) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "project by that name already exists"))
              }
            }

            createProjectOperation <- gcsDAO.createProject(projectName, billingAccount).recoverWith {
              case t: Throwable =>
                // failed to create project in google land, rollback inserts above
                dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.delete(projectName) } map(_ => throw t)
            }

            _ <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.insertOperations(Seq(createProjectOperation))
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

  private def reduceErrorReports(errorReportOptions: Iterable[Option[ErrorReport]]): Option[ErrorReport] = {
    val errorReports = errorReportOptions.collect {
      case Some(errorReport) => errorReport
    }.toSeq

    errorReports match {
      case Seq() => None
      case Seq(single) => Option(single)
      case many => Option(ErrorReport("multiple errors", errorReports))
    }
  }

  /**
   * handles a Future [ Seq [ Try [ T ] ] ], calling success with the successful result of the tries or failure with any exceptions
   *
   * @param futures
   * @param success
   * @param failure
   * @tparam T
   * @return
   */
  private def handleFutures[T, R](futures: Future[Seq[Try[T]]])(success: Seq[T] => R, failure: Seq[Throwable] => R): Future[R] = {
    futures map { tries =>
      val exceptions = tries.collect { case Failure(t) => t }
      if (exceptions.isEmpty) {
        success(tries.map(_.get))
      } else {
        failure(exceptions)
      }
    }
  }

  private def handleException(message: String)(exceptions: Seq[Throwable]): PerRequestMessage = {
    throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, message, exceptions.map(ErrorReport(_))))
  }
}

