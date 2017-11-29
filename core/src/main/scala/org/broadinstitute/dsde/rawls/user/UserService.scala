package org.broadinstitute.dsde.rawls.user

import java.sql.SQLException
import java.util.UUID

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
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete, RequestCompleteWithLocation}
import spray.http.StatusCodes
import spray.json._
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  val allUsersGroupRef = RawlsGroupRef(RawlsGroupName("All_Users"))

  def props(userServiceConstructor: UserInfo => UserService, userInfo: UserInfo): Props = {
    Props(userServiceConstructor(userInfo))
  }

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String, notificationDAO: NotificationDAO, samDAO: SamDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, gpsDAO, gpsGroupSyncTopic, notificationDAO, samDAO)

  sealed trait UserServiceMessage
  case class SetRefreshToken(token: UserRefreshToken) extends UserServiceMessage
  case object GetRefreshTokenDate extends UserServiceMessage

  case object CreateUser extends UserServiceMessage
  case class AdminGetUserStatus(userRef: RawlsUserRef) extends UserServiceMessage
  case object UserGetUserStatus extends UserServiceMessage

  case class AdminDeleteUser(userRef: RawlsUserRef) extends UserServiceMessage
  case class ListGroupsForUser(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class GetUserGroup(groupRef: RawlsGroupRef) extends UserServiceMessage

  case class CreateManagedGroup(groupRef: ManagedGroupRef) extends UserServiceMessage
  case class GetManagedGroup(groupRef: ManagedGroupRef) extends UserServiceMessage
  case object ListManagedGroupsForUser extends UserServiceMessage
  case class RequestAccessToManagedGroup(groupRef: ManagedGroupRef) extends UserServiceMessage
  case class SetManagedGroupAccessInstructions(groupRef: ManagedGroupRef, instructions: ManagedGroupAccessInstructions) extends UserServiceMessage
  case class AddManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, email: String) extends UserServiceMessage
  case class RemoveManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, email: String) extends UserServiceMessage
  case class OverwriteManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class DeleteManagedGroup(groupRef: ManagedGroupRef) extends UserServiceMessage

  case class AdminDeleteRefreshToken(userRef: RawlsUserRef) extends UserServiceMessage
  case object AdminDeleteAllRefreshTokens extends UserServiceMessage

  case object ListBillingProjects extends UserServiceMessage
  case class AddUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) extends UserServiceMessage
  case class RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate) extends UserServiceMessage
  case object ListBillingAccounts extends UserServiceMessage

  case class CreateBillingProjectFull(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName) extends UserServiceMessage
  case class GetBillingProjectMembers(projectName: RawlsBillingProjectName) extends UserServiceMessage

  case class AdminCreateGroup(groupRef: RawlsGroupRef) extends UserServiceMessage
  case class AdminListGroupMembers(groupRef: RawlsGroupRef) extends UserServiceMessage
  case class AdminDeleteGroup(groupRef: RawlsGroupRef) extends UserServiceMessage
  case class AdminOverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AdminAddGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AddGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AdminRemoveGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class RemoveGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AdminSynchronizeGroupMembers(groupRef: RawlsGroupRef) extends UserServiceMessage
  case class InternalSynchronizeGroupMembers(groupRef: RawlsGroupRef) extends UserServiceMessage

  case class IsAdmin(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class IsLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminAddLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String, notificationDAO: NotificationDAO, samDAO: SamDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with RoleSupport with FutureSupport with UserWiths with LazyLogging {

  import dataSource.dataAccess.driver.api._
  import spray.json.DefaultJsonProtocol._

  override def receive = {
    case SetRefreshToken(token) => setRefreshToken(token) pipeTo sender
    case GetRefreshTokenDate => getRefreshTokenDate() pipeTo sender

    case CreateUser => createUser() pipeTo sender
    case AdminDeleteUser(userRef) => asFCAdmin { deleteUser(userRef) } pipeTo sender
    case ListGroupsForUser(userEmail) => listGroupsForUser(userEmail) pipeTo sender
    case GetUserGroup(groupRef) => getUserGroup(groupRef) pipeTo sender

    case ListBillingProjects => listBillingProjects pipeTo sender

    case AddUserToBillingProject(projectName, projectAccessUpdate) => requireProjectAction(projectName, SamResourceActions.alterPolicies) { addUserToBillingProject(projectName, projectAccessUpdate) } pipeTo sender
    case RemoveUserFromBillingProject(projectName, projectAccessUpdate) => requireProjectAction(projectName, SamResourceActions.alterPolicies) { removeUserFromBillingProject(projectName, projectAccessUpdate) } pipeTo sender
    case ListBillingAccounts => listBillingAccounts() pipeTo sender

    case AdminCreateGroup(groupRef) => asFCAdmin { createGroup(groupRef) } pipeTo sender
    case AdminListGroupMembers(groupRef) => asFCAdmin { listGroupMembers(groupRef) } pipeTo sender
    case AdminDeleteGroup(groupName) => asFCAdmin { deleteGroup(groupName) } pipeTo sender
    case AdminOverwriteGroupMembers(groupName, memberList) => asFCAdmin { overwriteGroupMembers(groupName, memberList) } to sender

    case CreateManagedGroup(groupRef) => createManagedGroup(groupRef) pipeTo sender
    case GetManagedGroup(groupRef) => getManagedGroup(groupRef) pipeTo sender
    case RequestAccessToManagedGroup(groupRef) => requestAccessToManagedGroup(groupRef) pipeTo sender
    case SetManagedGroupAccessInstructions(groupRef, instructions) => asFCAdmin { setManagedGroupAccessInstructions(groupRef, instructions) } pipeTo sender
    case ListManagedGroupsForUser => listManagedGroupsForUser pipeTo sender
    case AddManagedGroupMembers(groupRef, role, email) => addManagedGroupMembers(groupRef, role, email) pipeTo sender
    case RemoveManagedGroupMembers(groupRef, role, email) => removeManagedGroupMembers(groupRef, role, email) pipeTo sender
    case OverwriteManagedGroupMembers(groupRef, role, memberList) => overwriteManagedGroupMembers(groupRef, role, memberList) pipeTo sender
    case DeleteManagedGroup(groupRef) => deleteManagedGroup(groupRef) pipeTo sender

    case CreateBillingProjectFull(projectName, billingAccount) => startBillingProjectCreation(projectName, billingAccount) pipeTo sender
    case GetBillingProjectMembers(projectName) => requireProjectAction(projectName, SamResourceActions.readPolicies) { getBillingProjectMembers(projectName) } pipeTo sender

    case OverwriteGroupMembers(groupName, memberList) => overwriteGroupMembers(groupName, memberList) to sender
    case AdminAddGroupMembers(groupName, memberList) => asFCAdmin { updateGroupMembers(groupName, addMemberList = memberList) } to sender
    case AdminRemoveGroupMembers(groupName, memberList) => asFCAdmin { updateGroupMembers(groupName, removeMemberList = memberList) } to sender
    case AddGroupMembers(groupName, memberList) => updateGroupMembers(groupName, addMemberList = memberList) to sender
    case RemoveGroupMembers(groupName, memberList) => updateGroupMembers(groupName, removeMemberList = memberList) to sender
    case AdminSynchronizeGroupMembers(groupRef) => asFCAdmin { synchronizeGroupMembersApi(groupRef) } pipeTo sender
    case InternalSynchronizeGroupMembers(groupRef) => synchronizeGroupMembers(groupRef) pipeTo sender

    case AdminDeleteRefreshToken(userRef) => asFCAdmin { deleteRefreshToken(userRef) } pipeTo sender
    case AdminDeleteAllRefreshTokens => asFCAdmin { deleteAllRefreshTokens() } pipeTo sender

    case IsAdmin(userEmail) => { isAdmin(userEmail) } pipeTo sender
    case IsLibraryCurator(userEmail) => { isLibraryCurator(userEmail) } pipeTo sender
    case AdminAddLibraryCurator(userEmail) => asFCAdmin { addLibraryCurator(userEmail) } pipeTo sender
    case AdminRemoveLibraryCurator(userEmail) => asFCAdmin { removeLibraryCurator(userEmail) } pipeTo sender
  }

  def requireProjectAction(projectName: RawlsBillingProjectName, action: SamResourceActions.SamResourceAction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    samDAO.userHasAction(SamResourceTypeNames.billingProject, projectName.value, SamResourceActions.createWorkspace, userInfo).flatMap {
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
  def createUser(): Future[PerRequestMessage] = {
    val user = RawlsUser(userInfo)

    handleFutures(Future.sequence(Seq(toFutureTry(turnInvitesIntoRealAccess(user)))))(_ => {
      notificationDAO.fireAndForgetNotification(ActivationNotification(user.userSubjectId))
      RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId.value}")
    }, handleException("Errors creating user"))
  }

  def turnInvitesIntoRealAccess(user: RawlsUser) = {
    val groupRefs = dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.findWorkspaceInvitesForUser(user.userEmail).flatMap { invites =>
        DBIO.sequence(invites.map { case (workspaceName, accessLevel) =>
          dataAccess.workspaceQuery.loadAccessGroup(workspaceName, accessLevel)
        })
      }
    }

    groupRefs.flatMap { refs =>
      Future.sequence(refs.map { ref =>
        updateGroupMembers(ref, RawlsGroupMemberList(userSubjectIds = Option(Seq(user.userSubjectId.value))), RawlsGroupMemberList())
      })
    } flatMap { _ =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.deleteWorkspaceInvitesForUser(user.userEmail)
      }
    }
  }

  def getUserGroup(rawlsGroupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.loadGroupIfMember(rawlsGroupRef, RawlsUser(userInfo)) map {
        case None => RequestComplete(ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found or member not in group"))
        case Some(group) => RequestComplete(group.toRawlsGroupShort)
      }
    }
  }

  private def loadUser(userRef: RawlsUserRef): Future[RawlsUser] = dataSource.inTransaction { dataAccess => withUser(userRef, dataAccess)(DBIO.successful) }

  def listGroupsForUser(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withUser(userEmail, dataAccess) { user =>
        for {
          groups <- dataAccess.rawlsGroupQuery.listGroupsForUser(user)
        } yield {
          groups map {ref => ref.groupName.value}
        }
      }
    } map(RequestComplete(_))
  }

  private def verifyNoSubmissions(userRef: RawlsUserRef, dataAccess: DataAccess): ReadAction[Unit] = {
    dataAccess.submissionQuery.findBySubmitter(userRef.userSubjectId.value).exists.result flatMap {
      case false => DBIO.successful(())
      case _ => DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Cannot delete a user with submissions")))
    }
  }

  private def deleteUserFromDB(userRef: RawlsUserRef): Future[Unit] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsUserQuery.deleteUser(userRef.userSubjectId)
    }
  }

  def deleteUser(userRef: RawlsUserRef): Future[PerRequestMessage] = {

    // 1. load user from DB
    // 2. in Futures, can be parallel: remove user from aux DB tables, User Directory, and Proxy Group, revoke & delete refresh token
    // 3. only remove user from DB if/when all of #2 succeed, because failures mean we need to keep the DB user around for subsequent attempts

    val userF = loadUser(userRef)

    val dbTablesRemoval = dataSource.inTransaction { dataAccess =>
      for {
        _ <- verifyNoSubmissions(userRef, dataAccess)
        _ <- dataAccess.rawlsGroupQuery.removeUserFromAllGroups(userRef)
      } yield ()
    }

    val proxyGroupDeletion = userF.flatMap(gcsDAO.deleteProxyGroup) recover { case e: HttpResponseException if e.getStatusCode == 404 => Unit }

    for {
      _ <- Future.sequence(Seq(dbTablesRemoval, proxyGroupDeletion, deleteRefreshTokenInternal(userRef)))
      _ <- deleteUserFromDB(userRef)
    } yield RequestComplete(StatusCodes.NoContent)
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

  def listBillingProjects(): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      DBIO.from(samDAO.getPoliciesForType(SamResourceTypeNames.billingProject, userInfo)).flatMap { resourceIdsWithPolicyNames =>
        dataAccess.rawlsBillingProjectQuery.getBillingProjectDetails(resourceIdsWithPolicyNames.map(idWithPolicyName => RawlsBillingProjectName(idWithPolicyName.resourceId))).map { projectDetails =>
          resourceIdsWithPolicyNames.map { idWithPolicyName =>
            RawlsBillingProjectMembership(RawlsBillingProjectName(idWithPolicyName.resourceId), ProjectRoles.withName(idWithPolicyName.accessPolicyName), projectDetails(idWithPolicyName.resourceId)._1, projectDetails(idWithPolicyName.resourceId)._2)
          }
        }
      }.map(RequestComplete(_))
    }
  }

  def getBillingProjectMembers(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    samDAO.getResourcePolicies(SamResourceTypeNames.billingProject, projectName.value, userInfo).map { policies =>
      for {
        policyWithName <- policies
        email <- policyWithName.policy.memberEmails
      } yield RawlsBillingProjectMember(RawlsUserEmail(email), ProjectRoles.withName(policyWithName.policyName))
    }.map(RequestComplete(_))
  }

  def deleteBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    // delete actual project in google-y way, then remove from Rawls DB and Sam
    gcsDAO.deleteProject(projectName) flatMap {
      _ => unregisterBillingProject(projectName)
    }
  }

  def unregisterBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    val isDeleted = dataSource.inTransaction { dataAccess =>
      withBillingProject(projectName, dataAccess) { project =>
        dataAccess.rawlsBillingProjectQuery.delete(project.projectName)
      }
    }

    //make sure we actually deleted it in the rawls database before destroying the permissions in Sam
    isDeleted.flatMap {
      case true => samDAO.deleteResource(SamResourceTypeNames.billingProject, projectName.value, userInfo)
      case false => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"Could not delete billing project [${projectName.value}]"))
    }.map(_ => RequestComplete(StatusCodes.OK))
  }

  def addUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    samDAO.addUserToPolicy(SamResourceTypeNames.billingProject, projectName.value, projectAccessUpdate.role.toString, projectAccessUpdate.email, userInfo).map(_ => RequestComplete(StatusCodes.OK))
  }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    samDAO.removeUserFromPolicy(SamResourceTypeNames.billingProject, projectName.value, projectAccessUpdate.role.toString, projectAccessUpdate.email, userInfo).map(_ => RequestComplete(StatusCodes.OK))
  }

  def listGroupMembers(groupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.load(groupRef) flatMap {
        case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Group ${groupRef.groupName.value} does not exist")))
        case Some(group) =>
          dataAccess.rawlsGroupQuery.loadMemberEmails(group).map(memberEmails => RequestComplete(StatusCodes.OK, UserList(memberEmails)))
      }
    }
  }

  def createGroup(groupRef: RawlsGroupRef) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.load(groupRef) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Group ${groupRef.groupName} already exists")))
        case None =>
          createGroupInternal(groupRef, dataAccess) map { _ => RequestComplete(StatusCodes.Created) }
      }
    }
  }

  private def createGroupInternal(groupRef: RawlsGroupRef, dataAccess: DataAccess): ReadWriteAction[RawlsGroup] = {
    DBIO.from(gcsDAO.createGoogleGroup(groupRef)).flatMap { rawlsGroup =>
      dataAccess.rawlsGroupQuery.save(rawlsGroup)
    }
  }

  def deleteGroup(groupRef: RawlsGroupRef) = {
    for {
      group <- dataSource.inTransaction { dataAccess =>
        withGroup(groupRef, dataAccess) { group =>
          dataAccess.rawlsGroupQuery.delete(groupRef) map { _ => group }
        }
      }
      _ <- gcsDAO.deleteGoogleGroup(group)
    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def overwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList): Future[PerRequestMessage] = {
    for {
      (users, groups) <- dataSource.inTransaction({ dataAccess =>
        withGroup(groupRef, dataAccess) { group =>
          loadMemberUsersAndGroups(memberList, dataAccess)
        }
      }, TransactionIsolation.ReadCommitted) // read committed required to reduce db locks and allow concurrency

      _ <- overwriteGroupMembership(groupRef, users.map(RawlsUser.toRef), groups.map(RawlsGroup.toRef))
    } yield RequestComplete(StatusCodes.NoContent)
  }

  def updateGroupMembership(groupRef: RawlsGroupRef, addUsers: Set[RawlsUserRef] = Set.empty, removeUsers: Set[RawlsUserRef] = Set.empty, addSubGroups: Set[RawlsGroupRef] = Set.empty, removeSubGroups: Set[RawlsGroupRef] = Set.empty): Future[RawlsGroup] = {
    updateGroupMembershipInternal(groupRef) { group =>
      group.copy(
        users = group.users ++ addUsers -- removeUsers,
        subGroups = group.subGroups ++ addSubGroups -- removeSubGroups
      )
    }
  }

  /** completely overwrites all group members */
  def overwriteGroupMembership(groupRef: RawlsGroupRef, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): Future[RawlsGroup] = {
    updateGroupMembershipInternal(groupRef) { group =>
      group.copy(users = users, subGroups = subGroups)
    }
  }

  def createManagedGroup(groupRef: ManagedGroupRef):  Future[PerRequestMessage] = {
    val userDefinedRegex = "[A-z0-9_-]+".r
    if(! userDefinedRegex.pattern.matcher(groupRef.membersGroupName.value).matches) {
      val msg = s"Invalid input: ${groupRef.membersGroupName}. Input may only contain alphanumeric characters, underscores, and dashes."
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = msg, statusCode = StatusCodes.BadRequest))
    }
    if(groupRef.membersGroupName.value.length > 50) throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"Invalid input: ${groupRef.membersGroupName}. Input may be a max of 50 characters.", statusCode = StatusCodes.BadRequest))
    val usersGroupRef = groupRef.toMembersGroupRef
    val ownersGroupRef = RawlsGroupRef(RawlsGroupName(groupRef.membersGroupName.value + "-owners"))
    for {
      managedGroup <- createManagedGroupInternal(usersGroupRef, ownersGroupRef)
      _ <- updateGroupMembership(managedGroup.adminsGroup, addUsers = Set(RawlsUser(userInfo)))
      _ <- updateGroupMembership(managedGroup.membersGroup, addSubGroups = Set(managedGroup.adminsGroup))
    } yield {
      RequestComplete(StatusCodes.Created, ManagedGroupWithMembers(managedGroup.membersGroup.toRawlsGroupShort, managedGroup.adminsGroup.toRawlsGroupShort, Seq(managedGroup.adminsGroup.groupEmail.value), Seq(userInfo.userEmail.value)))
    }
  }

  private def createManagedGroupInternal(usersGroupRef: RawlsGroupRef, ownersGroupRef: RawlsGroupRef): Future[ManagedGroup] = {
    dataSource.inTransaction { dataAccess =>
      val existingGroups = for {
        preexistingUsersGroup <- dataAccess.rawlsGroupQuery.load(usersGroupRef)
        preexistingOwnersGroup <- dataAccess.rawlsGroupQuery.load(ownersGroupRef)
      } yield {
          (preexistingUsersGroup, preexistingOwnersGroup)
        }

      existingGroups.flatMap {
        case (None, None) =>
          for {
            usersGroup <- createGroupInternal(usersGroupRef, dataAccess)
            ownersGroup <- createGroupInternal(ownersGroupRef, dataAccess)
            managedGroup <- dataAccess.managedGroupQuery.createManagedGroup(ManagedGroup(usersGroup, ownersGroup))
          } yield {
            managedGroup
          }

        case _ => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "please choose a different name"))
      }
    }
  }

  def getManagedGroup(groupRef: ManagedGroupRef):  Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withManagedGroupOwnerAccess(groupRef, RawlsUser(userInfo), dataAccess) { managedGroup =>
        for {
          usersEmails <- dataAccess.rawlsGroupQuery.loadMemberEmails(managedGroup.membersGroup)
          ownersEmails <- dataAccess.rawlsGroupQuery.loadMemberEmails(managedGroup.adminsGroup)
        } yield {
          // we want to hide the fact that the users group contains the owners group because this structure is
          // confusing to the user even though the functionality is desired
          val userEmailsSansOwnerGroup = usersEmails.filterNot(_ == managedGroup.adminsGroup.groupEmail.value)
          RequestComplete(ManagedGroupWithMembers(managedGroup.membersGroup.toRawlsGroupShort, managedGroup.adminsGroup.toRawlsGroupShort, userEmailsSansOwnerGroup, ownersEmails))
        }
      }
    }
  }

  def listManagedGroupsForUser(): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      for {
        groupsWithAccess <- dataAccess.managedGroupQuery.listManagedGroupsForUser(RawlsUserRef(userInfo.userSubjectId))
        emailsByGroup <- dataAccess.rawlsGroupQuery.loadEmails(groupsWithAccess.map(_.managedGroupRef.toMembersGroupRef).toSeq)
      } yield {
        val response = groupsWithAccess.groupBy(_.managedGroupRef).map { case (groupRef, accessEntries) =>
          ManagedGroupAccessResponse(groupRef.membersGroupName, emailsByGroup(groupRef.toMembersGroupRef), accessEntries.map(_.role).max)
        }
        RequestComplete(StatusCodes.OK, response)
      }
    }
  }

  def requestAccessToManagedGroup(groupRef: ManagedGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      val query = for {
        group <- dataAccess.managedGroupQuery.load(groupRef)
        accessInstructions <- dataAccess.managedGroupQuery.getManagedGroupAccessInstructions(Set(groupRef))
      } yield (group, accessInstructions)

      query.flatMap { case (group, accessInstructions) =>
        group match {
          case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"The group [${groupRef.membersGroupName.value}] was not found"))
          case Some(managedGroup) =>
            if (accessInstructions.nonEmpty) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "You may not request access to this group"))
            else {
              dataAccess.rawlsGroupQuery.flattenGroupMembership(managedGroup.adminsGroup).map { users =>
                users.foreach { user =>
                  notificationDAO.fireAndForgetNotification(GroupAccessRequestNotification(user.userSubjectId, groupRef.membersGroupName.value, users.map(_.userSubjectId) + userInfo.userSubjectId, userInfo.userSubjectId))
                }
                RequestComplete(StatusCodes.NoContent)
              }
            }
        }
      }
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

  def addManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, email: String): Future[PerRequestMessage] = {
    loadRefList(email) flatMap { addMemberList => updateManagedGroupMembers(groupRef, role, addMemberList = addMemberList) }
  }

  def removeManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, email: String): Future[PerRequestMessage] = {
    loadRefList(email) flatMap { removeMemberList => updateManagedGroupMembers(groupRef, role, removeMemberList = removeMemberList) }
  }

  def loadRefList(email: String): Future[RawlsGroupMemberList] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.loadRefsFromEmails(Seq(email)).map(_.values.headOption) map {
        case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"User or group with email $email not found"))
        case Some(Left(userRef)) => RawlsGroupMemberList(userSubjectIds = Option(Seq(userRef.userSubjectId.value)))
        case Some(Right(groupRef)) => RawlsGroupMemberList(subGroupNames = Option(Seq(groupRef.groupName.value)))
      }
    }
  }

  private def updateManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, addMemberList: RawlsGroupMemberList = RawlsGroupMemberList(), removeMemberList: RawlsGroupMemberList = RawlsGroupMemberList()): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withManagedGroupOwnerAccess(groupRef, RawlsUser(userInfo), dataAccess) { managedGroup =>
        if (role == ManagedRoles.Admin &&
          (removeMemberList.userEmails.getOrElse(Seq.empty).contains(userInfo.userEmail.value) ||
            removeMemberList.userSubjectIds.getOrElse(Seq.empty).contains(userInfo.userSubjectId.value))) {

          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "You may not remove your own access."))
        }

        DBIO.successful(rawlsGroupForRole(role, managedGroup))
      }
    } flatMap { rawlsGroup =>
      updateGroupMembers(rawlsGroup, addMemberList, removeMemberList)
    }
  }

  private def rawlsGroupForRole(role: ManagedRole, managedGroup: ManagedGroup): RawlsGroup = {
    role match {
      case ManagedRoles.Admin => managedGroup.adminsGroup
      case ManagedRoles.Member => managedGroup.membersGroup
    }
  }

  def overwriteManagedGroupMembers(groupRef: ManagedGroupRef, role: ManagedRole, memberList: RawlsGroupMemberList): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withManagedGroupOwnerAccess(groupRef, RawlsUser(userInfo), dataAccess) { managedGroup =>
        if (role == ManagedRoles.Admin &&
          !memberList.userEmails.getOrElse(Seq.empty).contains(userInfo.userEmail.value) &&
            !memberList.userSubjectIds.getOrElse(Seq.empty).contains(userInfo.userSubjectId.value)) {

          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "You may not remove your own access."))
        }

        DBIO.successful(rawlsGroupForRole(role, managedGroup))
      }
    } flatMap { rawlsGroup =>
      overwriteGroupMembers(rawlsGroup, memberList)
    }
  }

  def deleteManagedGroup(groupRef: ManagedGroupRef) = {
    for{
      _ <- dataSource.inTransaction { dataAccess =>
        for {
          groupToCheck <- dataAccess.managedGroupQuery.load(groupRef).map(_.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Group does not exist,"))))
          admins <- dataAccess.rawlsGroupQuery.listAncestorGroups(groupToCheck.adminsGroup.groupName)
          members <- dataAccess.rawlsGroupQuery.listAncestorGroups(groupToCheck.membersGroup.groupName)
        } yield {
          if ((admins - groupToCheck.membersGroup.groupName).nonEmpty || members.nonEmpty) {
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"Cannot delete group because it is in use. Admins: ${admins} Members: ${members}"))
          }
        }
      }
      groupEmailsToDelete <- dataSource.inTransaction { dataAccess =>
        withManagedGroupOwnerAccess(groupRef, RawlsUser(userInfo), dataAccess) { managedGroup =>
          DBIO.seq(
            dataAccess.managedGroupQuery.deleteManagedGroup(groupRef),
            dataAccess.rawlsGroupQuery.delete(managedGroup.membersGroup),
            dataAccess.rawlsGroupQuery.delete(managedGroup.adminsGroup)
          ).map(_ => Seq(managedGroup.membersGroup, managedGroup.adminsGroup))
        }
      }
      _ <- Future.traverse(groupEmailsToDelete) { group =>
        gcsDAO.deleteGoogleGroup(group).recover {
          // log any exception but ignore
          case t: Throwable => logger.error(s"error deleting google group $group", t)
        }
      }
    } yield {
      RequestComplete(StatusCodes.NoContent)
    }
  }


  /**
   * Internal function to update a group
   *
   * @param groupRef group to update
   * @param update function that takes the existing group as input and should produce an updated version to be saved
   * @return
   */
  private def updateGroupMembershipInternal(groupRef: RawlsGroupRef)(update: RawlsGroup => RawlsGroup): Future[RawlsGroup] = {
    for {
      (savedGroup, intersectionGroups) <- dataSource.inTransaction ({ dataAccess =>
        for {
          groupOption <- dataAccess.rawlsGroupQuery.load(groupRef)
          group = groupOption.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"group ${groupRef.groupName.value} not found")))
          updatedGroup = update(group)
          savedGroup <- dataAccess.rawlsGroupQuery.save(updatedGroup)

          // update intersection groups associated with groupRef
          groupsToIntersects <- dataAccess.workspaceQuery.findAssociatedGroupsToIntersect(savedGroup)
          intersectionGroups <- updateIntersectionGroupMembers(groupsToIntersects.toSet, dataAccess)
        } yield (savedGroup, intersectionGroups)
      }, TransactionIsolation.ReadCommitted)

      messages = (intersectionGroups.toSeq :+ RawlsGroup.toRef(savedGroup)).map(_.toJson.compactPrint)

      _ <- gpsDAO.publishMessages(gpsGroupSyncTopic, messages)
    } yield savedGroup
  }

  private def loadMemberUsersAndGroups(memberList: RawlsGroupMemberList, dataAccess: DataAccess): ReadWriteAction[(Set[RawlsUser], Set[RawlsGroup])] = {
    val userQueriesByEmail = for {
      email <- memberList.userEmails.getOrElse(Seq.empty)
    } yield dataAccess.rawlsUserQuery.loadUserByEmail(RawlsUserEmail(email)).map((email, _))

    val userQueriesBySub = for {
      sub <- memberList.userSubjectIds.getOrElse(Seq.empty)
    } yield dataAccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(sub))).map((sub, _))

    val userQueries = DBIO.sequence(userQueriesByEmail ++ userQueriesBySub)

    val groupQueriesByEmail = for {
      email <- memberList.subGroupEmails.getOrElse(Seq.empty)
    } yield dataAccess.rawlsGroupQuery.loadGroupByEmail(RawlsGroupEmail(email)).map((email, _))

    val groupQueriesByName = for {
      name <- memberList.subGroupNames.getOrElse(Seq.empty)
    } yield dataAccess.rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(name))).map((name, _))

    val subGroupQueries = DBIO.sequence(groupQueriesByEmail ++ groupQueriesByName)

    for {
      users <- userQueries
      subGroups <- subGroupQueries
    } yield {
      (users.collect { case (email, None) => email }, subGroups.collect { case (email, None) => email }) match {
        // success case, all users and groups found
        case (Seq(), Seq()) => (users.map(_._2.get).toSet, subGroups.map(_._2.get).toSet)

        // failure cases, some users and/or groups not found
        case (Seq(), missingGroups) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some groups not found: ${missingGroups.mkString(", ")}"))
        case (missingUsers, Seq()) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}"))
        case (missingUsers, missingGroups) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}. Some groups not found: ${missingGroups.mkString(", ")}"))
      }
    }
  }

  def updateGroupMembers(groupRef: RawlsGroupRef, addMemberList: RawlsGroupMemberList = RawlsGroupMemberList(), removeMemberList: RawlsGroupMemberList = RawlsGroupMemberList()): Future[PerRequestMessage] = {
    for {
      (addUsers, addSubGroups, removeUsers, removeSubGroups) <- dataSource.inTransaction { dataAccess =>
        for {
          (addUsers, addSubGroups) <- loadMemberUsersAndGroups(addMemberList, dataAccess)
          (removeUsers, removeSubGroups) <- loadMemberUsersAndGroups(removeMemberList, dataAccess)
        } yield (addUsers, addSubGroups, removeUsers, removeSubGroups)
      }
      _ <- updateGroupMembership(groupRef,
        addUsers = addUsers.map(RawlsUser.toRef),
        addSubGroups = addSubGroups.map(RawlsGroup.toRef),
        removeUsers = removeUsers.map(RawlsUser.toRef),
        removeSubGroups = removeSubGroups.map(RawlsGroup.toRef)
      )
      
    } yield RequestComplete(StatusCodes.NoContent)
  }

  def synchronizeGroupMembersApi(groupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    synchronizeGroupMembers(groupRef) map { syncReport =>
      val statusCode = if (syncReport.items.exists(_.errorReport.isDefined)) {
        StatusCodes.BadGateway // status 500 is used for all other errors, 502 seems like the best otherwise
      } else {
        StatusCodes.OK
      }
      RequestComplete(statusCode, syncReport)
    }
  }

  def synchronizeGroupMembers(groupRef: RawlsGroupRef): Future[SyncReport] = {
    dataSource.inTransaction { dataAccess =>
      withGroup(groupRef, dataAccess) { group =>
        synchronizeGroupMembersInternal(group, dataAccess)
      }
    }
  }

  def synchronizeGroupMembersInternal(group: RawlsGroup, dataAccess: DataAccess): ReadWriteAction[SyncReport] = {
    def loadRefs(refs: Set[Either[RawlsUserRef, RawlsGroupRef]]) = {
      DBIO.sequence(refs.map {
        case Left(userRef) => dataAccess.rawlsUserQuery.load(userRef).map(userOption => Left(userOption.getOrElse(throw new RawlsException(s"user $userRef not found"))))
        case Right(groupRef) => dataAccess.rawlsGroupQuery.load(groupRef).map(groupOption => Right(groupOption.getOrElse(throw new RawlsException(s"group $groupRef not found"))))
      }.toSeq)
    }

    def toSyncReportItem(operation: String, email: String, result: Try[Unit]) = {
      SyncReportItem(
        operation,
        email,
        result match {
          case Success(_) => None
          case Failure(t) => Option(ErrorReport(t))
        }
      )
    }

    DBIO.from(gcsDAO.listGroupMembers(group)) flatMap {
      case None => DBIO.from(gcsDAO.createGoogleGroup(group) map (_ => Map.empty[String, Option[Either[RawlsUserRef, RawlsGroupRef]]]))
      case Some(members) => DBIO.successful(members)
    } flatMap { membersByEmail =>

      val knownEmailsByMember = membersByEmail.collect { case (email, Some(member)) => (member, email) }
      val unknownEmails = membersByEmail.collect { case (email, None) => email }

      val toRemove = knownEmailsByMember.keySet -- group.users.map(Left(_)) -- group.subGroups.map(Right(_))
      val emailsToRemove = unknownEmails ++ toRemove.map(knownEmailsByMember)
      val removeFutures = DBIO.sequence(emailsToRemove map { removeMember =>
        DBIO.from(toFutureTry(gcsDAO.removeEmailFromGoogleGroup(group.groupEmail.value, removeMember)).map(toSyncReportItem("removed", removeMember, _)))
      })


      val realMembers: Set[Either[RawlsUserRef, RawlsGroupRef]] = group.users.map(Left(_)) ++ group.subGroups.map(Right(_))
      val toAdd = realMembers -- knownEmailsByMember.keySet
      val addFutures = loadRefs(toAdd) flatMap { addMembers =>
        DBIO.sequence(addMembers map { addMember =>
          val memberEmail = addMember match {
            case Left(user) => user.userEmail.value
            case Right(subGroup) => subGroup.groupEmail.value
          }
          DBIO.from(toFutureTry(gcsDAO.addMemberToGoogleGroup(group, addMember)).map(toSyncReportItem("added", memberEmail, _)))
        })
      }

      for {
        syncReportItems <- DBIO.sequence(Seq(removeFutures, addFutures))
        _ <- dataAccess.rawlsGroupQuery.updateSynchronizedDate(group)
      } yield {
        SyncReport(group.groupEmail, syncReportItems.flatten)
      }
    }
  }

  def updateIntersectionGroupMembers(groupsToIntersect: Set[GroupsToIntersect], dataAccess:DataAccess): ReadWriteAction[Iterable[RawlsGroupRef]] = {

    val allGroupRefs = groupsToIntersect.flatMap(_.groups)

    dataAccess.rawlsGroupQuery.loadGroupsRecursive(allGroupRefs).flatMap { allGroups =>
      // load all the groups first because this is fast and most are likely to be empty or with no subgroups
      // only with subgroups do we need to go back to rawlsGroupQuery to do the intersection (which is expensive)
      val groupsByName = allGroups.map(g => g.groupName -> g).toMap

      // this set makes sure to query once per set of groups
      val intersectionsToMake = Set() ++ groupsToIntersect.map(_.groups)
      val intersections = DBIO.sequence(intersectionsToMake.toSeq.map { groups =>
        // this is the right thing to call when we know how to make it perform well
        // dataAccess.rawlsGroupQuery.intersectGroupMembership(groups).map(members => groups -> members)
        DBIO.successful(groups -> groups.map(g => dataAccess.rawlsGroupQuery.flattenGroup(groupsByName(g.groupName), groupsByName)).reduce(_ intersect _))
      })

      val intersectionMemberships = intersections.map { sourceGroupsWithMembers =>
        val membersBySourceGroups = sourceGroupsWithMembers.toMap
        groupsToIntersect.map { gti =>
          gti.target -> membersBySourceGroups(gti.groups)
        }.toSeq
      }

      intersectionMemberships.flatMap(dataAccess.rawlsGroupQuery.overwriteGroupUsers).map(_ => groupsToIntersect.map(_.target))
    }
  }

  def deleteRefreshToken(rawlsUserRef: RawlsUserRef): Future[PerRequestMessage] = {
    deleteRefreshTokenInternal(rawlsUserRef).map(_ => RequestComplete(StatusCodes.OK))

  }

  def deleteAllRefreshTokens(): Future[PerRequestMessage] = {
    for {
      users <- dataSource.inTransaction { _.rawlsUserQuery.loadAllUsers() }
      tries <- Future.traverse(users) { user => toFutureTry(deleteRefreshTokenInternal(user)) }
    } yield {
      val errors = tries.collect {
        case Failure(t) => ErrorReport(t)
      }
      if (errors.isEmpty) {
        RequestComplete(StatusCodes.OK)
      } else {
        RequestComplete(ErrorReport("exceptions revoking/deleting some tokens", errors))
      }
    }
  }

  def startBillingProjectCreation(projectName: RawlsBillingProjectName, billingAccountName: RawlsBillingAccountName): Future[PerRequestMessage] = {
    gcsDAO.listBillingAccounts(userInfo) flatMap { billingAccountNames =>
      billingAccountNames.find(_.accountName == billingAccountName) match {
        case Some(billingAccount) if billingAccount.firecloudHasAccess =>
          for {
            _ <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
                case None =>
                  for {
                    resource <- DBIO.from(samDAO.createResource(SamResourceTypeNames.billingProject, projectName.value, userInfo))
                    userPolicy <- DBIO.from(samDAO.overwritePolicy(SamResourceTypeNames.billingProject, projectName.value, ProjectRoles.User.toString.toLowerCase, SamPolicy(Seq.empty, Seq.empty, Seq(SamProjectRoles.batchComputeUser, SamProjectRoles.workspaceCreator, SamProjectRoles.notebookUser)), userInfo))
                    groupEmail <- DBIO.from(samDAO.syncPolicyToGoogle(SamResourceTypeNames.billingProject, projectName.value, SamProjectRoles.owner, userInfo)).map(_.keys.headOption.getOrElse(throw new RawlsException("Error getting owner policy email")))
                    project <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, RawlsGroup(RawlsGroupName(SamProjectRoles.owner), groupEmail, Set.empty, Set.empty), "gs://" + gcsDAO.getCromwellAuthBucketName(projectName), CreationStatuses.Creating, Option(billingAccountName), None))
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
        case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, s"You must be a billing administrator of ${billingAccountName.value} to create a project with it.")))
        case Some(billingAccount) if !billingAccount.firecloudHasAccess => Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, s"${gcsDAO.billingEmail} must be a billing administrator of ${billingAccountName.value} to create a project with it.")))
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
  private def handleFutures[T](futures: Future[Seq[Try[T]]])(success: Seq[T] => PerRequestMessage, failure: Seq[Throwable] => PerRequestMessage): Future[PerRequestMessage] = {
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

