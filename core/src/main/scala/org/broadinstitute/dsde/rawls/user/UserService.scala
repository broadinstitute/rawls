package org.broadinstitute.dsde.rawls.user

import _root_.slick.jdbc.TransactionIsolation
import akka.actor.{Actor, Props}
import akka.pattern._
import com.google.api.client.http.HttpResponseException
import com.google.api.client.auth.oauth2.TokenResponseException
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{RoleSupport, FutureSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete, RequestCompleteWithLocation}
import spray.http.StatusCodes
import spray.json._
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

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

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, userDirectoryDAO, gpsDAO, gpsGroupSyncTopic)

  sealed trait UserServiceMessage
  case class SetRefreshToken(token: UserRefreshToken) extends UserServiceMessage
  case object GetRefreshTokenDate extends UserServiceMessage

  case object CreateUser extends UserServiceMessage
  case class AdminGetUserStatus(userRef: RawlsUserRef) extends UserServiceMessage
  case object UserGetUserStatus extends UserServiceMessage
  case class AdminEnableUser(userRef: RawlsUserRef) extends UserServiceMessage
  case class AdminDisableUser(userRef: RawlsUserRef) extends UserServiceMessage
  case class AdminDeleteUser(userRef: RawlsUserRef) extends UserServiceMessage
  case class AdminAddToLDAP(userSubjectId: RawlsUserSubjectId) extends UserServiceMessage
  case class AdminRemoveFromLDAP(userSubjectId: RawlsUserSubjectId) extends UserServiceMessage
  case object AdminListUsers extends UserServiceMessage
  case class ListGroupsForUser(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class GetUserGroup(groupRef: RawlsGroupRef) extends UserServiceMessage
  case object ListRealmsForUser extends UserServiceMessage

  case class AdminDeleteRefreshToken(userRef: RawlsUserRef) extends UserServiceMessage
  case object AdminDeleteAllRefreshTokens extends UserServiceMessage

  case object ListBillingProjects extends UserServiceMessage
  case class AdminListBillingProjectsForUser(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminDeleteBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class AdminRegisterBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class AdminUnregisterBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class AdminAddUserToBillingProject(projectName: RawlsBillingProjectName, accessUpdate: ProjectAccessUpdate) extends UserServiceMessage
  case class AdminRemoveUserFromBillingProject(projectName: RawlsBillingProjectName, accessUpdate: ProjectAccessUpdate) extends UserServiceMessage
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
  case class AdminCreateRealm(groupRef: RawlsRealmRef) extends UserServiceMessage
  case class AdminDeleteRealm(groupRef: RawlsRealmRef) extends UserServiceMessage
  case object AdminListAllRealms extends UserServiceMessage

  case class IsAdmin(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class IsLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminAddLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO, gpsDAO: GooglePubSubDAO, gpsGroupSyncTopic: String)(implicit protected val executionContext: ExecutionContext) extends Actor with RoleSupport with FutureSupport with UserWiths {

  import dataSource.dataAccess.driver.api._

  override def receive = {
    case SetRefreshToken(token) => setRefreshToken(token) pipeTo sender
    case GetRefreshTokenDate => getRefreshTokenDate() pipeTo sender

    case CreateUser => createUser() pipeTo sender
    case AdminGetUserStatus(userRef) => asFCAdmin { getUserStatus(userRef) } pipeTo sender
    case UserGetUserStatus => getUserStatus() pipeTo sender
    case AdminEnableUser(userRef) => asFCAdmin { enableUser(userRef) } pipeTo sender
    case AdminDisableUser(userRef) => asFCAdmin { disableUser(userRef) } pipeTo sender
    case AdminDeleteUser(userRef) => asFCAdmin { deleteUser(userRef) } pipeTo sender
    case AdminAddToLDAP(userSubjectId) => asFCAdmin { addToLDAP(userSubjectId) } pipeTo sender
    case AdminRemoveFromLDAP(userSubjectId) => asFCAdmin { removeFromLDAP(userSubjectId) } pipeTo sender
    case AdminListUsers => asFCAdmin { listUsers() } pipeTo sender
    case ListGroupsForUser(userEmail) => listGroupsForUser(userEmail) pipeTo sender
    case GetUserGroup(groupRef) => getUserGroup(groupRef) pipeTo sender

    case ListBillingProjects => listBillingProjects(RawlsUser(userInfo).userEmail) pipeTo sender
    case AdminListBillingProjectsForUser(userEmail) => asFCAdmin { listBillingProjects(userEmail) } pipeTo sender
    case AdminDeleteBillingProject(projectName) => asFCAdmin { deleteBillingProject(projectName) } pipeTo sender
    case AdminRegisterBillingProject(projectName) => asFCAdmin { registerBillingProject(projectName) } pipeTo sender
    case AdminUnregisterBillingProject(projectName) => asFCAdmin { unregisterBillingProject(projectName) } pipeTo sender
    case AdminAddUserToBillingProject(projectName, projectAccessUpdate) => asFCAdmin { addUserToBillingProject(projectName, projectAccessUpdate) } pipeTo sender
    case AdminRemoveUserFromBillingProject(projectName, projectAccessUpdate) => asFCAdmin { removeUserFromBillingProject(projectName, projectAccessUpdate) } pipeTo sender

    case AddUserToBillingProject(projectName, projectAccessUpdate) => asProjectOwner(projectName) { addUserToBillingProject(projectName, projectAccessUpdate) } pipeTo sender
    case RemoveUserFromBillingProject(projectName, projectAccessUpdate) => asProjectOwner(projectName) { removeUserFromBillingProject(projectName, projectAccessUpdate) } pipeTo sender
    case ListBillingAccounts => listBillingAccounts() pipeTo sender

    case AdminCreateGroup(groupRef) => asFCAdmin { createGroup(groupRef) } pipeTo sender
    case AdminListGroupMembers(groupRef) => asFCAdmin { listGroupMembers(groupRef) } pipeTo sender
    case AdminDeleteGroup(groupName) => asFCAdmin { deleteGroup(groupName) } pipeTo sender
    case AdminOverwriteGroupMembers(groupName, memberList) => asFCAdmin { overwriteGroupMembers(groupName, memberList) } to sender

    case AdminCreateRealm(realmRef) => asFCAdmin { createRealm(realmRef) } pipeTo sender
    case AdminDeleteRealm(realmRef) => asFCAdmin { deleteRealm(realmRef) } pipeTo sender
    case AdminListAllRealms => asFCAdmin { listAllRealms } pipeTo sender
    case ListRealmsForUser => listRealmsForUser pipeTo sender

    case CreateBillingProjectFull(projectName, billingAccount) => startBillingProjectCreation(projectName, billingAccount) pipeTo sender
    case GetBillingProjectMembers(projectName) => asProjectOwner(projectName) { getBillingProjectMembers(projectName) } pipeTo sender

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

  def asProjectOwner(projectName: RawlsBillingProjectName)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    val isOwner = dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.hasOneOfProjectRole(projectName, RawlsUser(userInfo), Set(ProjectRoles.Owner))
    }
    isOwner flatMap {
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

  def createUser(): Future[PerRequestMessage] = {
    val user = RawlsUser(userInfo)

    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already created groups/entries are ok
    handleFutures(
      Future.sequence(Seq(
        toFutureTry(gcsDAO.createProxyGroup(user) flatMap( _ => gcsDAO.addUserToProxyGroup(user))),
        toFutureTry(for {
        // these things need to be done in order
          _ <- dataSource.inTransaction { dataAccess => dataAccess.rawlsUserQuery.save(user) }
          _ <- dataSource.inTransaction { dataAccess => getOrCreateAllUsersGroup(dataAccess) }
          _ <- updateGroupMembership(allUsersGroupRef, addUsers = Set(user))
        } yield ()),
        toFutureTry(userDirectoryDAO.createUser(user.userSubjectId) flatMap( _ => userDirectoryDAO.enableUser(user.userSubjectId)))

      )).flatMap{ _ => Future.sequence(Seq(toFutureTry(turnInvitesIntoRealAccess(user))))})(_ =>
      RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId.value}"), handleException("Errors creating user")
    )
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

  import spray.json.DefaultJsonProtocol._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsUserInfoListFormat

  def listUsers(): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.loadAllUsersAndTheirProjects map { projectsByUser =>
        val userInfoList = projectsByUser map {
          case (user, projectNames) => RawlsUserInfo(user, projectNames.toSeq)
        }
        RequestComplete(RawlsUserInfoList(userInfoList.toSeq))
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

  private def getOrCreateAllUsersGroup(dataAccess: DataAccess): ReadWriteAction[RawlsGroup] = {
    dataAccess.rawlsGroupQuery.load(allUsersGroupRef) flatMap {
      case Some(g) => DBIO.successful(g)
      case None => createGroupInternal(allUsersGroupRef, dataAccess).asTry flatMap {
        case Success(group) => DBIO.successful(group)
        case Failure(t: HttpResponseException) if t.getStatusCode == StatusCodes.Conflict.intValue =>
          // this case is where the group was not in our db but already in google
          dataAccess.rawlsGroupQuery.save(RawlsGroup(allUsersGroupRef.groupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(allUsersGroupRef.groupName)), Set.empty, Set.empty))
        case Failure(regrets) => DBIO.failed(regrets)
      }
    }
  }

  def getUserStatus(): Future[PerRequestMessage] = {
    getUserStatus(RawlsUserRef(RawlsUserSubjectId(userInfo.userSubjectId)))
  }

  private def loadUser(userRef: RawlsUserRef): Future[RawlsUser] = dataSource.inTransaction { dataAccess => withUser(userRef, dataAccess)(DBIO.successful) }

  def getUserStatus(userRef: RawlsUserRef): Future[PerRequestMessage] = loadUser(userRef) flatMap { user =>
    handleFutures(Future.sequence(Seq(
      toFutureTry(gcsDAO.isUserInProxyGroup(user).map("google" -> _)),
      toFutureTry(userDirectoryDAO.isEnabled(user.userSubjectId).map("ldap" -> _)),
      toFutureTry {
        dataSource.inTransaction { dataAccess =>
          val allUsersGroup = getOrCreateAllUsersGroup(dataAccess)
          allUsersGroup.map("allUsersGroup" -> _.users.contains(userRef))
        }
      }

    )))(statuses => RequestComplete(UserStatus(user, statuses.toMap)), handleException("Error checking if a user is enabled"))
  }

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

  def enableUser(userRef: RawlsUserRef): Future[PerRequestMessage] = {
    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already added entries are ok
    loadUser(userRef) flatMap { user =>
      handleFutures(Future.sequence(Seq(
        toFutureTry(gcsDAO.addUserToProxyGroup(user)),
        toFutureTry(userDirectoryDAO.enableUser(user.userSubjectId))

      )))(_ => RequestComplete(StatusCodes.NoContent), handleException("Errors enabling user"))
    }
  }

  def disableUser(userRef: RawlsUserRef): Future[PerRequestMessage] = {
    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already removed entries are ok
    loadUser(userRef) flatMap { user =>
      handleFutures(Future.sequence(Seq(
        toFutureTry(gcsDAO.removeUserFromProxyGroup(user)),
        toFutureTry(userDirectoryDAO.disableUser(user.userSubjectId))

      )))(_ => RequestComplete(StatusCodes.NoContent), handleException("Errors disabling user"))
    }
  }

  private def verifyNoSubmissions(userRef: RawlsUserRef, dataAccess: DataAccess): ReadAction[Unit] = {
    dataAccess.submissionQuery.findBySubmitter(userRef.userSubjectId.value).exists.result flatMap {
      case false => DBIO.successful(Unit)
      case _ => DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Cannot delete a user with submissions")))
    }
  }

  private def deleteUserFromDB(userRef: RawlsUserRef): Future[Int] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsUserQuery.findUserBySubjectId(userRef.userSubjectId.value).delete
    } flatMap {
      case 1 => Future.successful(1)
      case rowsDeleted =>
        val error = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Expected to delete 1 row from user table, but deleted $rowsDeleted"))
        Future.failed(error)
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

    val userDirectoryRemoval = for {
      _ <- userDirectoryDAO.disableUser(userRef.userSubjectId)   // may not be strictly necessary, but does not hurt
      _ <- userDirectoryDAO.removeUser(userRef.userSubjectId)
    } yield ()

    val proxyGroupDeletion = userF.flatMap(gcsDAO.deleteProxyGroup)

    for {
      _ <- Future.sequence(Seq(dbTablesRemoval, userDirectoryRemoval, proxyGroupDeletion, deleteRefreshTokenInternal(userRef)))
      _ <- deleteUserFromDB(userRef)
    } yield RequestComplete(StatusCodes.NoContent)
  }

  def addToLDAP(userSubjectId: RawlsUserSubjectId): Future[PerRequestMessage] = {
    userDirectoryDAO.createUser(userSubjectId) flatMap { _ =>
      userDirectoryDAO.enableUser(userSubjectId) } map { _ =>
        RequestComplete(StatusCodes.Created)
    }
  }

  def removeFromLDAP(userSubjectId: RawlsUserSubjectId): Future[PerRequestMessage] = {
    userDirectoryDAO.disableUser(userSubjectId) flatMap { _ =>
      userDirectoryDAO.removeUser(userSubjectId) map { _ =>
        RequestComplete(StatusCodes.NoContent)
      }
    }
  }

  def isAdmin(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(tryIsFCAdmin(userEmail.value)) map {
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
  import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsBillingProjectNameFormat

  def listBillingAccounts(): Future[PerRequestMessage] =
    gcsDAO.listBillingAccounts(userInfo) map(RequestComplete(_))

  // when called for the current user, admin access is not required
  def listBillingProjects(userEmail: RawlsUserEmail): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withUser(userEmail, dataAccess) { user =>
        for {
          groups <- dataAccess.rawlsGroupQuery.listGroupsForUser(user)
          memberships <- dataAccess.rawlsBillingProjectQuery.listProjectMembershipsForGroups(groups)
        } yield memberships
      } map(RequestComplete(_))
    }

  def getBillingProjectMembers(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.loadDirectProjectMembersWithEmail(projectName).map(RequestComplete(_))
    }
  }

  def deleteBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    // delete actual project in google-y way, then remove from Rawls DB
    gcsDAO.deleteProject(projectName) flatMap {
      _ => unregisterBillingProject(projectName)
    }
  }

  @deprecated(message = "this api is used in the old process of creating billing projects outside of firecloud, startBillingProjectCreation is the new way", since = "d9209db")
  def registerBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess => dataAccess.rawlsBillingProjectQuery.load(projectName) } flatMap {
      case Some(_) =>
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Cannot create billing project [${projectName.value}] in database because it already exists")))
      case None =>
        for {
          groups <- createBillingProjectGroups(projectName)
          project <- createBillingProjectInternal(projectName, CreationStatuses.Ready, groups)
        } yield RequestComplete(StatusCodes.Created)
    }
  }

  @deprecated(message = "this api is used in the old process of creating billing projects outside of firecloud, startBillingProjectCreation is the new way", since = "d9209db")
  private def createBillingProjectGroups(projectName: RawlsBillingProjectName, creators: Set[RawlsUserRef] = Set.empty): Future[Map[ProjectRoles.ProjectRole, RawlsGroup]] = {
    for {
      groups <- dataSource.inTransaction { dataAccess =>
        DBIO.sequence(ProjectRoles.all.map { role =>
          val name = RawlsGroupName(gcsDAO.toBillingProjectGroupName(projectName, role))
          createGroupInternal(RawlsGroupRef(name), dataAccess).map(g => role -> g)
        }.toSeq)
      }
      groupMap = groups.toMap
      _ <- updateGroupMembership(groupMap(ProjectRoles.Owner), addUsers = creators)
    } yield groupMap
  }

  @deprecated(message = "this api is used in the old process of creating billing projects outside of firecloud, startBillingProjectCreation is the new way", since = "d9209db")
  private def createBillingProjectInternal(projectName: RawlsBillingProjectName, status: CreationStatuses.CreationStatus, groups: Map[ProjectRoles.ProjectRole, RawlsGroup] = Map.empty): Future[RawlsBillingProject] = {
    for {
      bucketName <- gcsDAO.createCromwellAuthBucket(projectName)
      billingProject <- dataSource.inTransaction { dataAccess =>
        val bucketUrl = "gs://" + bucketName
        val billingProject = RawlsBillingProject(projectName, groups, bucketUrl, status, None, None)
        dataAccess.rawlsBillingProjectQuery.create(billingProject)
      }
    } yield billingProject
  }

  private def createBillingProjectGroupsNoGoogle(dataAccess: DataAccess, projectName: RawlsBillingProjectName, creators: Set[RawlsUserRef]): ReadWriteAction[Map[ProjectRoles.ProjectRole, RawlsGroup]] = {
    val groupsByRole = ProjectRoles.all.map { role =>
      val name = RawlsGroupName(gcsDAO.toBillingProjectGroupName(projectName, role))
      val members: Set[RawlsUserRef] = role match {
        case ProjectRoles.Owner => creators
        case _ => Set.empty
      }
      role -> RawlsGroup(name, RawlsGroupEmail(gcsDAO.toGoogleGroupName(name)), members, Set.empty)
    }.toMap

    val groupSaves = DBIO.sequence(groupsByRole.values.map { dataAccess.rawlsGroupQuery.save })

    groupSaves.map(_ => groupsByRole.toMap)
  }

  def unregisterBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = {
    for {
      groups <- dataSource.inTransaction { dataAccess =>
        withBillingProject(projectName, dataAccess) { project =>
          dataAccess.rawlsBillingProjectQuery.delete(project.projectName) map {
            case true => project.groups
            case false => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"Could not delete billing project [${projectName.value}]"))
          }
        }
      }

      _ <- Future.sequence(groups.map {case (_, group) => gcsDAO.deleteGoogleGroup(group) })

    } yield {
      RequestComplete(StatusCodes.OK)
    }
  }

  def addUserToBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    for {
      (project, addUsers, addSubGroups) <- loadMembersAndProject(projectName, projectAccessUpdate)
      _ <- updateGroupMembership(project.groups(projectAccessUpdate.role), addUsers = addUsers, addSubGroups = addSubGroups)
    } yield RequestComplete(StatusCodes.OK)
  }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    for {
      (project, removeUsers, removeSubGroups) <- loadMembersAndProject(projectName, projectAccessUpdate)
      _ <- updateGroupMembership(project.groups(projectAccessUpdate.role), removeUsers = removeUsers, removeSubGroups = removeSubGroups)
    } yield RequestComplete(StatusCodes.OK)
  }

  def loadMembersAndProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[(RawlsBillingProject, Set[RawlsUserRef], Set[RawlsGroupRef])] = {
    dataSource.inTransaction { dataAccess =>
      for {
        (addUsers, addSubGroups) <- dataAccess.rawlsGroupQuery.loadFromEmail(projectAccessUpdate.email).map {
          case Some(Left(user)) => (Set[RawlsUserRef](user), Set.empty[RawlsGroupRef])
          case Some(Right(group)) => (Set.empty[RawlsUserRef], Set[RawlsGroupRef](group))
          case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Member ${projectAccessUpdate.email} not found"))
        }
        projectOption <- dataAccess.rawlsBillingProjectQuery.load(projectName)
      } yield {
        (projectOption.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Project ${projectName.value} not found"))),
          addUsers, addSubGroups)
      }
    }
  }

  def listGroupMembers(groupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.load(groupRef) flatMap {
        case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Group ${groupRef.groupName.value} does not exist")))
        case Some(group) =>
          for {
            memberUsers <- dataAccess.rawlsGroupQuery.loadGroupUserEmails(group)
            memberGroups <- dataAccess.rawlsGroupQuery.loadGroupSubGroupEmails(group)
          } yield RequestComplete(StatusCodes.OK, UserList(memberUsers.map(_.value) ++ memberGroups.map(_.value)))
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

  def createRealm(realmRef: RawlsRealmRef) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.load(realmRef.toUserGroupRef) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Group [${realmRef.realmName.value}] already exists")))
        case None =>
          createGroupInternal(realmRef.toUserGroupRef, dataAccess) andThen dataAccess.rawlsGroupQuery.setGroupAsRealm(realmRef) map { _ => RequestComplete(StatusCodes.Created) }
      }
    }
  }

  def deleteRealm(realmRef: RawlsRealmRef) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.listWorkspacesInRealm(realmRef) flatMap {
        case Seq() =>
          dataAccess.rawlsGroupQuery.deleteRealmRecord(realmRef) flatMap { _ =>
            withGroup(realmRef.toUserGroupRef, dataAccess) { group =>
              dataAccess.rawlsGroupQuery.delete(realmRef.toUserGroupRef) andThen
                DBIO.from(gcsDAO.deleteGoogleGroup(group)) map { _ => RequestComplete(StatusCodes.OK) }
            }
          }
        case _ =>
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Unable to delete realm [${realmRef.realmName.value}] because there are workspaces in this realm")))
      }
    }
  }

  // Lists every realm in the system
  def listAllRealms() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.listAllRealms() map { realms =>
        RequestComplete(StatusCodes.OK, realms)
      }
    }
  }

  // Lists every realm that the user has access to
  def listRealmsForUser() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.listRealmsForUser(RawlsUserRef(RawlsUserSubjectId(userInfo.userSubjectId))).map { realms =>
        RequestComplete(StatusCodes.OK, realms)
      }
    }
  }

  private def createGroupInternal(groupRef: RawlsGroupRef, dataAccess: DataAccess): ReadWriteAction[RawlsGroup] = {
    DBIO.from(gcsDAO.createGoogleGroup(groupRef)).flatMap { rawlsGroup =>
      dataAccess.rawlsGroupQuery.save(rawlsGroup)
    }
  }

  def deleteGroup(groupRef: RawlsGroupRef) = {
    dataSource.inTransaction { dataAccess =>
      withGroup(groupRef, dataAccess) { group =>
        dataAccess.rawlsGroupQuery.delete(groupRef) andThen
        DBIO.from(gcsDAO.deleteGoogleGroup(group)) map { _ => RequestComplete(StatusCodes.OK) }
      }
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
          intersectionGroups <- updateIntersectionGroupMembers(groupsToIntersects, dataAccess)
        } yield (savedGroup, intersectionGroups)
      }, TransactionIsolation.ReadCommitted)

      messages = (intersectionGroups.toSeq :+ RawlsGroup.toRef(savedGroup)).map(_.toJson.compactPrint)

      _ <- gpsDAO.publishMessages(gpsGroupSyncTopic, messages)
    } yield savedGroup
  }

  private def loadMemberUsersAndGroups(memberList: RawlsGroupMemberList, dataAccess: DataAccess): ReadAction[(Set[RawlsUser], Set[RawlsGroup])] = {
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
      
    } yield RequestComplete(StatusCodes.OK)
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
    val groups = groupsToIntersect.flatMap { case GroupsToIntersect(target, group1, group2) => Seq(group1, group2) }.toSet
    val flattenedGroupsAction = DBIO.sequence(groups.map(group => dataAccess.rawlsGroupQuery.flattenGroupMembership(group).map(group -> _)).toSeq).map(_.toMap)

    val intersectionMemberships = flattenedGroupsAction.map { flattenedGroups =>
      groupsToIntersect.map { case GroupsToIntersect(target, group1, group2) =>
        (target, flattenedGroups(group1) intersect flattenedGroups(group2))
      }
    }

    intersectionMemberships.flatMap(dataAccess.rawlsGroupQuery.overwriteGroupUsers).map(_ => groupsToIntersect.map(_.target))
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
            createProjectOperation <- gcsDAO.createProject(projectName, billingAccount)
            result <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
                case None =>
                  for {
                    groups <- createBillingProjectGroupsNoGoogle(dataAccess, projectName, Set(RawlsUser(userInfo)))
                    _ <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, groups, "gs://" + gcsDAO.getCromwellAuthBucketName(projectName), CreationStatuses.Creating, Option(billingAccountName), None))
                    _ <- dataAccess.rawlsBillingProjectQuery.insertOperations(Seq(createProjectOperation))

                  } yield {
                    RequestComplete(StatusCodes.Created)
                  }

                case Some(_) => DBIO.successful(RequestComplete(StatusCodes.Conflict))
              }
            }
          } yield {
            result
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
      case many => Option(ErrorReport("multiple errors", errorReports.toSeq))
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

