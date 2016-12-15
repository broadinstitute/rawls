package org.broadinstitute.dsde.rawls.user

import _root_.slick.jdbc.TransactionIsolation
import akka.actor.{Actor, Props}
import akka.pattern._
import com.google.api.client.http.HttpResponseException
import com.google.api.client.auth.oauth2.TokenResponseException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, ReadWriteAction, DataAccess}
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WriteAction, ReadAction, ReadWriteAction, DataAccess}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{RoleSupport, FutureSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete, RequestCompleteWithLocation}
import spray.http.StatusCodes
import spray.json._
import spray.httpx.SprayJsonSupport._
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

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, userDirectoryDAO)

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
  case class AdminImportUsers(rawlsUserInfoList: RawlsUserInfoList) extends UserServiceMessage
  case class GetUserGroup(groupRef: RawlsGroupRef) extends UserServiceMessage

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

  case class IsAdmin(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class IsLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminAddLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class AdminRemoveLibraryCurator(userEmail: RawlsUserEmail) extends UserServiceMessage
}

class UserService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with RoleSupport with FutureSupport with UserWiths {

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
    case AdminImportUsers(rawlsUserInfoList) => asFCAdmin { importUsers(rawlsUserInfoList) } pipeTo sender
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

    case CreateBillingProjectFull(projectName, billingAccount) => startBillingProjectCreation(projectName, billingAccount) pipeTo sender
    case GetBillingProjectMembers(projectName) => asProjectOwner(projectName) { getBillingProjectMembers(projectName) } pipeTo sender

    case OverwriteGroupMembers(groupName, memberList) => overwriteGroupMembers(groupName, memberList) to sender
    case AdminAddGroupMembers(groupName, memberList) => asFCAdmin { updateGroupMembers(groupName, memberList, AddGroupMembersOp) } to sender
    case AdminRemoveGroupMembers(groupName, memberList) => asFCAdmin { updateGroupMembers(groupName, memberList, RemoveGroupMembersOp) } to sender
    case AddGroupMembers(groupName, memberList) => updateGroupMembers(groupName, memberList, AddGroupMembersOp) to sender
    case RemoveGroupMembers(groupName, memberList) => updateGroupMembers(groupName, memberList, RemoveGroupMembersOp) to sender
    case AdminSynchronizeGroupMembers(groupRef) => asFCAdmin { synchronizeGroupMembers(groupRef) } pipeTo sender

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
        throw new RawlsExceptionWithErrorReport(ErrorReport(t, t.getStatusCode))
    }
  }

  def createUser(): Future[PerRequestMessage] = {
    val user = RawlsUser(userInfo)

    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already created groups/entries are ok
    handleFutures(Future.sequence(Seq(
      toFutureTry(gcsDAO.createProxyGroup(user) flatMap( _ => gcsDAO.addUserToProxyGroup(user))),
      toFutureTry(dataSource.inTransaction { dataAccess => dataAccess.rawlsUserQuery.save(user).flatMap(user => addUsersToAllUsersGroup(Set(user), dataAccess)) }),
      toFutureTry(userDirectoryDAO.createUser(user.userSubjectId) flatMap( _ => userDirectoryDAO.enableUser(user.userSubjectId)))

    )))(_ => RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId.value}"), handleException("Errors creating user")
    )
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

  //imports the response from the above listUsers
  def importUsers(rawlsUserInfoList: RawlsUserInfoList): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      val userInfos = rawlsUserInfoList.userInfoList
      DBIO.sequence(
        userInfos.map { u =>
          dataAccess.rawlsUserQuery.save(u.user) flatMap { user =>
            DBIO.seq(u.billingProjects.map { projectName =>
              dataAccess.rawlsBillingProjectQuery.load(projectName) map {
                case Some(project) => updateGroupMembersInternal(project.groups(ProjectRoles.User), Set(user), Set.empty, AddGroupMembersOp, dataAccess)
                case None => throw new RawlsException(s"Project [${projectName.value}] not found")
              }
            }: _*) map (_ => user)
          }
        }
      ) flatMap { users =>
        addUsersToAllUsersGroup(users.toSet, dataAccess)
      } map (_ match {
        case None => RequestComplete(StatusCodes.Created)
        case Some(error) => throw new RawlsExceptionWithErrorReport(errorReport = error)
      })
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

  private def addUsersToAllUsersGroup(users: Set[RawlsUser], dataAccess: DataAccess): ReadWriteAction[Option[ErrorReport]] = {
    getOrCreateAllUsersGroup(dataAccess) flatMap { allUsersGroup =>
      updateGroupMembersInternal(allUsersGroup, users, Set.empty, AddGroupMembersOp, dataAccess)
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
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(t, StatusCodes.InternalServerError))
      case Success(b) => b match {
        case true => RequestComplete(StatusCodes.OK)
        case false => RequestComplete(StatusCodes.NotFound)
      }
    }
  }

  def isLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.isLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(t, StatusCodes.InternalServerError))
      case Success(b) => b match {
        case true => RequestComplete(StatusCodes.OK)
        case false => RequestComplete(StatusCodes.NotFound)
      }
    }
  }

  def addLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.addLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(t, StatusCodes.InternalServerError))
      case Success(_) => RequestComplete(StatusCodes.OK)
    }
  }

  def removeLibraryCurator(userEmail: RawlsUserEmail): Future[PerRequestMessage] = {
    toFutureTry(gcsDAO.removeLibraryCurator(userEmail.value)) map {
      case Failure(t) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(t, StatusCodes.InternalServerError))
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

  @deprecated(message = "this api is used in the old process of creating billing projects outside of firecloud, startBillingProjectCreation is the new way")
  def registerBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = dataSource.inTransaction { dataAccess =>
    dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
      case Some(_) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Cannot create billing project [${projectName.value}] in database because it already exists")))
      case None =>
        for {
          groups <- createBillingProjectGroups(dataAccess, projectName)
          project <- createBillingProjectInternal(dataAccess, projectName, CreationStatuses.Ready, groups) map (_ => RequestComplete(StatusCodes.Created))
        } yield project
    }
  }

  @deprecated
  private def createBillingProjectGroups(dataAccess: DataAccess, projectName: RawlsBillingProjectName, creators: Set[RawlsUserRef] = Set.empty): ReadWriteAction[Map[ProjectRoles.ProjectRole, RawlsGroup]] = {
    val createGroups = DBIO.sequence(ProjectRoles.all.map { role =>
      val name = RawlsGroupName(gcsDAO.toBillingProjectGroupName(projectName, role))
      createGroupInternal(RawlsGroupRef(name), dataAccess).map(g => role -> g)
    }.toSeq)

    createGroups flatMap { groups =>
      val groupMap = groups.toMap
      DBIO.sequence(creators.map(dataAccess.rawlsUserQuery.load).toSeq) flatMap { owners =>
        updateGroupMembersInternal(groupMap(ProjectRoles.Owner), owners.flatten.toSet, Set.empty, AddGroupMembersOp, dataAccess).map(_ => groupMap)
      }
    }
  }

  @deprecated
  private def createBillingProjectInternal(dataAccess: DataAccess, projectName: RawlsBillingProjectName, status: CreationStatuses.CreationStatus, groups: Map[ProjectRoles.ProjectRole, RawlsGroup] = Map.empty): ReadWriteAction[RawlsBillingProject] = {
    DBIO.from(gcsDAO.createCromwellAuthBucket(projectName)) flatMap { bucketName =>
      val bucketUrl = "gs://" + bucketName

      val billingProject = RawlsBillingProject(projectName, groups, bucketUrl, status, None)
      dataAccess.rawlsBillingProjectQuery.create(billingProject)
    }
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
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.loadFromEmail(projectAccessUpdate.email).map {
        case Some(member) => member
        case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Member ${projectAccessUpdate.email} not found"))
      }
    } map {
      case Left(user) => RawlsGroupMemberList(userEmails = Some(Seq(user.userEmail.value)))
      case Right(group) => RawlsGroupMemberList(subGroupEmails = Some(Seq(group.groupEmail.value)))
    } flatMap { memberList =>
      updateGroupMembers(RawlsGroupRef(RawlsGroupName(gcsDAO.toBillingProjectGroupName(projectName, projectAccessUpdate.role))), memberList, AddGroupMembersOp)
    }
  }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName, projectAccessUpdate: ProjectAccessUpdate): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsGroupQuery.loadFromEmail(projectAccessUpdate.email).map {
        case Some(member) => member
        case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Member ${projectAccessUpdate.email} not found"))
      }
    } map {
      case Left(user) => RawlsGroupMemberList(userEmails = Some(Seq(user.userEmail.value)))
      case Right(group) => RawlsGroupMemberList(subGroupEmails = Some(Seq(group.groupEmail.value)))
    } flatMap { memberList =>
      updateGroupMembers(RawlsGroupRef(RawlsGroupName(gcsDAO.toBillingProjectGroupName(projectName, projectAccessUpdate.role))), memberList, RemoveGroupMembersOp)
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
    dataSource.inTransaction ({ dataAccess =>
      withGroup(groupRef, dataAccess) { group =>
        withMemberUsersAndGroups(memberList, dataAccess) { (users, subGroups) =>
          val addMembersAction: ReadWriteAction[Option[ErrorReport]] = overwriteGroupMembersInternal(group, users, subGroups, dataAccess)

          // finally report the results
          addMembersAction.map {
            case None => RequestComplete(StatusCodes.NoContent)
            case Some(error) => throw new RawlsExceptionWithErrorReport(errorReport = error)
          }
        }
      }
    }, TransactionIsolation.ReadCommitted) // read committed required to reduce db locks and allow concurrency
  }

  def overwriteGroupMembersInternal(group: RawlsGroup, users: Set[RawlsUser], subGroups: Set[RawlsGroup], dataAccess: DataAccess): ReadWriteAction[Option[ErrorReport]] = {
    val usersToRemove = group.users -- users.map(RawlsUser.toRef(_))
    val subGroupsToRemove = group.subGroups -- subGroups.map(RawlsGroup.toRef(_))

    // first remove members that should be removed
    val removeMembersAction = for {
      users <- DBIO.sequence(usersToRemove.map(dataAccess.rawlsUserQuery.load(_).map(_.get)).toSeq)
      subGroups <- DBIO.sequence(subGroupsToRemove.map(dataAccess.rawlsGroupQuery.load(_).map(_.get)).toSeq)
      result <- updateGroupMembersInternal(group, users.toSet, subGroups.toSet, RemoveGroupMembersOp, dataAccess)
    } yield result

    // then if there were no errors, add users that should be added
    removeMembersAction.flatMap {
      case Some(errorReport) => DBIO.successful(Option(errorReport))
      case None =>
        val usersToAdd = users.filter(user => !group.users.contains(user))
        val subGroupsToAdd = subGroups.filter(subGroup => !group.subGroups.contains(subGroup))

        // need to reload group cause it changed if members were removed
        dataAccess.rawlsGroupQuery.load(group).map(_.get) flatMap {
          updateGroupMembersInternal(_, usersToAdd, subGroupsToAdd, AddGroupMembersOp, dataAccess)
        }
    }
  }

  private def withMemberUsersAndGroups(memberList: RawlsGroupMemberList, dataAccess: DataAccess)(op: (Set[RawlsUser], Set[RawlsGroup]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
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

    // nested function makes following for comprehension easier to read
    def maybeExecuteOp(users: Iterable[(String, Option[RawlsUser])], subGroups: Iterable[(String, Option[RawlsGroup])]): ReadWriteAction[PerRequestMessage] = {
      (users.collect { case (email, None) => email }, subGroups.collect { case (email, None) => email }) match {
        // success case, all users and groups found
        case (Seq(), Seq()) => op(users.map(_._2.get).toSet, subGroups.map(_._2.get).toSet)

        // failure cases, some users and/or groups not found
        case (Seq(), missingGroups) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some groups not found: ${missingGroups.mkString(", ")}")))
        case (missingUsers, Seq()) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}")))
        case (missingUsers, missingGroups) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}. Some groups not found: ${missingGroups.mkString(", ")}")))
      }
    }

    for {
      users <- userQueries
      subGroups <- subGroupQueries
      result <- maybeExecuteOp(users, subGroups)
    } yield result
  }

  def updateGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList, operation: UpdateGroupMembersOp): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withGroup(groupRef, dataAccess) { group =>
        withMemberUsersAndGroups(memberList, dataAccess) { (users, subGroups) =>
          updateGroupMembersInternal(group, users, subGroups, operation, dataAccess) map {
            case None => RequestComplete(StatusCodes.OK)
            case Some(error) => throw new RawlsExceptionWithErrorReport(errorReport = error)
          }
        }
      }
    }
  }

  def synchronizeGroupMembers(groupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withGroup(groupRef, dataAccess) { group =>
        synchronizeGroupMembersInternal(group, dataAccess) map { syncReport =>
          val statusCode = if (syncReport.items.exists(_.errorReport.isDefined)) {
            StatusCodes.BadGateway // status 500 is used for all other errors, 502 seems like the best otherwise
          } else {
            StatusCodes.OK
          }
          RequestComplete(statusCode, syncReport)
        }
      }
    }
  }

  def synchronizeGroupMembersInternal(group: RawlsGroup, dataAccess: DataAccess): ReadAction[SyncReport] = {
    def loadRefs(refs: Set[Either[RawlsUserRef, RawlsGroupRef]]) = {
      DBIO.sequence(refs.map {
        case Left(userRef) => dataAccess.rawlsUserQuery.load(userRef).map(userOption => Left(userOption.getOrElse(throw new RawlsException(s"user $userRef not found"))))
        case Right(groupRef) => dataAccess.rawlsGroupQuery.load(groupRef).map(groupOption => Right(groupOption.getOrElse(throw new RawlsException(s"group $groupRef not found"))))
      }.toSeq)
    }

    def toSyncReportItem(operation: String, member: Either[RawlsUser, RawlsGroup], result: Try[Unit]) = {
      SyncReportItem(
        operation,
        member match {
          case Left(user) => Option(user)
          case _ => None
        },
        member match {
          case Right(group) => Option(group.toRawlsGroupShort)
          case _ => None
        },
        result match {
          case Success(_) => None
          case Failure(t) => Option(ErrorReport(t))
        }
      )
    }

    DBIO.from(gcsDAO.listGroupMembers(group)) flatMap {
      case None => DBIO.from(gcsDAO.createGoogleGroup(group) map (_ => Seq.empty[Either[RawlsUserRef, RawlsGroupRef]]))
      case Some(members) => DBIO.successful(members)
    } flatMap { members =>

      val toRemove = members.toSet -- group.users.map(Left(_)) -- group.subGroups.map(Right(_))
      val removeFutures = loadRefs(toRemove) flatMap { removeMembers =>
        DBIO.sequence(removeMembers map { removeMember =>
          DBIO.from(toFutureTry(gcsDAO.removeMemberFromGoogleGroup(group, removeMember)).map(toSyncReportItem("removed", removeMember, _)))
        })
      }

      val realMembers: Set[Either[RawlsUserRef, RawlsGroupRef]] = group.users.map(Left(_)) ++ group.subGroups.map(Right(_))
      val toAdd = realMembers -- members
      val addFutures = loadRefs(toAdd) flatMap { addMembers =>
        DBIO.sequence(addMembers map { addMember =>
          DBIO.from(toFutureTry(gcsDAO.addMemberToGoogleGroup(group, addMember)).map(toSyncReportItem("added", addMember, _)))
        })
      }

      DBIO.sequence(Seq(removeFutures, addFutures)).map(x => SyncReport(x.flatten))
    }
  }

  /**
   * updates the contents of a group. in the event of a failure updating the user in the google group, the user will not be updated in the RawlsGroup.
   *
   * @param group the group the update
   * @param users users to add or remove from the group
   * @param subGroups sub groups to add or remove from the group
   * @param operation which update operation to perform
   * @param dataAccess
   * @return ReadWriteAction(None) if all went well
   */
  private def updateGroupMembersInternal(group: RawlsGroup, users: Set[RawlsUser], subGroups: Set[RawlsGroup], operation: UpdateGroupMembersOp, dataAccess: DataAccess): ReadWriteAction[Option[ErrorReport]] = {
    // update the google group, each update happens in the future and may or may not succeed.
    val googleUpdateTrials: Set[Future[Try[Either[RawlsUser, RawlsGroup]]]] = users.map { user =>
      toFutureTry(operation.updateGoogle(group, Left(user))).map(_ match {
        case Success(_) => Success(Left(user))
        case Failure(f) => Failure(new RawlsException(s"Could not update user ${user.userEmail.value}", f))
      })
    } ++ subGroups.map { subGroup =>
      toFutureTry(operation.updateGoogle(group, Right(subGroup))).map(_ match {
        case Success(_) => Success(Right(subGroup))
        case Failure(f) => Failure(new RawlsException(s"Could not update group ${subGroup.groupEmail.value}", f))
      })
    }

    // wait for all google updates to finish then for each successful update change the rawls database
    DBIO.from(Future.sequence(googleUpdateTrials)).flatMap { tries =>
      val successfulUsers = tries.collect { case Success(Left(member)) => RawlsUser.toRef(member) }
      val successfulGroups = tries.collect { case Success(Right(member)) => RawlsGroup.toRef(member) }
      dataAccess.rawlsGroupQuery.save(operation.updateGroupObject(group, successfulUsers, successfulGroups)) map { _ =>
        val exceptions = tries.collect { case Failure(t) => ErrorReport(t) }
        if (exceptions.isEmpty) {
          None
        } else {
          Option(ErrorReport(StatusCodes.BadRequest, "Unable to update the following member(s)", exceptions.toSeq))
        }
      }
    } flatMap { errorReport =>
      dataAccess.workspaceQuery.findWorkspacesForGroup(group).flatMap { workspaces =>
        val priorResult: ReadWriteAction[Option[ErrorReport]] = DBIO.successful(errorReport)
        val instersectionUpdatesResults = workspaces.map(updateIntersectionGroupMembers(_, dataAccess))
        DBIO.sequence(instersectionUpdatesResults :+ priorResult)
      }
    } map(reduceErrorReports)
  }

  def updateIntersectionGroupMembers(workspace: Workspace, dataAccess:DataAccess): ReadWriteAction[Option[ErrorReport]] = {
    val actions = workspace.realm match {
      case None => Seq(DBIO.successful(None))
      case Some(realm) =>
        workspace.accessLevels.map {
          case (accessLevel, group) =>
            dataAccess.rawlsGroupQuery.load(workspace.realmACLs(accessLevel)) flatMap {
              case None => DBIO.failed(new RawlsException("Unable to load intersection group"))
              case Some(intersectionGroup) =>
                dataAccess.rawlsGroupQuery.intersectGroupMembership(group, realm) flatMap { newMemberRefs =>
                  DBIO.sequence(newMemberRefs.map(dataAccess.rawlsUserQuery.load(_).map(_.get)).toSeq)
                } flatMap { newMembers =>
                  overwriteGroupMembersInternal(intersectionGroup, newMembers.toSet, Set.empty, dataAccess)
                }
            }
        }
    }

    DBIO.sequence(actions).map(reduceErrorReports)
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
            _ <- gcsDAO.createProject(projectName, billingAccount)
            result <- dataSource.inTransaction { dataAccess =>
              dataAccess.rawlsBillingProjectQuery.load(projectName) flatMap {
                case None =>
                  for {
                    groups <- createBillingProjectGroupsNoGoogle(dataAccess, projectName, Set(RawlsUser(userInfo)))
                    _ <- dataAccess.rawlsBillingProjectQuery.create(RawlsBillingProject(projectName, groups, "gs://" + gcsDAO.getCromwellAuthBucketName(projectName), CreationStatuses.Creating, Option(billingAccountName)))

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

  private sealed trait UpdateGroupMembersOp {
    def updateGoogle(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]): Future[Unit]
    def updateGroupObject(group: RawlsGroup, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): RawlsGroup
  }
  private object AddGroupMembersOp extends UpdateGroupMembersOp {
    override def updateGoogle(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]): Future[Unit] = gcsDAO.addMemberToGoogleGroup(group, member)
    override def updateGroupObject(group: RawlsGroup, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): RawlsGroup = group.copy( users = (group.users ++ users), subGroups = (group.subGroups ++ subGroups))
  }
  private object RemoveGroupMembersOp extends UpdateGroupMembersOp {
    override def updateGoogle(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]): Future[Unit] = gcsDAO.removeMemberFromGoogleGroup(group, member)
    override def updateGroupObject(group: RawlsGroup, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): RawlsGroup = group.copy( users = (group.users -- users), subGroups = (group.subGroups -- subGroups))
  }
}

