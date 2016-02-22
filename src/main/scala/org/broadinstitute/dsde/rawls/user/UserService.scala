package org.broadinstitute.dsde.rawls.user

import _root_.slick.dbio._
import akka.actor.{Actor, Props}
import akka.pattern._
import com.google.api.client.http.HttpResponseException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, DataAccess}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, AdminSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestComplete, RequestCompleteWithLocation, PerRequestMessage}
import spray.http.StatusCodes
import spray.json._
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Try, Failure}

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
  case class EnableUser(userRef: RawlsUserRef) extends UserServiceMessage
  case class DisableUser(userRef: RawlsUserRef) extends UserServiceMessage
  case object ListUsers extends UserServiceMessage
  case class ImportUsers(rawlsUserInfoList: RawlsUserInfoList) extends UserServiceMessage
  case class GetUserGroup(groupRef: RawlsGroupRef) extends UserServiceMessage

  case object ListBillingProjects extends UserServiceMessage
  case class ListBillingProjectsForUser(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class CreateBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class DeleteBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class AddUserToBillingProject(projectName: RawlsBillingProjectName, userEmail: RawlsUserEmail) extends UserServiceMessage
  case class RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, userEmail: RawlsUserEmail) extends UserServiceMessage

  case class CreateGroup(groupRef: RawlsGroupRef) extends UserServiceMessage
  case class ListGroupMembers(groupName: String) extends UserServiceMessage
  case class DeleteGroup(groupRef: RawlsGroupRef) extends UserServiceMessage
  case class AdminOverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class OverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AdminAddGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AddGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class AdminRemoveGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class RemoveGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class SynchronizeGroupMembers(groupRef: RawlsGroupRef) extends UserServiceMessage
}

class UserService(protected val userInfo: UserInfo, dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with AdminSupport with FutureSupport {
  override def receive = {
    case SetRefreshToken(token) => setRefreshToken(token) pipeTo sender
    case GetRefreshTokenDate => getRefreshTokenDate() pipeTo sender

    case CreateUser => createUser() pipeTo sender
    case AdminGetUserStatus(userRef) => adminGetUserStatus(userRef) pipeTo sender
    case UserGetUserStatus => userGetUserStatus() pipeTo sender
    case EnableUser(userRef) => enableUser(userRef) pipeTo sender
    case DisableUser(userRef) => disableUser(userRef) pipeTo sender
    case ListUsers => listUsers pipeTo sender
    case ImportUsers(rawlsUserInfoList) => importUsers(rawlsUserInfoList) pipeTo sender
    case GetUserGroup(groupRef) => getUserGroup(groupRef) pipeTo sender

    // ListBillingProjects is for the current user, not as admin
    // ListBillingProjectsForUser is for any user, as admin

    case ListBillingProjects => listBillingProjects(RawlsUser(userInfo).userEmail) pipeTo sender
    case ListBillingProjectsForUser(userEmail) => asAdmin { listBillingProjects(userEmail) } pipeTo sender
    case CreateBillingProject(projectName) => createBillingProject(projectName) pipeTo sender
    case DeleteBillingProject(projectName) => deleteBillingProject(projectName) pipeTo sender
    case AddUserToBillingProject(projectName, userEmail) => addUserToBillingProject(projectName, userEmail) pipeTo sender
    case RemoveUserFromBillingProject(projectName, userEmail) => removeUserFromBillingProject(projectName, userEmail) pipeTo sender

    case CreateGroup(groupRef) => pipe(createGroup(groupRef)) to sender
    case ListGroupMembers(groupName) => pipe(listGroupMembers(groupName)) to sender
    case DeleteGroup(groupName) => pipe(deleteGroup(groupName)) to sender
    case AdminOverwriteGroupMembers(groupName, memberList) => adminOverwriteGroupMembers(groupName, memberList) to sender
    case OverwriteGroupMembers(groupName, memberList) => overwriteGroupMembers(groupName, memberList) to sender
    case AdminAddGroupMembers(groupName, memberList) => asAdmin { updateGroupMembers(groupName, memberList, AddGroupMembersOp) } to sender
    case AdminRemoveGroupMembers(groupName, memberList) => asAdmin { updateGroupMembers(groupName, memberList, RemoveGroupMembersOp) } to sender
    case AddGroupMembers(groupName, memberList) => updateGroupMembers(groupName, memberList, AddGroupMembersOp) to sender
    case RemoveGroupMembers(groupName, memberList) => updateGroupMembers(groupName, memberList, RemoveGroupMembersOp) to sender
    case SynchronizeGroupMembers(groupRef) => synchronizeGroupMembers(groupRef) to sender
  }

  def setRefreshToken(userRefreshToken: UserRefreshToken): Future[PerRequestMessage] = {
    gcsDAO.storeToken(userInfo, userRefreshToken.refreshToken).map(_ => RequestComplete(StatusCodes.Created))
  }

  def getRefreshTokenDate(): Future[PerRequestMessage] = {
    gcsDAO.getTokenDate(userInfo).map( _ match {
      case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"no refresh token stored for ${userInfo.userEmail}"))
      case Some(date) => RequestComplete(UserRefreshTokenDate(date))
    })
  }

  def createUser(): Future[PerRequestMessage] = {
    val user = RawlsUser(userInfo)

    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already created groups/entries are ok
    handleFutures(Future.sequence(Seq(
      toFutureTry(gcsDAO.createProxyGroup(user)),
      toFutureTry(dataSource.inTransaction { dataAccess => dataAccess.rawlsUserQuery.save(user).flatMap(user => addUsersToAllUsersGroup(Set(user), dataAccess)) }),
      toFutureTry(userDirectoryDAO.createUser(user))

    )))(_ => RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId.value}"), handleException("Errors creating user")
    )
  }

  import spray.json.DefaultJsonProtocol._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsUserInfoListFormat

  def listUsers(): Future[PerRequestMessage] = asAdmin {
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsUserQuery.loadAllUsersWithProjects map { projectsByUser =>
        val userInfoList = projectsByUser map {
          case (user, projectNames) => RawlsUserInfo(user, projectNames.toSeq)
        }
        RequestComplete(RawlsUserInfoList(userInfoList.toSeq))
      }
    }
  }

  //imports the response from the above listUsers
  def importUsers(rawlsUserInfoList: RawlsUserInfoList): Future[PerRequestMessage] = asAdmin {
    dataSource.inTransaction { dataAccess =>
      val userInfos = rawlsUserInfoList.userInfoList
      DBIO.sequence(
        userInfos.map { u =>
          dataAccess.rawlsUserQuery.save(u.user) flatMap { user =>
            DBIO.seq(u.billingProjects.map(b =>
              dataAccess.rawlsBillingProjectQuery.load(b) flatMap { project =>
                dataAccess.rawlsBillingProjectQuery.addUserToProject(u.user, project.get)
              }
            ):_*) map (_=> user)
          }
        }
      ) flatMap { users =>
        addUsersToAllUsersGroup(users.toSet, dataAccess)
      } map(_ match {
        case None => RequestComplete(StatusCodes.Created)
        case Some(error) => throw new RawlsExceptionWithErrorReport(errorReport = error)
      })
    }
  }

  def getUserGroup(rawlsGroupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadGroupIfMember(rawlsGroupRef, RawlsUser(userInfo), txn) match {
        case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found or member not in group")))
        case Some(group) => Future.successful(RequestComplete(group.toRawlsGroupShort))
      }
    }
  }

  private def addUsersToAllUsersGroup(users: Set[RawlsUser], dataAccess: DataAccess): ReadWriteAction[Option[ErrorReport]] = {
    getOrCreateAllUsersGroup(dataAccess) flatMap { allUsersGroup =>
      updateGroupMembersInternal(allUsersGroup, users, Set.empty, AddGroupMembersOp, dataAccess)
    }
  }

  private def getOrCreateAllUsersGroup(dataAccess: DataAccess): ReadWriteAction[RawlsGroup] = {
    dataAccess.rawlsGroupQuery.load(allUsersGroupRef) flatMap (_ match {
      case Some(g) => DBIO.successful(g)
      case None => createGroupInternal(allUsersGroupRef, dataAccess).asTry flatMap {
        case Success(group) => DBIO.successful(group)
        case Failure(t: HttpResponseException) if t.getStatusCode == StatusCodes.Conflict.intValue =>
          // this case is where the group was not in our db but already in google, the recovery code makes the assumption
          // that createGroupInternal saves the group in our db before creating the group in google so loadGroup should work
          dataAccess.rawlsGroupQuery.load(allUsersGroupRef).map(_.get)
        case Failure(regrets) => DBIO.failed(regrets)
      }
    })
  }

  def userGetUserStatus(): Future[PerRequestMessage] = {
    getUserStatus(RawlsUserRef(RawlsUserSubjectId(userInfo.userSubjectId)))
  }

  def adminGetUserStatus(userRef: RawlsUserRef): Future[PerRequestMessage] = asAdmin {
    getUserStatus(userRef)
  }

  def getUserStatus(userRef: RawlsUserRef): Future[PerRequestMessage] = withUser(userRef) { user =>
    handleFutures(Future.sequence(Seq(
      toFutureTry(gcsDAO.isUserInProxyGroup(user).map("google" -> _)),
      toFutureTry(userDirectoryDAO.isEnabled(user).map("ldap" -> _)),
      toFutureTry {
        dataSource.inFutureTransaction() { txn =>
          val allUsersGroup = getOrCreateAllUsersGroup(txn)
          allUsersGroup.map("allUsersGroup" -> _.users.contains(userRef))
        }
      }

    )))(statuses => RequestComplete(UserStatus(user, statuses.toMap)), handleException("Error checking if a user is enabled"))
  }

  def enableUser(userRef: RawlsUserRef): Future[PerRequestMessage] = asAdmin {
    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already added entries are ok
    withUser(userRef) { user =>
      handleFutures(Future.sequence(Seq(
        toFutureTry(gcsDAO.addUserToProxyGroup(user)),
        toFutureTry(userDirectoryDAO.enableUser(user))

      )))(_ => RequestComplete(StatusCodes.NoContent), handleException("Errors enabling user"))
    }
  }

  def disableUser(userRef: RawlsUserRef): Future[PerRequestMessage] = asAdmin {
    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already removed entries are ok
    withUser(userRef) { user =>
      handleFutures(Future.sequence(Seq(
        toFutureTry(gcsDAO.removeUserFromProxyGroup(user)),
        toFutureTry(userDirectoryDAO.disableUser(user))

      )))(_ => RequestComplete(StatusCodes.NoContent), handleException("Errors disabling user"))
    }
  }

  import spray.json.DefaultJsonProtocol._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectNameFormat

  // when called for the current user, admin access is not required
  def listBillingProjects(userEmail: RawlsUserEmail): Future[PerRequestMessage] =
    withUser(userEmail) { user =>
      dataSource.inFutureTransaction() { txn =>
        Future {
          RequestComplete(containerDAO.billingDAO.listUserProjects(user, txn).toSeq)
        }
      }
    }

  def createBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = asAdmin {
    dataSource.inFutureTransaction() { txn =>
      containerDAO.billingDAO.loadProject(projectName, txn) match {
        case Some(_) =>
          Future {
            throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Cannot create billing project [${projectName.value}] in database because it already exists"))
          }
        case None =>
          // note: executes in a Future
          gcsDAO.createCromwellAuthBucket(projectName) map { bucketName =>
            val bucketUrl = "gs://" + bucketName
            containerDAO.billingDAO.saveProject(RawlsBillingProject(projectName, Set.empty, bucketUrl), txn)
            RequestComplete(StatusCodes.Created)
          }
      }
    }
  }

  def deleteBillingProject(projectName: RawlsBillingProjectName): Future[PerRequestMessage] = asAdmin {
    withBillingProject(projectName) { project =>
      dataSource.inFutureTransaction() { txn =>
        Future {
          if (containerDAO.billingDAO.deleteProject(project, txn)) RequestComplete(StatusCodes.OK)
          else throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"Could not delete billing project [${projectName.value}]"))
        }
      }
    }
  }

  def addUserToBillingProject(projectName: RawlsBillingProjectName, userEmail: RawlsUserEmail): Future[PerRequestMessage] = asAdmin {
    withBillingProject(projectName) { project =>
      withUser(userEmail) { user =>
        dataSource.inFutureTransaction() { txn =>
          Future {
            containerDAO.billingDAO.addUserToProject(user, project, txn)
            RequestComplete(StatusCodes.OK)
          }
        }
      }
    }
  }

  def removeUserFromBillingProject(projectName: RawlsBillingProjectName, userEmail: RawlsUserEmail): Future[PerRequestMessage] = asAdmin {
    withBillingProject(projectName) { project =>
      withUser(userEmail) { user =>
        dataSource.inFutureTransaction() { txn =>
          Future {
            containerDAO.billingDAO.removeUserFromProject(user, project, txn)
            RequestComplete(StatusCodes.OK)
          }
        }
      }
    }
  }

  def listGroupMembers(groupName: String) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        Future {
          containerDAO.authDAO.loadGroup(RawlsGroupRef(RawlsGroupName(groupName)), txn) match {
            case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Group ${groupName} does not exist"))
            case Some(group) =>
              val memberUsers = group.users.map(u => containerDAO.authDAO.loadUser(u, txn).get.userEmail.value)
              val memberGroups = group.subGroups.map(g => containerDAO.authDAO.loadGroup(g, txn).get.groupEmail.value)
              RequestComplete(StatusCodes.OK, UserList((memberUsers ++ memberGroups).toSeq))
          }
        }
      }
    }
  }

  def createGroup(groupRef: RawlsGroupRef) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        containerDAO.authDAO.loadGroup(groupRef, txn) match {
          case Some(_) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Group ${groupRef.groupName} already exists")))
          case None =>
            createGroupInternal(groupRef, txn) map { _ => RequestComplete(StatusCodes.Created) }
        }
      }
    }
  }

  private def createGroupInternal(groupRef: RawlsGroupRef, dataAccess: DataAccess): ReadWriteAction[RawlsGroup] = {
    DBIO.from(gcsDAO.createGoogleGroup(groupRef)).flatMap { rawlsGroup =>
      dataAccess.rawlsGroupQuery.save(rawlsGroup)
    }
  }

  def deleteGroup(groupRef: RawlsGroupRef) = {
    asAdmin {
      dataSource.inTransaction() { txn =>
        withGroup(groupRef, txn) { group =>
          containerDAO.authDAO.deleteGroup(groupRef, txn)
          gcsDAO.deleteGoogleGroup(group) map { _ => RequestComplete(StatusCodes.OK) }
        }
      }
    }
  }

  def adminOverwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList): Future[PerRequestMessage] = {
    asAdmin {
      overwriteGroupMembers(groupRef, memberList)
    }
  }

  def overwriteGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction() { txn =>
      withGroup(groupRef, txn) { group =>
        withMemberUsersAndGroups(memberList, txn) { (users, subGroups) =>
          val usersToRemove = group.users -- users.map(RawlsUser.toRef(_))
          val subGroupsToRemove = group.subGroups -- subGroups.map(RawlsGroup.toRef(_))

          // first remove members that should be removed
          val removeMembersFuture = updateGroupMembersInternal(group,
            usersToRemove.map(containerDAO.authDAO.loadUser(_, txn).get),
            subGroupsToRemove.map(containerDAO.authDAO.loadGroup(_, txn).get),
            RemoveGroupMembersOp, txn)

          // then if there were no errors, add users that should be added
          val addMembersFuture = removeMembersFuture.flatMap {
            _ match {
              case Some(errorReport) => Future.successful(Option(errorReport))
              case None =>
                val usersToAdd = users.filter(user => !group.users.contains(user))
                val subGroupsToAdd = subGroups.filter(subGroup => !group.subGroups.contains(subGroup))

                // need to reload group cause it changed if members were removed
                updateGroupMembersInternal(containerDAO.authDAO.loadGroup(groupRef, txn).get, usersToAdd, subGroupsToAdd, AddGroupMembersOp, txn)
            }
          }

          // finally report the results
          addMembersFuture.map {
            _ match {
              case None => RequestComplete(StatusCodes.NoContent)
              case Some(error) => throw new RawlsExceptionWithErrorReport(errorReport = error)
            }
          }
        }
      }
    }
  }

  private def withMemberUsersAndGroups(memberList: RawlsGroupMemberList, txn: RawlsTransaction)(op: (Set[RawlsUser], Set[RawlsGroup]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    val users =
      memberList.userEmails.map(_.map(email => (email, containerDAO.authDAO.loadUserByEmail(email, txn)))).getOrElse(Seq.empty) ++
      memberList.userSubjectIds.map(_.map(sub => (sub, containerDAO.authDAO.loadUser(RawlsUserRef(RawlsUserSubjectId(sub)), txn)))).getOrElse(Seq.empty)
    val subGroups =
      memberList.subGroupEmails.map(_.map(email => (email, containerDAO.authDAO.loadGroupByEmail(email, txn)))).getOrElse(Seq.empty) ++
      memberList.subGroupNames.map(_.map(name => (name, containerDAO.authDAO.loadGroup(RawlsGroupRef(RawlsGroupName(name)), txn)))).getOrElse(Seq.empty)

    (users.collect { case (email, None) => email }, subGroups.collect { case (email, None) => email }) match {
      // success case, all users and groups found
      case (Seq(), Seq()) => op(users.map(_._2.get).toSet, subGroups.map(_._2.get).toSet)

      // failure cases, some users and/or groups not found
      case (Seq(), missingGroups) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some groups not found: ${missingGroups.mkString(", ")}")))
      case (missingUsers, Seq()) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}")))
      case (missingUsers, missingGroups) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}. Some groups not found: ${missingGroups.mkString(", ")}")))
    }
  }

  def updateGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList, operation: UpdateGroupMembersOp): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction() { txn =>
      withGroup(groupRef, txn) { group =>
        withMemberUsersAndGroups(memberList, txn) { (users, subGroups) =>
          updateGroupMembersInternal(group, users, subGroups, operation, txn) map {
            _ match {
              case None => RequestComplete(StatusCodes.OK)
              case Some(error) => throw new RawlsExceptionWithErrorReport(errorReport = error)
            }
          }
        }
      }
    }
  }

  def synchronizeGroupMembers(groupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        withGroup(groupRef, txn) { group =>
          synchronizeGroupMembersInternal(group, txn) map { syncReport =>
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
  }

  def synchronizeGroupMembersInternal(group: RawlsGroup, txn: RawlsTransaction): Future[SyncReport] = {
    def loadRefs(refs: Set[Either[RawlsUserRef, RawlsGroupRef]]) = {
      refs.map {
        case Left(userRef) => Left(containerDAO.authDAO.loadUser(userRef, txn).getOrElse(throw new RawlsException(s"user $userRef not found")))
        case Right(groupRef) => Right(containerDAO.authDAO.loadGroup(groupRef, txn).getOrElse(throw new RawlsException(s"group $groupRef not found")))
      }
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

    gcsDAO.listGroupMembers(group) flatMap {
      case None => gcsDAO.createGoogleGroup(group) map (_ => Seq.empty[Either[RawlsUserRef, RawlsGroupRef]])
      case Some(members) => Future.successful(members)
    } flatMap { members =>

      val toRemove = members.toSet -- group.users.map(Left(_)) -- group.subGroups.map(Right(_))

      val removeFutures = loadRefs(toRemove).map { removeMember =>
        toFutureTry(gcsDAO.removeMemberFromGoogleGroup(group, removeMember)).map(toSyncReportItem("removed", removeMember, _))
      }

      val realMembers: Set[Either[RawlsUserRef, RawlsGroupRef]] = group.users.map(Left(_)) ++ group.subGroups.map(Right(_))
      val toAdd = realMembers -- members
      val addFutures = loadRefs(toAdd).map { addMember =>
        toFutureTry(gcsDAO.addMemberToGoogleGroup(group, addMember)).map(toSyncReportItem("added", addMember, _))
      }

      Future.sequence(removeFutures ++ addFutures) map (SyncReport(_))
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
   * @return Future(None) if all went well
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
        val exceptions = tries.collect { case Failure(t) => t }
        if (exceptions.isEmpty) {
          None
        } else {
          Option(ErrorReport(StatusCodes.BadRequest, "Unable to update the following member(s)", exceptions.map(ErrorReport(_)).toSeq))
        }
      }
    }
  }

  private def withUser(rawlsUserRef: RawlsUserRef)(op: RawlsUser => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadUser(rawlsUserRef, txn)
    } match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"user [${rawlsUserRef.userSubjectId.value}] not found")))
      case Some(user) => op(user)
    }
  }

  private def withUser(userEmail: RawlsUserEmail)(op: RawlsUser => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadUserByEmail(userEmail.value, txn)
    } match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"user [${userEmail.value}] not found")))
      case Some(user) => op(user)
    }
  }

  private def withGroup(rawlsGroupRef: RawlsGroupRef, txn: RawlsTransaction)(op: RawlsGroup => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.authDAO.loadGroup(rawlsGroupRef, txn) match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found")))
      case Some(group) => op(group)
    }
  }

  private def withBillingProject(projectName: RawlsBillingProjectName)(op: RawlsBillingProject => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.billingDAO.loadProject(projectName, txn)
    } match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"billing project [${projectName.value}] not found")))
      case Some(project) => op(project)
    }
  }

  /**
   * handles a Future [ Seq [ Try [ T ] ] ], calling success with the successful result of the tries or failure with any exceptions
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

