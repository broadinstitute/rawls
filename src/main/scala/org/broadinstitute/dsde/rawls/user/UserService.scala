package org.broadinstitute.dsde.rawls.user

import akka.actor.{Actor, Props}
import akka.pattern._
import com.google.api.client.http.HttpResponseException
import org.broadinstitute.dsde.rawls.RawlsException
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

  def constructor(dataSource: DataSource, googleServicesDAO: GoogleServicesDAO, containerDAO: GraphContainerDAO, userDirectoryDAO: UserDirectoryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, containerDAO, userDirectoryDAO)

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
  case class AddGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
  case class RemoveGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) extends UserServiceMessage
}

class UserService(protected val userInfo: UserInfo, dataSource: DataSource, protected val gcsDAO: GoogleServicesDAO, containerDAO: GraphContainerDAO, userDirectoryDAO: UserDirectoryDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with AdminSupport with FutureSupport {
  override def receive = {
    case SetRefreshToken(token) => setRefreshToken(token) pipeTo context.parent
    case GetRefreshTokenDate => getRefreshTokenDate() pipeTo context.parent

    case CreateUser => createUser() pipeTo context.parent
    case AdminGetUserStatus(userRef) => adminGetUserStatus(userRef) pipeTo context.parent
    case UserGetUserStatus => userGetUserStatus() pipeTo context.parent
    case EnableUser(userRef) => enableUser(userRef) pipeTo context.parent
    case DisableUser(userRef) => disableUser(userRef) pipeTo context.parent
    case ListUsers => listUsers pipeTo context.parent
    case ImportUsers(rawlsUserInfoList) => importUsers(rawlsUserInfoList) pipeTo context.parent
    case GetUserGroup(groupRef) => getUserGroup(groupRef) pipeTo context.parent

    // ListBillingProjects is for the current user, not as admin
    // ListBillingProjectsForUser is for any user, as admin

    case ListBillingProjects => listBillingProjects(RawlsUser(userInfo).userEmail) pipeTo context.parent
    case ListBillingProjectsForUser(userEmail) => asAdmin { listBillingProjects(userEmail) } pipeTo context.parent
    case CreateBillingProject(projectName) => createBillingProject(projectName) pipeTo context.parent
    case DeleteBillingProject(projectName) => deleteBillingProject(projectName) pipeTo context.parent
    case AddUserToBillingProject(projectName, userEmail) => addUserToBillingProject(projectName, userEmail) pipeTo context.parent
    case RemoveUserFromBillingProject(projectName, userEmail) => removeUserFromBillingProject(projectName, userEmail) pipeTo context.parent

    case CreateGroup(groupRef) => pipe(createGroup(groupRef)) to context.parent
    case ListGroupMembers(groupName) => pipe(listGroupMembers(groupName)) to context.parent
    case DeleteGroup(groupName) => pipe(deleteGroup(groupName)) to context.parent
    case AddGroupMembers(groupName, memberList) => updateGroupMembers(groupName, memberList, AddGroupMembersOp) to context.parent
    case RemoveGroupMembers(groupName, memberList) => updateGroupMembers(groupName, memberList, RemoveGroupMembersOp) to context.parent
  }

  def setRefreshToken(userRefreshToken: UserRefreshToken): Future[PerRequestMessage] = {
    gcsDAO.storeToken(userInfo, userRefreshToken.refreshToken).map(_ => RequestComplete(StatusCodes.Created))
  }

  def getRefreshTokenDate(): Future[PerRequestMessage] = {
    gcsDAO.getTokenDate(userInfo).map( _ match {
      case None => RequestComplete(ErrorReport(StatusCodes.NotFound, s"no refresh token stored for ${userInfo.userEmail}"))
      case Some(date) => RequestComplete(UserRefreshTokenDate(date))
    })
  }

  def createUser(): Future[PerRequestMessage] = {
    val user = RawlsUser(userInfo)

    // if there is any error, may leave user in weird state which can be seen with getUserStatus
    // retrying this call will retry the failures, failures due to already created groups/entries are ok
    handleFutures(Future.sequence(Seq(
      toFutureTry(gcsDAO.createProxyGroup(user)),
      toFutureTry(dataSource.inFutureTransaction() { txn =>
        Future(containerDAO.authDAO.saveUser(user, txn)).
          flatMap(user => addUsersToAllUsersGroup(Seq(user), txn)) }),
      toFutureTry(userDirectoryDAO.createUser(user))

    )))(_ => RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId.value}"), handleException("Errors creating user")
    )
  }

  import spray.json.DefaultJsonProtocol._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsUserInfoListFormat

  def listUsers(): Future[PerRequestMessage] = asAdmin {
    Future(dataSource.inTransaction() { txn =>
      val users = containerDAO.authDAO.loadAllUsers(txn)
      val userInfoList = users.map(u => RawlsUserInfo(u, containerDAO.billingDAO.listUserProjects(u, txn).toSeq))
      RequestComplete(RawlsUserInfoList(userInfoList))
    })
  }

  //imports the response from the above listUsers
  def importUsers(rawlsUserInfoList: RawlsUserInfoList): Future[PerRequestMessage] = asAdmin {
    dataSource.inFutureTransaction() { txn =>
      Future { 
        val userInfos = rawlsUserInfoList.userInfoList
        userInfos.map { u =>
          val user = containerDAO.authDAO.saveUser(u.user, txn)
          u.billingProjects.foreach(b =>
            containerDAO.billingDAO.addUserToProject(u.user, containerDAO.billingDAO.loadProject(b, txn).get, txn)
          )
          user
        }
      } flatMap { users =>
        addUsersToAllUsersGroup(users, txn)
      } map(_ match {
        case None => RequestComplete(StatusCodes.Created)
        case Some(error) => RequestComplete(error)
      })
    }
  }

  def getUserGroup(rawlsGroupRef: RawlsGroupRef): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadGroupIfMember(rawlsGroupRef, RawlsUser(userInfo), txn) match {
        case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found or member not in group")))
        case Some(group) => Future.successful(RequestComplete(group.toRawlsGroupShort))
      }
    }
  }

  private def addUsersToAllUsersGroup(users: Seq[RawlsUser], txn: RawlsTransaction): Future[Option[ErrorReport]] = {
    getOrCreateAllUsersGroup(txn) flatMap { allUsersGroup =>
      updateGroupMembersInternal(allUsersGroup, users, Seq.empty, AddGroupMembersOp, txn)
    }
  }

  private def getOrCreateAllUsersGroup(txn: RawlsTransaction): Future[RawlsGroup] = {
    Future {
      containerDAO.authDAO.loadGroup(allUsersGroupRef, txn)
    } flatMap (_ match {
      case Some(g) => Future.successful(g)
      case None => createGroupInternal(allUsersGroupRef, txn).recover {
        // this case is where the group was not in our db but already in google, the recovery code makes the assumption
        // that createGroupInternal saves the group in our db before creating the group in google so loadGroup should work
        case t: HttpResponseException if t.getStatusCode == StatusCodes.Conflict.intValue => containerDAO.authDAO.loadGroup(allUsersGroupRef, txn).get
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
            RequestComplete(ErrorReport(StatusCodes.Conflict, s"Cannot create billing project [${projectName.value}] in database because it already exists"))
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
          else RequestComplete(ErrorReport(StatusCodes.InternalServerError, s"Could not delete billing project [${projectName.value}]"))
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
            case None => RequestComplete(ErrorReport(StatusCodes.NotFound, s"Group ${groupName} does not exist"))
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
      dataSource.inTransaction() { txn =>
        containerDAO.authDAO.loadGroup(groupRef, txn) match {
          case Some(_) => Future.successful(RequestComplete(ErrorReport(StatusCodes.Conflict, s"Group ${groupRef.groupName} already exists")))
          case None =>
            createGroupInternal(groupRef, txn) map { _ => RequestComplete(StatusCodes.Created) }
        }
      }
    }
  }

  private def createGroupInternal(groupRef: RawlsGroupRef, txn: RawlsTransaction): Future[RawlsGroup] = {
    val rawlsGroup = containerDAO.authDAO.saveGroup(RawlsGroup(groupRef.groupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef]), txn)
    gcsDAO.createGoogleGroup(groupRef).map(_ => rawlsGroup)
  }

  def deleteGroup(groupRef: RawlsGroupRef) = {
    asAdmin {
      dataSource.inTransaction() { txn =>
        withGroup(groupRef, txn) { group =>
          containerDAO.authDAO.deleteGroup(groupRef, txn)
          gcsDAO.deleteGoogleGroup(groupRef) map { _ => RequestComplete(StatusCodes.OK) }
        }
      }
    }
  }

  def updateGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList, operation: UpdateGroupMembersOp): Future[PerRequestMessage] = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        withGroup(groupRef, txn) { group =>
          val users = memberList.userEmails.map(email => (email, containerDAO.authDAO.loadUserByEmail(email, txn)))
          val subGroups = memberList.subGroupEmails.map(email => (email, containerDAO.authDAO.loadGroupByEmail(email, txn)))

          (users.collect { case (email, None) => email }, subGroups.collect { case (email, None) => email }) match {
            // success case, all users and groups found
            case (Seq(), Seq()) =>
              updateGroupMembersInternal(group, users.map(_._2.get), subGroups.map(_._2.get), operation, txn) map {
                _ match {
                  case None => RequestComplete(StatusCodes.OK)
                  case Some(error) => RequestComplete(error)
                }
              }

            // failure cases, some users and/or groups not found
            case (Seq(), missingGroups) => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Some groups not found: ${missingGroups.mkString(", ")}")))
            case (missingUsers, Seq()) => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}")))
            case (missingUsers, missingGroups) => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Some users not found: ${missingUsers.mkString(", ")}. Some groups not found: ${missingGroups.mkString(", ")}")))
          }
        }
      }
    }
  }

  /**
   * updates the contents of a group. in the event of a failure updating the user in the google group, the user will not be updated in the RawlsGroup.
   *
   * @param group the group the update
   * @param users users to add or remove from the group
   * @param subGroups sub groups to add or remove from the group
   * @param operation which update operation to perform
   * @param txn the transaction to operate within
   * @return Future(None) if all went well
   */
  private def updateGroupMembersInternal(group: RawlsGroup, users: Seq[RawlsUser], subGroups: Seq[RawlsGroup], operation: UpdateGroupMembersOp, txn: RawlsTransaction): Future[Option[ErrorReport]] = {
    // update the google group, each update happens in the future and may or may not succeed.
    val googleUpdateTrials: Seq[Future[Try[Either[RawlsUser, RawlsGroup]]]] = users.map { user =>
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
    Future.sequence(googleUpdateTrials).map { tries =>
      val successfulUsers = tries.collect { case Success(Left(member)) => RawlsUser.toRef(member) }.toSet
      val successfulGroups = tries.collect { case Success(Right(member)) => RawlsGroup.toRef(member) }.toSet
      containerDAO.authDAO.saveGroup(operation.updateGroupObject(group, successfulUsers, successfulGroups), txn)

      val exceptions = tries.collect { case Failure(t) => t }
      if (exceptions.isEmpty) {
        None
      } else {
        Option(ErrorReport(StatusCodes.BadRequest, "Unable to update the following member(s)", exceptions.map(ErrorReport(_))))
      }
    }
  }

  private def withUser(rawlsUserRef: RawlsUserRef)(op: RawlsUser => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadUser(rawlsUserRef, txn)
    } match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"user [${rawlsUserRef.userSubjectId.value}] not found")))
      case Some(user) => op(user)
    }
  }

  private def withUser(userEmail: RawlsUserEmail)(op: RawlsUser => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadUserByEmail(userEmail.value, txn)
    } match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"user [${userEmail.value}] not found")))
      case Some(user) => op(user)
    }
  }

  private def withGroup(rawlsGroupRef: RawlsGroupRef, txn: RawlsTransaction)(op: RawlsGroup => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.authDAO.loadGroup(rawlsGroupRef, txn) match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found")))
      case Some(group) => op(group)
    }
  }

  private def withBillingProject(projectName: RawlsBillingProjectName)(op: RawlsBillingProject => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.billingDAO.loadProject(projectName, txn)
    } match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"billing project [${projectName.value}] not found")))
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
    RequestComplete(ErrorReport(StatusCodes.InternalServerError, message, exceptions.map(ErrorReport(_))))
  }

  private sealed trait UpdateGroupMembersOp {
    def updateGoogle(groupRef: RawlsGroupRef, member: Either[RawlsUser, RawlsGroup]): Future[Unit]
    def updateGroupObject(group: RawlsGroup, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): RawlsGroup
  }
  private object AddGroupMembersOp extends UpdateGroupMembersOp {
    override def updateGoogle(groupRef: RawlsGroupRef, member: Either[RawlsUser, RawlsGroup]): Future[Unit] = gcsDAO.addMemberToGoogleGroup(groupRef, member)
    override def updateGroupObject(group: RawlsGroup, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): RawlsGroup = group.copy( users = (group.users ++ users), subGroups = (group.subGroups ++ subGroups))
  }
  private object RemoveGroupMembersOp extends UpdateGroupMembersOp {
    override def updateGoogle(groupRef: RawlsGroupRef, member: Either[RawlsUser, RawlsGroup]): Future[Unit] = gcsDAO.removeMemberFromGoogleGroup(groupRef, member)
    override def updateGroupObject(group: RawlsGroup, users: Set[RawlsUserRef], subGroups: Set[RawlsGroupRef]): RawlsGroup = group.copy( users = (group.users -- users), subGroups = (group.subGroups -- subGroups))
  }
}

