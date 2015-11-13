package org.broadinstitute.dsde.rawls.user

import akka.actor.{Actor, Props}
import akka.pattern._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{UserDirectoryDAO, GraphContainerDAO, GoogleServicesDAO, DataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, AdminSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestCompleteWithLocation, PerRequestMessage, RequestComplete}
import spray.http.StatusCodes

import spray.json._
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Try, Failure}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  def props(userServiceConstructor: UserInfo => UserService, userInfo: UserInfo): Props = {
    Props(userServiceConstructor(userInfo))
  }

  def constructor(dataSource: DataSource, googleServicesDAO: GoogleServicesDAO, containerDAO: GraphContainerDAO, userDirectoryDAO: UserDirectoryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO, containerDAO, userDirectoryDAO)

  sealed trait UserServiceMessage
  case class SetRefreshToken(token: UserRefreshToken) extends UserServiceMessage
  case object GetRefreshTokenDate extends UserServiceMessage
  case object CreateUser extends UserServiceMessage
  case class GetUserStatus(userRef: RawlsUserRef) extends UserServiceMessage
  case class EnableUser(userRef: RawlsUserRef) extends UserServiceMessage
  case class DisableUser(userRef: RawlsUserRef) extends UserServiceMessage

  case object ListAdmins extends UserServiceMessage
  case class IsAdmin(userId: String) extends UserServiceMessage
  case class AddAdmin(userId: String) extends UserServiceMessage
  case class DeleteAdmin(userId: String) extends UserServiceMessage

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
    case GetUserStatus(userRef) => getUserStatus(userRef) pipeTo context.parent
    case EnableUser(userRef) => enableUser(userRef) pipeTo context.parent
    case DisableUser(userRef) => disableUser(userRef) pipeTo context.parent

    case ListAdmins => pipe(listAdmins()) to context.parent
    case IsAdmin(userId) => pipe(isAdmin(userId)) to context.parent
    case AddAdmin(userId) => pipe(addAdmin(userId)) to context.parent
    case DeleteAdmin(userId) => pipe(deleteAdmin(userId)) to context.parent

    case CreateGroup(groupRef) => pipe(createGroup(groupRef)) to context.parent
    case ListGroupMembers(groupName) => pipe(listGroupMembers(groupName)) to context.parent
    case DeleteGroup(groupName) => pipe(deleteGroup(groupName)) to context.parent
    case AddGroupMembers(groupName, memberList) => addGroupMembers(groupName, memberList) to context.parent
    case RemoveGroupMembers(groupName, memberList) => removeGroupMembers(groupName, memberList) to context.parent
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
      toFutureTry(Future(dataSource.inTransaction()(txn => containerDAO.authDAO.saveUser(user, txn)))),
      toFutureTry(userDirectoryDAO.createUser(user))

    )))(_ => RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId.value}"), handleException("Errors creating user")
    )
  }

  def getUserStatus(userRef: RawlsUserRef): Future[PerRequestMessage] = asAdmin {
    withUser(userRef) { user =>
      handleFutures(Future.sequence(Seq(
        toFutureTry(gcsDAO.isUserInProxyGroup(user).map("google" -> _)),
        toFutureTry(userDirectoryDAO.isEnabled(user).map("ldap" -> _))

      )))(statuses => RequestComplete(UserStatus(user, statuses.toMap)), handleException("Error checking if a user is enabled"))
    }
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

  def listAdmins() = {
    asAdmin {
      gcsDAO.listAdmins.map(users => RequestComplete(StatusCodes.OK, UserList(users))).recover{ case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway,"Unable to list admins.",gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def isAdmin(userId: String) = {
    asAdmin {
      tryIsAdmin(userId) map { admin =>
        if (admin) RequestComplete(StatusCodes.NoContent)
        else RequestComplete(ErrorReport(StatusCodes.NotFound, s"User ${userId} is not an admin."))
      } recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to determine whether ${userId} is an admin.", gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def addAdmin(userId: String) = {
    asAdmin {
      tryIsAdmin(userId) flatMap { admin =>
        if (admin) {
          Future.successful(RequestComplete(StatusCodes.NoContent))
        } else {
          gcsDAO.addAdmin(userId) map (_ => RequestComplete(StatusCodes.Created)) recover { case throwable =>
            RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to add ${userId} as an admin.", gcsDAO.toErrorReport(throwable)))
          }
        }
      } recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to determine whether ${userId} is an admin.", gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def deleteAdmin(userId: String) = {
    asAdmin {
      tryIsAdmin(userId) flatMap { admin =>
        if (admin) {
          gcsDAO.deleteAdmin(userId) map (_ => RequestComplete(StatusCodes.NoContent)) recover { case throwable =>
            RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to delete ${userId} as an admin.", gcsDAO.toErrorReport(throwable)))
          }
        } else {
          Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound,s"${userId} is not an admin.")))
        }
      } recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to determine whether ${userId} is an admin.", gcsDAO.toErrorReport(throwable)))
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
            containerDAO.authDAO.createGroup(RawlsGroup(groupRef.groupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef]), txn)
            gcsDAO.createGoogleGroup(groupRef) map { _ => RequestComplete(StatusCodes.Created) }
        }
      }
    }
  }

  def deleteGroup(groupRef: RawlsGroupRef) = {
    asAdmin {
      dataSource.inTransaction() { txn =>
        withGroup(groupRef) { group =>
          containerDAO.authDAO.deleteGroup(groupRef, txn)
          gcsDAO.deleteGoogleGroup(groupRef) map { _ => RequestComplete(StatusCodes.OK) }
        }
      }
    }
  }

  //ideally this would just return the already loaded users to avoid loading twice
  def allMembersExist(memberList: RawlsGroupMemberList): Boolean = {
    dataSource.inTransaction() { txn =>
      memberList.userEmails.foreach { user =>
        containerDAO.authDAO.loadUserByEmail(user, txn).getOrElse(return false)
      }
      memberList.subGroupEmails.foreach { subGroup =>
        containerDAO.authDAO.loadGroupByEmail(subGroup, txn).getOrElse(return false)
      }
      true
    }
  }

  def addGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        withGroup(groupRef) { group =>
          Future {
            if (!allMembersExist(memberList))
              RequestComplete(StatusCodes.NotFound, "Not all members are registered. Please ensure that all users/groups exist")
            else {
              val addMap = memberList.userEmails.map { user =>
                val newUser = containerDAO.authDAO.loadUserByEmail(user, txn).get
                val addTry = toFutureTry(gcsDAO.addMemberToGoogleGroup(groupRef, Left(newUser)))
                addTry.map(Left(newUser) -> _)
              } ++ memberList.subGroupEmails.map { subGroup =>
                val newGroup = containerDAO.authDAO.loadGroupByEmail(subGroup, txn).get
                val addTry = toFutureTry(gcsDAO.addMemberToGoogleGroup(groupRef, Right(newGroup)))
                addTry.map(Right(newGroup) -> _)
              }


              Future.sequence(addMap) map { case (user, result) =>
                  result match {
                    case Success(_) => user match { //determine if the member is a user or a group, then add appropriately
                    }
                    case Failure(t) => // return exception, collecting the errors and returning them to the user but still going through with anything that's successful
                  }
              }
              RequestComplete(StatusCodes.OK)
            }
          }
        }
      }
    }
  }

  //do same as above
  def removeGroupMembers(groupRef: RawlsGroupRef, memberList: RawlsGroupMemberList) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        Future {
          if(!allMembersExist(memberList))
            RequestComplete(StatusCodes.NotFound, "Not all members are registered. Please ensure that all users/groups exist")
          else {
            val group = containerDAO.authDAO.loadGroup(groupRef, txn).getOrElse(throw new RawlsException(s"Group ${groupRef.groupName.value} does not exist"))
            memberList.userEmails.foreach { user =>
              val newUser = containerDAO.authDAO.loadUserByEmail(user, txn).get
              gcsDAO.removeMemberFromGoogleGroup(groupRef, Left(newUser))
              containerDAO.authDAO.saveGroup(group.copy(users = group.users - RawlsUserRef(newUser.userSubjectId)), txn)
            }
            memberList.subGroupEmails.foreach { subGroup =>
              val newGroup = containerDAO.authDAO.loadGroupByEmail(subGroup, txn).get
              gcsDAO.removeMemberFromGoogleGroup(groupRef, Right(newGroup))
              containerDAO.authDAO.saveGroup(group.copy(subGroups = group.subGroups - RawlsGroupRef(newGroup.groupName)), txn)
            }
            RequestComplete(StatusCodes.OK)
          }
        }
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

  private def withGroup(rawlsGroupRef: RawlsGroupRef)(op: RawlsGroup => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadGroup(rawlsGroupRef, txn)
    } match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"group [${rawlsGroupRef.groupName.value}] not found")))
      case Some(group) => op(group)
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
}

