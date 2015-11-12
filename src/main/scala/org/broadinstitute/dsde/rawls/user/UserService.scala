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
import scala.util.{Try, Failure}

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
  case class AddGroupMember(groupName: String, memberEmail: String) extends UserServiceMessage
  case class RemoveGroupMember(groupName: String, memberEmail: String) extends UserServiceMessage
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
    case AddGroupMember(groupName, memberEmail) => addGroupMember(groupName, memberEmail) to context.parent
    case RemoveGroupMember(groupName, memberEmail) => removeGroupMember(groupName, memberEmail) to context.parent
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


  // **change this back once more important stuff is fixed
  def listAdmins() = {
    asAdmin {
      //gcsDAO.listAdmins.map(RequestComplete(StatusCodes.OK, _)).recover{ case throwable =>
      //  RequestComplete(ErrorReport(StatusCodes.BadGateway,"Unable to list admins.",gcsDAO.toErrorReport(throwable)))
      //}
      Future.successful(RequestComplete(StatusCodes.OK))
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
      dataSource.inFutureTransaction() { txn =>
        Future {
          containerDAO.authDAO.loadGroup(groupRef, txn) match {
            case Some(_) => RequestComplete(ErrorReport(StatusCodes.Conflict, s"Group ${groupRef.groupName} already exists"))
            case None =>
              gcsDAO.createGoogleGroup(groupRef)
              containerDAO.authDAO.createGroup(RawlsGroup(groupRef.groupName, RawlsGroupEmail(gcsDAO.toGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef]), txn)
              RequestComplete(StatusCodes.Created)
          }
        }
      }
    }
  }

  def deleteGroup(groupRef: RawlsGroupRef) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        Future {
          containerDAO.authDAO.loadGroup(groupRef, txn) match {
            case None => RequestComplete(ErrorReport(StatusCodes.NotFound, s"Group ${groupRef.groupName} does not exist"))
            case Some(_) =>
              gcsDAO.deleteGoogleGroup(groupRef)
              containerDAO.authDAO.deleteGroup(groupRef, txn)
              RequestComplete(StatusCodes.OK)
          }
        }
      }
    }
  }

  //this is used a lot. is the memberEmail associated with a user or a group?
  def isGroup(memberEmail: String): Boolean = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadGroupByEmail(memberEmail, txn) match {
        case Some(group) => true
        case None => false
      }
    }
  }

  def isUser(memberEmail: String): Boolean = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadUserByEmail(memberEmail, txn) match {
        case Some(group) => true
        case None => false
      }
    }
  }

  def addGroupMember(groupName: String, memberEmail: String) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        Future {
          val groupRef = RawlsGroupRef(RawlsGroupName(groupName))
          val group = containerDAO.authDAO.loadGroup(groupRef, txn).getOrElse(throw new RawlsException(s"Group ${groupName} does not exist"))
          if(isUser(memberEmail)) {
            val member = containerDAO.authDAO.loadUserByEmail(memberEmail, txn).get
            gcsDAO.addMemberToGoogleGroup(groupRef, gcsDAO.toProxyFromUser(member.userSubjectId))
            containerDAO.authDAO.saveGroup(group.copy(users = group.users + RawlsUserRef(member.userSubjectId)), txn)
            RequestComplete(StatusCodes.OK)
          }
          else if(isGroup(memberEmail)) {
            val member = containerDAO.authDAO.loadGroupByEmail(memberEmail, txn).get
            gcsDAO.addMemberToGoogleGroup(groupRef, memberEmail)
            containerDAO.authDAO.saveGroup(group.copy(subGroups = group.subGroups + RawlsGroupRef(member.groupName)), txn)
            RequestComplete(StatusCodes.OK)
          }
          else {
            RequestComplete(ErrorReport(StatusCodes.NotFound, s"Member ${memberEmail} does not exist"))
          }
        }
      }
    }
  }

  def removeGroupMember(groupName: String, memberEmail: String) = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        Future {
          val groupRef = RawlsGroupRef(RawlsGroupName(groupName))
          val group = containerDAO.authDAO.loadGroup(groupRef, txn).getOrElse(throw new RawlsException(s"Group ${groupName} does not exist"))
          if(isUser(memberEmail)) {
            val member = containerDAO.authDAO.loadUserByEmail(memberEmail, txn).get
            gcsDAO.removeMemberFromGoogleGroup(groupRef, gcsDAO.toProxyFromUser(member.userSubjectId))
            containerDAO.authDAO.saveGroup(group.copy(users = group.users - RawlsUserRef(RawlsUserSubjectId(member.userSubjectId.value))), txn)
            RequestComplete(StatusCodes.OK)
          }
          else {
            val member = containerDAO.authDAO.loadGroupByEmail(memberEmail, txn).get
            gcsDAO.removeMemberFromGoogleGroup(groupRef, memberEmail)
            containerDAO.authDAO.saveGroup(group.copy(subGroups = group.subGroups - RawlsGroupRef(RawlsGroupName(member.groupName.value))), txn)
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

