package org.broadinstitute.dsde.rawls.user

import akka.actor.{Actor, Props}
import akka.pattern._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, AdminSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestComplete, RequestCompleteWithLocation, PerRequestMessage}
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

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

  case object ListBillingProjects extends UserServiceMessage
  case class ListBillingProjectsForUser(userEmail: RawlsUserEmail) extends UserServiceMessage
  case class CreateBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class DeleteBillingProject(projectName: RawlsBillingProjectName) extends UserServiceMessage
  case class AddUserToBillingProject(projectName: RawlsBillingProjectName, userEmail: RawlsUserEmail) extends UserServiceMessage
  case class RemoveUserFromBillingProject(projectName: RawlsBillingProjectName, userEmail: RawlsUserEmail) extends UserServiceMessage
}

class UserService(protected val userInfo: UserInfo, dataSource: DataSource, protected val gcsDAO: GoogleServicesDAO, containerDAO: GraphContainerDAO, userDirectoryDAO: UserDirectoryDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with AdminSupport with FutureSupport {
  override def receive = {
    case SetRefreshToken(token) => setRefreshToken(token) pipeTo context.parent
    case GetRefreshTokenDate => getRefreshTokenDate() pipeTo context.parent

    case CreateUser => createUser() pipeTo context.parent
    case GetUserStatus(userRef) => getUserStatus(userRef) pipeTo context.parent
    case EnableUser(userRef) => enableUser(userRef) pipeTo context.parent
    case DisableUser(userRef) => disableUser(userRef) pipeTo context.parent

    // ListBillingProjects is for the current user, not as admin
    // ListBillingProjectsForUser is for any user, as admin

    case ListBillingProjects => listBillingProjects(RawlsUser(userInfo).userEmail) pipeTo context.parent
    case ListBillingProjectsForUser(userEmail) => asAdmin { listBillingProjects(userEmail) } pipeTo context.parent
    case CreateBillingProject(projectName) => createBillingProject(projectName) pipeTo context.parent
    case DeleteBillingProject(projectName) => deleteBillingProject(projectName) pipeTo context.parent
    case AddUserToBillingProject(projectName, userEmail) => addUserToBillingProject(projectName, userEmail) pipeTo context.parent
    case RemoveUserFromBillingProject(projectName, userEmail) => removeUserFromBillingProject(projectName, userEmail) pipeTo context.parent
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

    )))(_ => RequestCompleteWithLocation(StatusCodes.Created, s"/user/${user.userSubjectId}"), handleException("Errors creating user")
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
      Future {
        containerDAO.billingDAO.loadProject(projectName, txn) match {
          case Some(_) =>
            RequestComplete(ErrorReport(StatusCodes.Conflict, s"Cannot create billing project [${projectName.value}] in database because it already exists"))
          case None =>
            containerDAO.billingDAO.saveProject(RawlsBillingProject(projectName, Set.empty), txn)
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

  private def withUser(rawlsUserRef: RawlsUserRef)(op: RawlsUser => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.loadUser(rawlsUserRef, txn)
    } match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"user [${rawlsUserRef.userSubjectId}] not found")))
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
}

