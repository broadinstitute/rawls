package org.broadinstitute.dsde.rawls.user

import akka.actor.{Actor, Props}
import akka.pattern._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, DataSource}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, UserRefreshTokenDate, UserInfo, UserRefreshToken}
import org.broadinstitute.dsde.rawls.user.UserService._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestComplete, PerRequestMessage}
import spray.http.StatusCodes
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.httpx.SprayJsonSupport._

import scala.concurrent.{Future, ExecutionContext}

/**
 * Created by dvoet on 10/27/15.
 */
object UserService {
  def props(userServiceConstructor: UserInfo => UserService, userInfo: UserInfo): Props = {
    Props(userServiceConstructor(userInfo))
  }

  def constructor(dataSource: DataSource, googleServicesDAO: GoogleServicesDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new UserService(userInfo, dataSource, googleServicesDAO)

  sealed trait UserServiceMessage
  case class SetRefreshToken(token: UserRefreshToken) extends UserServiceMessage
  case object GetRefreshTokenDate extends UserServiceMessage
}

class UserService(userInfo: UserInfo, dataSource: DataSource, googleServicesDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext) extends Actor {
  override def receive = {
    case SetRefreshToken(token) => setRefreshToken(token) pipeTo context.parent
    case GetRefreshTokenDate => getRefreshTokenDate() pipeTo context.parent
  }

  def setRefreshToken(userRefreshToken: UserRefreshToken): Future[PerRequestMessage] = {
    googleServicesDAO.storeToken(userInfo, userRefreshToken.refreshToken).map(_ => RequestComplete(StatusCodes.Created))
  }

  def getRefreshTokenDate(): Future[PerRequestMessage] = {
    googleServicesDAO.getTokenDate(userInfo).map( _ match {
      case None => RequestComplete(ErrorReport(StatusCodes.NotFound, s"no refresh token stored for ${userInfo.userEmail}"))
      case Some(date) => RequestComplete(UserRefreshTokenDate(date))
    })
  }
}
