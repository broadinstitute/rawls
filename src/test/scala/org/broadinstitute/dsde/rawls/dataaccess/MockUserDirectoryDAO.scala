package org.broadinstitute.dsde.rawls.dataaccess

import javax.naming.NameAlreadyBoundException

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsUser, RawlsUserSubjectId}
import spray.http.StatusCodes

import scala.collection.mutable
import scala.concurrent.Future

/**
 * Created by dvoet on 11/6/15.
 */
class MockUserDirectoryDAO extends UserDirectoryDAO{
  val users = mutable.Map[RawlsUserSubjectId, Boolean]()

  override def createUser(user: RawlsUserSubjectId): Future[Unit] = {
    if( users.contains(user) ) {
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, "user already exists")))
    } else {
      Future.successful(users += (user -> false))
    }
  }

  override def removeUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users -= user)

  override def isEnabled(user: RawlsUserSubjectId): Future[Boolean] = Future.successful(users.getOrElse(user, false))

  override def disableUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users += (user -> false))

  override def enableUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users += (user -> true))

  def exists(user: RawlsUser) = users.keys.exists(_ == user)
}
