package org.broadinstitute.dsde.rawls.dataaccess

import javax.naming.NameAlreadyBoundException

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsUser, RawlsUserSubjectId}
import spray.http.StatusCodes

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

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

  override def getAnyUser(implicit executionContext: ExecutionContext): Future[RawlsUserSubjectId] = users.keys.headOption match {
    case Some(user) => Future.successful(user)
    case None => Future.failed(new RawlsException("no users found"))
  }

  def exists(user: RawlsUserSubjectId) = users.keys.exists(_ == user)
}
