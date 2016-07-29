package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserSubjectId}

import scala.collection.mutable
import scala.concurrent.Future

/**
 * Created by dvoet on 11/6/15.
 */
class MockUserDirectoryDAO extends UserDirectoryDAO{
  val users = mutable.Map[RawlsUserSubjectId, Boolean]()

  override def createUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users += (user -> false))

  override def removeUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users -= user)

  override def isEnabled(user: RawlsUserSubjectId): Future[Boolean] = Future.successful(users.getOrElse(user, false))

  override def disableUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users += (user -> false))

  override def enableUser(user: RawlsUserSubjectId): Future[Unit] = Future.successful(users += (user -> true))

  def exists(user: RawlsUser) = users.keys.exists(_ == user)
}
