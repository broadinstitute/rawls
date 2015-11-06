package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.RawlsUser

import scala.collection.mutable
import scala.concurrent.Future

/**
 * Created by dvoet on 11/6/15.
 */
class MockUserDirectoryDAO extends UserDirectoryDAO{
  val users = mutable.Map[RawlsUser, Boolean]()

  override def createUser(user: RawlsUser): Future[Unit] = Future.successful(users += (user -> false))

  override def isEnabled(user: RawlsUser): Future[Boolean] = Future.successful(users.getOrElse(user, false))

  override def disableUser(user: RawlsUser): Future[Unit] = Future.successful(users += (user -> false))

  override def enableUser(user: RawlsUser): Future[Unit] = Future.successful(users += (user -> true))

  def exists(user: RawlsUser) = users.keys.exists(_ == user)
}
