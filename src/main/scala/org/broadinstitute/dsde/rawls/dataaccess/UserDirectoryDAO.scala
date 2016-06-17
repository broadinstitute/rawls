package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.RawlsUser

import scala.concurrent.Future

/**
 * Created by dvoet on 11/5/15.
 */
trait UserDirectoryDAO {
  def createUser(user: RawlsUser): Future[Unit]
  def removeUser(user: RawlsUser): Future[Unit]
  def enableUser(user: RawlsUser): Future[Unit]
  def disableUser(user: RawlsUser): Future[Unit]
  def isEnabled(user: RawlsUser): Future[Boolean]
}
