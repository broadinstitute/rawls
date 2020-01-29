package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by dvoet on 11/5/15.
 */
trait UserDirectoryDAO {
  def createUser(user: RawlsUserSubjectId, email: RawlsUserEmail): Future[Unit]
  def removeUser(user: RawlsUserSubjectId): Future[Unit]
  def enableUser(user: RawlsUserSubjectId): Future[Unit]
  def disableUser(user: RawlsUserSubjectId): Future[Unit]
  def isEnabled(user: RawlsUserSubjectId): Future[Boolean]
  def listUsers(implicit executionContext: ExecutionContext): Future[List[RawlsUserSubjectId]]
}
