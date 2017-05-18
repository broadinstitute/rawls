package org.broadinstitute.dsde.rawls.health

import org.broadinstitute.dsde.rawls.dataaccess.UserDirectoryDAO

import scala.concurrent._

sealed trait Monitorable[T] {
  def test(t: T)(implicit executionContext: ExecutionContext): Map[String, Future[Nothing]]
}

/**
  * Created by rtitle on 5/17/17.
  */
object HealthMonitorInstances {

  implicit object LdapMonitor extends Monitorable[UserDirectoryDAO] {
    def test(dao: UserDirectoryDAO)(implicit executionContext: ExecutionContext) =
      Map("ldap" -> dao.getAnyUser.mapTo[Nothing])
  }

}
