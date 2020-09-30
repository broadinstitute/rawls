package org.broadinstitute.dsde.rawls.dataaccess.martha

import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.Future

trait DrsResolver {
  val dosDrsUriPattern: String = "^(dos|drs)://.*"
  def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]]
}
