package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.Future

trait DosResolver {
  val dosUriPattern: String = "^(dos|drs)://.*"
  def dosServiceAccountEmail(dos: String, userInfo: UserInfo): Future[Option[String]]
}
