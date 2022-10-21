package org.broadinstitute.dsde.rawls.dataaccess.drs

import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.Future

trait DrsResolver {
  def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]]
}

object DrsResolver {
  val dosDrsUriPattern: String = "^(dos|drs)://.*"
}

case class ServiceAccountPayload(data: Option[ServiceAccountEmail])
case class ServiceAccountEmail(client_email: String)
