package org.broadinstitute.dsde.rawls.dataaccess

import scala.concurrent.Future

trait DosResolver {
  val dosUriPattern: String = "^dos://.*"
  def dosServiceAccountEmail(dos: String): Future[Option[String]]
}
