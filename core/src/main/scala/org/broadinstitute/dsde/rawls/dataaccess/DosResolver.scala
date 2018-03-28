package org.broadinstitute.dsde.rawls.dataaccess

import scala.concurrent.Future

trait DosResolver {
  val dosUriPattern: String = "^dos://.*"
  def dosToGs(v: String): Future[String]
}
