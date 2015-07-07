package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1
import spray.routing.Directives._

import scala.concurrent.{Future, ExecutionContext}

trait MockOpenAmDirectives extends OpenAmDirectives {
  def usernameFromCookie(magnet: ImplicitMagnet[ExecutionContext]): Directive1[String] = {
    // just return the cookie text as the common name
    cookie("iPlanetDirectoryPro") map { _.content }
  }
}
