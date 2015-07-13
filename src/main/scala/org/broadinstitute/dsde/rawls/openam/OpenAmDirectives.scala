package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1

import scala.concurrent.ExecutionContext

/**
 * This trait allows us to specify directives that may be implemented either by the OpenAM directives class
 * in vault-common or the mock OpenAM directives in our test suite.
 */
trait OpenAmDirectives {
  def usernameFromCookie(magnet: ImplicitMagnet[ExecutionContext]): Directive1[String]
}
