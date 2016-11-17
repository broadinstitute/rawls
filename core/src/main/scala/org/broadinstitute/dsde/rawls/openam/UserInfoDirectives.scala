package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1

import scala.concurrent.ExecutionContext

/**
 * Directives to get user information.
 */
trait UserInfoDirectives {
  def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo]
}
