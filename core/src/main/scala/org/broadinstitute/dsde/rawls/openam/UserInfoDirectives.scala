package org.broadinstitute.dsde.rawls.openam

import org.broadinstitute.dsde.rawls.model.UserInfo
import spray.routing.Directive1

/**
 * Directives to get user information.
 */
trait UserInfoDirectives {
  def requireUserInfo(): Directive1[UserInfo]
}
