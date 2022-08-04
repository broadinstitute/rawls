package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.server.Directive1
import org.broadinstitute.dsde.rawls.model.UserInfo

/**
 * Directives to get user information.
 */
trait UserInfoDirectives {
  def requireUserInfo(): Directive1[UserInfo]
}
