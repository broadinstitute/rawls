package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.server.Directive1
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.model.UserInfo

/**
 * Directives to get user information.
 */
trait UserInfoDirectives {
  def requireUserInfo(span: Option[Span] = None): Directive1[UserInfo]
}
