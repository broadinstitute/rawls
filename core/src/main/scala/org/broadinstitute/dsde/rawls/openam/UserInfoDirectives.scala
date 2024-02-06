package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.server.Directive1
import io.opencensus.trace.Span
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.model.UserInfo

/**
 * Directives to get user information.
 */
trait UserInfoDirectives {
  def requireUserInfo(otelContext: Option[Context] = None): Directive1[UserInfo]
}
