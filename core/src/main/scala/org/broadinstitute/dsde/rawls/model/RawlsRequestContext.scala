package org.broadinstitute.dsde.rawls.model

import io.opentelemetry.context.Context

final case class RawlsRequestContext(userInfo: UserInfo, otelContext: Option[Context] = None) {
  def toTracingContext: RawlsTracingContext = RawlsTracingContext(otelContext)
}
final case class RawlsTracingContext(otelContext: Option[Context] = None)
