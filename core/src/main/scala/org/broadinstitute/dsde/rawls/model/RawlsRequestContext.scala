package org.broadinstitute.dsde.rawls.model

import io.opencensus.trace.Span

final case class RawlsRequestContext(userInfo: UserInfo, tracingSpan: Option[Span] = None)
final case class RawlsTracingContext(tracingSpan: Option[Span] = None)
