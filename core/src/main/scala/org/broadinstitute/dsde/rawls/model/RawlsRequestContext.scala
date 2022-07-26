package org.broadinstitute.dsde.rawls.model

import io.opencensus.trace.Span

case class RawlsRequestContext(userInfo: UserInfo, traceSpan: Option[Span] = None)
