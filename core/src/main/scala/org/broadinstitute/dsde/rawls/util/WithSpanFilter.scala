package org.broadinstitute.dsde.rawls.util

import io.opentelemetry.context.{Context, Scope}
import jakarta.ws.rs.client.{ClientRequestContext, ClientRequestFilter, ClientResponseContext, ClientResponseFilter}
import jakarta.ws.rs.ext.Provider

/**
 * Deferred execution in Scala (Futures, IOs, DBIOs, etc) result in a thread switchery. OpenCensus tracing
 * often uses thread local variables to maintain trace information which does not work in this case.
 * This filter holds the span and sets it appropriately during request processing when we are pretty sure
 * there is no more thread switchery.
 * @param otelContext
 */
@Provider
class WithSpanFilter(otelContext: Context) extends ClientRequestFilter with ClientResponseFilter {
  private var scope: Scope = _

  override def filter(requestContext: ClientRequestContext): Unit =
    scope = otelContext.makeCurrent()

  override def filter(requestContext: ClientRequestContext, responseContext: ClientResponseContext): Unit =
    scope.close()
}
