package org.broadinstitute.dsde.rawls.metrics

import spray.routing.Directive0
import spray.routing.directives.BasicDirectives.mapHttpResponse
import spray.routing.directives.MiscDirectives.requestInstance

trait InstrumentationDirectives extends RawlsInstrumented {
  def instrumentRequest: Directive0 = requestInstance flatMap { request =>
    val timeStamp = System.currentTimeMillis
    mapHttpResponse { response =>
      httpRequestCounter(ExpandedMetricBuilder.empty)(request, response).inc()
      httpRequestTimer(ExpandedMetricBuilder.empty)(request, response).time(System.currentTimeMillis - timeStamp)
      response
    }
  }
}
