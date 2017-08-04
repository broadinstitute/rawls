package org.broadinstitute.dsde.rawls.metrics

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.{HttpResponse, HttpResponseException}
import nl.grons.metrics.scala.{Counter, Histogram, Timer}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented._
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService._

/**
  * Mixin trait for Google instrumentation.
  */
trait GoogleInstrumented extends WorkbenchInstrumented {
  final val GoogleServiceMetricKey = "googleService"

  protected implicit def googleCounters(implicit service: GoogleInstrumentedService): GoogleCounters =
    (request, responseOrException) => {
      val base = ExpandedMetricBuilder
        .expand(GoogleServiceMetricKey, service)
        .expand(HttpRequestMethodMetricKey, request.getRequestMethod.toLowerCase)
        .expand(HttpResponseStatusCodeMetricKey, responseOrException.fold(_.getStatusCode, _.getStatusCode))
      val counter = base.asCounter("request")
      val timer = base.asTimer("latency")
      (counter, timer)
    }

  protected implicit def googleRetryHistogram(implicit service: GoogleInstrumentedService): Histogram =
    ExpandedMetricBuilder
      .expand(GoogleServiceMetricKey, service)
      .asHistogram("retry")
}

object GoogleInstrumented {
  type GoogleCounters = (AbstractGoogleClientRequest[_], Either[HttpResponseException, HttpResponse]) => (Counter, Timer)
}