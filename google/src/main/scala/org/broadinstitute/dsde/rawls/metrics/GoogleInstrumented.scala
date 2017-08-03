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

  protected implicit def googleCounters[A: GoogleInstrumentedServiceMapper]: GoogleCounters[A] =
    (request, responseOrException) => {
      val base = ExpandedMetricBuilder
        .expand(GoogleServiceMetricKey, implicitly[GoogleInstrumentedServiceMapper[A]].service)
        .expand(HttpRequestMethodMetricKey, request.getRequestMethod.toLowerCase)
        .expand(HttpResponseStatusCodeMetricKey, responseOrException.fold(_.getStatusCode, _.getStatusCode))
      val counter = base.asCounter("request")
      val timer = base.asTimer("latency")
      (counter, timer)
    }

  protected implicit def googleRetryHistogram[A: GoogleInstrumentedServiceMapper]: Histogram =
    ExpandedMetricBuilder
      .expand(GoogleServiceMetricKey, implicitly[GoogleInstrumentedServiceMapper[A]].service)
      .asHistogram("retry")

  protected implicit def serviceMapperFromService[A](implicit service: GoogleInstrumentedService): GoogleInstrumentedServiceMapper[A] =
    GoogleInstrumentedServiceMapper[A](service)
}

object GoogleInstrumented {
  type GoogleCounters[A] = (AbstractGoogleClientRequest[A], Either[HttpResponseException, HttpResponse]) => (Counter, Timer)
}