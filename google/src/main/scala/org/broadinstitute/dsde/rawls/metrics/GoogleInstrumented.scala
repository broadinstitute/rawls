package org.broadinstitute.dsde.rawls.metrics

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.{HttpResponse, HttpResponseException}
import nl.grons.metrics4.scala.{Counter, Histogram, Timer}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented._
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService._

/**
  * Mixin trait for Google instrumentation.
  */
trait GoogleInstrumented extends WorkbenchInstrumented {

  implicit protected def googleCounters(implicit service: GoogleInstrumentedService): GoogleCounters =
    (request, responseOrException) => {
      val base = ExpandedMetricBuilder
        .expand(GoogleServiceMetricKey, service)
        .expand(HttpRequestMethodMetricKey, request.getRequestMethod.toLowerCase)
      val baseWithStatusCode =
        extractStatusCode(responseOrException).map(s => base.expand(HttpResponseStatusCodeMetricKey, s)).getOrElse(base)
      val counter = baseWithStatusCode.asCounter("request")
      val timer = baseWithStatusCode.asTimer("latency")
      (counter, timer)
    }

  implicit protected def googleRetryHistogram(implicit service: GoogleInstrumentedService): Histogram =
    ExpandedMetricBuilder
      .expand(GoogleServiceMetricKey, service)
      .asHistogram("retry")
}

object GoogleInstrumented {
  type GoogleCounters = (AbstractGoogleClientRequest[_], Either[Throwable, HttpResponse]) => (Counter, Timer)

  final val GoogleServiceMetricKey = "googleService"

  private def extractStatusCode(responseOrException: Either[Throwable, HttpResponse]): Option[Int] =
    responseOrException match {
      case Left(t: HttpResponseException) => Some(t.getStatusCode)
      case Left(_)                        => None
      case Right(response)                => Some(response.getStatusCode)
    }
}
