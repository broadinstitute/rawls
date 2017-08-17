package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import spray.http.{HttpRequest, HttpResponse, Uri}
import spray.routing.Directive0
import spray.routing.directives.BasicDirectives.mapHttpResponse
import spray.routing.directives.MiscDirectives.requestInstance

trait InstrumentationDirectives extends RawlsInstrumented {
  // Strip out workflow IDs from metrics by providing an implicit redactedUriExpansion
  private val WorkflowIdRegex = """^.*workflows\.(.*)\..*$""".r
  implicit val UriExpansion: Expansion[Uri] = Expansion.redactedUriExpansion(WorkflowIdRegex)

  override def httpRequestMetricBuilder(builder: ExpandedMetricBuilder): (HttpRequest, HttpResponse) => ExpandedMetricBuilder = {
    (httpRequest, httpResponse) => builder
      .expand(HttpRequestMethodMetricKey, httpRequest.method)
      .expand(HttpRequestUriMetricKey, httpRequest.uri)
      .expand(HttpResponseStatusCodeMetricKey, httpResponse.status)
  }

  def instrumentRequest: Directive0 = requestInstance flatMap { request =>
    val timeStamp = System.currentTimeMillis
    mapHttpResponse { response =>
      httpRequestCounter(ExpandedMetricBuilder.empty)(request, response).inc()
      httpRequestTimer(ExpandedMetricBuilder.empty)(request, response).update(System.currentTimeMillis - timeStamp, TimeUnit.MILLISECONDS)
      response
    }
  }
}
