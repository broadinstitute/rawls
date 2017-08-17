package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import spray.http.Uri
import spray.routing.Directive0
import spray.routing.directives.BasicDirectives.mapHttpResponse
import spray.routing.directives.MiscDirectives.requestInstance

trait InstrumentationDirectives extends RawlsInstrumented {
  // Strip out workflow IDs and entity names from metrics by providing a redactedUriExpansion
  private val WorkflowIdRegex = """^.*workflows\.([^.]*).*$""".r
  private val EntityIdRegex = """^.*entities\.[^.]*\.([^.]*).*$""".r
  override protected val UriExpansion: Expansion[Uri] = Expansion.redactedUriExpansion(WorkflowIdRegex, EntityIdRegex)

  def instrumentRequest: Directive0 = requestInstance flatMap { request =>
    val timeStamp = System.currentTimeMillis
    mapHttpResponse { response =>
      httpRequestCounter(ExpandedMetricBuilder.empty)(request, response).inc()
      httpRequestTimer(ExpandedMetricBuilder.empty)(request, response).update(System.currentTimeMillis - timeStamp, TimeUnit.MILLISECONDS)
      response
    }
  }
}
