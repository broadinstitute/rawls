package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import shapeless.HNil
import spray.http.Uri
import spray.routing._
import spray.routing.directives.BasicDirectives.mapHttpResponse
import spray.routing.directives.MiscDirectives.requestInstance
import spray.routing.directives.PathDirectives._

trait InstrumentationDirectives extends RawlsInstrumented {

  // Like Segment in that it matches and consumes any path segment, but does not extract a value.
  private val SegmentIgnore: PathMatcher0 = Segment.hmap(_ => HNil)

  private val redactWorkflowIds: PathMatcher1[String] =
    (Slash ~ "workspaces") / SegmentIgnore / SegmentIgnore / "submissions" / SegmentIgnore / "workflows" / (Segment ~ SegmentIgnore.repeat(separator = Slash))

  private val redactEntityIds: PathMatcher1[String] =
    (Slash ~ "workspaces") / SegmentIgnore / SegmentIgnore / "entities" / SegmentIgnore / (Segment ~ SegmentIgnore.repeat(separator = Slash))

  // Strip out workflow IDs and entity names from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(redactWorkflowIds | redactEntityIds)

  def instrumentRequest: Directive0 = requestInstance flatMap { request =>
    val timeStamp = System.currentTimeMillis
    mapHttpResponse { response =>
      httpRequestCounter(ExpandedMetricBuilder.empty)(request, response).inc()
      httpRequestTimer(ExpandedMetricBuilder.empty)(request, response).update(System.currentTimeMillis - timeStamp, TimeUnit.MILLISECONDS)
      response
    }
  }
}
