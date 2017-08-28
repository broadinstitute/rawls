package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import shapeless._
import spray.http.Uri
import spray.routing._
import spray.routing.directives.BasicDirectives.mapHttpResponse
import spray.routing.directives.MiscDirectives.requestInstance
import spray.routing.directives.PathDirectives._

trait InstrumentationDirectives extends RawlsInstrumented {

  // Like Segment in that it matches and consumes any path segment, but does not extract a value.
  private val SegmentIgnore: PathMatcher0 = Segment.hmap(_ => HNil)

  private val redactBillingProject =
    (Slash ~ "api").? / "billing" / Segment / "members"

  private val redactBillingProjectRoleEmail =
    (Slash ~ "api").? / "billing" / Segment / Segment / Segment

  private val redactUserGroup =
    (Slash ~ "api").? / "user" / "group" / Segment

  private val redactUserGroupRoleEmail =
    (Slash ~ "api").? / "user" / "groups" / Segment / Segment / Segment

  private val redactWorkflowIds =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / (Segment ~ SegmentIgnore.repeat(separator = Slash))

  private val redactSubmissionIds =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "submissions" / (!"validate" ~ Segment ~ SegmentIgnore.repeat(separator = Slash))

  private val redactEntityIds =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "entities" / SegmentIgnore / (Segment ~ SegmentIgnore.repeat(separator = Slash))

  private val redactMethodConfigs =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "methodconfigs" / Segment / (Segment ~ SegmentIgnore.repeat(separator = Slash))

  private val redactWorkspaceNames =
    (Slash ~ "api").? / "workspaces" / (!"entities" ~ Segment) / (Segment ~ SegmentIgnore.repeat(separator = Slash))

  // Strip out unique IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    redactBillingProject | redactBillingProjectRoleEmail | redactUserGroup | redactUserGroupRoleEmail | redactWorkflowIds
      | redactSubmissionIds | redactEntityIds | redactMethodConfigs | redactWorkspaceNames
  )

  def instrumentRequest: Directive0 = requestInstance flatMap { request =>
    val timeStamp = System.currentTimeMillis
    mapHttpResponse { response =>
      httpRequestCounter(ExpandedMetricBuilder.empty)(request, response).inc()
      httpRequestTimer(ExpandedMetricBuilder.empty)(request, response).update(System.currentTimeMillis - timeStamp, TimeUnit.MILLISECONDS)
      response
    }
  }
}
