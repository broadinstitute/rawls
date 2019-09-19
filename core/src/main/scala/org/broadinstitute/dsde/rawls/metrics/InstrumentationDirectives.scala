package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.{Directive0, PathMatcher, PathMatcher0}
import akka.http.scaladsl.server.directives.BasicDirectives.mapResponse
import akka.http.scaladsl.server.directives.BasicDirectives.extractRequest
import akka.http.scaladsl.server.directives.PathDirectives._
import akka.http.scaladsl.server.PathMatchers.Segment

trait InstrumentationDirectives extends RawlsInstrumented {

  // Like Segment in that it matches and consumes any path segment, but does not extract a value.
  private val SegmentIgnore: PathMatcher0 = Segment.tmap(_ => PathMatcher.provide(()))

  private val redactBillingProject =
    (Slash ~ "api").? / "billing" / Segment / "members"

  private val redactBillingProjectRoleEmail =
    (Slash ~ "api").? / "billing" / Segment / Segment / Segment

  private val redactUserGroup =
    (Slash ~ "api").? / "user" / "group" / Segment

  private val redactUserGroupRoleEmail =
    (Slash ~ "api").? / "user" / "groups" / Segment / Segment / Segment

  private val redactGroupAndUser =
    (Slash ~ "api").? / "groups" / Segment / SegmentIgnore / Segment

  private val redactGroups =
    (Slash ~ "api").? / "groups" / Segment

  private val redactWorkflowIds =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / (Segment ~ SegmentIgnore.repeat(0, Int.MaxValue, separator = Slash))

  private val redactSubmissionIds =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "submissions" / (!"validate" ~ Segment ~ SegmentIgnore.repeat(0, Int.MaxValue, separator = Slash))

  private val redactEntityIds =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "entities" / SegmentIgnore / (Segment ~ SegmentIgnore.repeat(0, Int.MaxValue, separator = Slash))

  private val redactMethodConfigs =
    (Slash ~ "api").? / "workspaces" / Segment / Segment / "methodconfigs" / Segment / (Segment ~ SegmentIgnore.repeat(0, Int.MaxValue, separator = Slash))

  private val redactWorkspaceNames =
    (Slash ~ "api").? / "workspaces" / (!"entities" ~ Segment) / (Segment ~ SegmentIgnore.repeat(0, Int.MaxValue, separator = Slash))

  private val redactAdminBilling =
    (Slash ~ "admin").? / "billing" / Segment / SegmentIgnore / Segment

  private val redactNotifications =
    (Slash ~ "api").? / "notifications" / "workspace" / Segment / Segment

  private val redactPapiIds =
    (Slash ~ "api").? / "workflows" / Segment / "genomics" / Segment.repeat(0, Int.MaxValue, separator = Slash)


  // Strip out unique IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    Seq(redactBillingProject, redactBillingProjectRoleEmail, redactUserGroup, redactUserGroupRoleEmail, redactGroupAndUser, redactGroups,
      redactWorkflowIds, redactSubmissionIds, redactEntityIds, redactMethodConfigs, redactWorkspaceNames,
      redactAdminBilling, redactNotifications, redactPapiIds).map(_.asInstanceOf[PathMatcher[Product]])
  )

  private lazy val globalRequestCounter = ExpandedMetricBuilder.empty.asCounter("request")
  private lazy val globalRequestTimer = ExpandedMetricBuilder.empty.asTimer("latency")

  /**
    * Captures elapsed time of request and increments counter. Important note: the route passed into this
    * directive must be sealed otherwise exceptions escape and are not instrumented appropriately.
    */
  def instrumentRequest: Directive0 = extractRequest flatMap { request =>
    val timeStamp = System.currentTimeMillis
    mapResponse { response =>
      val elapsed = System.currentTimeMillis - timeStamp
      globalRequestCounter.inc()
      globalRequestTimer.update(elapsed, TimeUnit.MILLISECONDS)
      httpRequestCounter(ExpandedMetricBuilder.empty)(request, response).inc()
      httpRequestTimer(ExpandedMetricBuilder.empty)(request, response).update(elapsed, TimeUnit.MILLISECONDS)
      response
    }
  }
}
