package org.broadinstitute.dsde.rawls.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class SwaggerRouteMatcherSpec extends AnyFlatSpec with Matchers {

  "SwaggerRouteMatcher.matchRoute" should "match getWorkspaceById before listWorkspaceDetails" in {
    val workspaceId = UUID.randomUUID().toString
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/id/$workspaceId")
    matchedRoute shouldBe defined
    matchedRoute.get.path shouldBe "/api/workspaces/id/{workspaceId}"
    matchedRoute.get.parameters("workspaceId") shouldBe workspaceId
  }

  it should "match listWorkspaceDetails" in {
    val namespace = "this is a namespace"
    val name = "this is a name"
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/$namespace/$name")
    matchedRoute shouldBe defined
    matchedRoute.get.path shouldBe "/api/workspaces/{workspaceNamespace}/{workspaceName}"
    matchedRoute.get.parameters("workspaceNamespace") shouldBe namespace
    matchedRoute.get.parameters("workspaceName") shouldBe name
  }

  it should "match getBucketUsage" in {
    val namespace = "this is a namespace"
    val name = "this is a name"
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/$namespace/$name/bucketUsage")
    matchedRoute shouldBe defined
    matchedRoute.get.path shouldBe "/api/workspaces/{workspaceNamespace}/{workspaceName}/bucketUsage"
    matchedRoute.get.parameters("workspaceNamespace") shouldBe namespace
    matchedRoute.get.parameters("workspaceName") shouldBe name
  }

  it should "match getWorkflowCost" in {
    val namespace = "this is a namespace"
    val name = "this is a name"
    val submissionId = UUID.randomUUID().toString
    val workflowId = UUID.randomUUID().toString
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/$namespace/$name/submissions/$submissionId/workflows/$workflowId/cost")
    matchedRoute shouldBe defined
    matchedRoute.get.path shouldBe "/api/workspaces/{workspaceNamespace}/{workspaceName}/submissions/{submissionId}/workflows/{workflowId}/cost"
    matchedRoute.get.parameters("workspaceNamespace") shouldBe namespace
    matchedRoute.get.parameters("workspaceName") shouldBe name
    matchedRoute.get.parameters("submissionId") shouldBe submissionId
    matchedRoute.get.parameters("workflowId") shouldBe workflowId
  }

  it should "match with no parameters" in {
    val matchedRoute = SwaggerRouteMatcher.matchRoute("/api/workspaces")
    matchedRoute shouldBe defined
    matchedRoute.get.path shouldBe "/api/workspaces"
    matchedRoute.get.parameters shouldBe empty
  }

  it should "return None when no match is found" in {
    val matchedRoute = SwaggerRouteMatcher.matchRoute("/api/this/is/not/a/valid/path")
    matchedRoute shouldBe empty
  }
}
