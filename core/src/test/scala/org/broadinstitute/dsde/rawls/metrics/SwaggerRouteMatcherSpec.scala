package org.broadinstitute.dsde.rawls.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class SwaggerRouteMatcherSpec extends AnyFlatSpec with Matchers {

  "SwaggerRouteMatcher.matchRoute" should "match getWorkspaceById before listWorkspaceDetails" in {
    val workspaceId = UUID.randomUUID().toString
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/id/$workspaceId")
    matchedRoute shouldBe defined
    matchedRoute.get.route shouldBe "/api/workspaces/id/{workspaceId}"
    matchedRoute.get.parametersByName("workspaceId") shouldBe workspaceId
  }

  it should "match listWorkspaceDetails" in {
    val namespace = "this is a namespace"
    val name = "this is a name"
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/$namespace/$name")
    matchedRoute shouldBe defined
    matchedRoute.get.route shouldBe "/api/workspaces/{workspaceNamespace}/{workspaceName}"
    matchedRoute.get.parametersByName("workspaceNamespace") shouldBe namespace
    matchedRoute.get.parametersByName("workspaceName") shouldBe name
  }

  it should "match getBucketUsage" in {
    val namespace = "this is a namespace"
    val name = "this is a name"
    val matchedRoute = SwaggerRouteMatcher.matchRoute(s"/api/workspaces/$namespace/$name/bucketUsage")
    matchedRoute shouldBe defined
    matchedRoute.get.route shouldBe "/api/workspaces/{workspaceNamespace}/{workspaceName}/bucketUsage"
    matchedRoute.get.parametersByName("workspaceNamespace") shouldBe namespace
    matchedRoute.get.parametersByName("workspaceName") shouldBe name
  }

  it should "match getWorkflowCost" in {
    val namespace = "this is a namespace"
    val name = "this is a name"
    val submissionId = UUID.randomUUID().toString
    val workflowId = UUID.randomUUID().toString
    val matchedRoute = SwaggerRouteMatcher.matchRoute(
      s"/api/workspaces/$namespace/$name/submissions/$submissionId/workflows/$workflowId/cost"
    )
    matchedRoute shouldBe defined
    matchedRoute.get.route shouldBe "/api/workspaces/{workspaceNamespace}/{workspaceName}/submissions/{submissionId}/workflows/{workflowId}/cost"
    matchedRoute.get.parametersByName("workspaceNamespace") shouldBe namespace
    matchedRoute.get.parametersByName("workspaceName") shouldBe name
    matchedRoute.get.parametersByName("submissionId") shouldBe submissionId
    matchedRoute.get.parametersByName("workflowId") shouldBe workflowId
  }

  it should "match with no parameters" in {
    val matchedRoute = SwaggerRouteMatcher.matchRoute("/api/workspaces")
    matchedRoute shouldBe defined
    matchedRoute.get.route shouldBe "/api/workspaces"
    matchedRoute.get.parametersByName shouldBe empty
  }

  it should "return None when no match is found" in {
    val matchedRoute = SwaggerRouteMatcher.matchRoute("/api/this/is/not/a/valid/path")
    matchedRoute shouldBe empty
  }
}
