package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.model.Workspace.buildMcWorkspace
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  GoogleProjectId,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceState
}
import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.language.postfixOps

class AggregatedWorkspaceSpec extends AnyFlatSpec {
  private val rawlsWorkspace = Workspace(
    namespace = "test-namespace",
    name = "test-name",
    workspaceId = "aWorkspaceId",
    bucketName = "aBucket",
    workflowCollectionName = Some("workflow-collection"),
    createdDate = new DateTime(),
    lastModified = new DateTime(),
    createdBy = "test",
    attributes = Map.empty
  )

  private val mcWorkspace = Workspace.buildReadyMcWorkspace(
    namespace = "fake",
    name = "fakews",
    workspaceId = UUID.randomUUID.toString,
    createdDate = DateTime.now(),
    lastModified = DateTime.now(),
    createdBy = "fake",
    attributes = Map.empty
  )

  behavior of "getCloudPlatform"

  it should "return GCP for a rawls workspace" in {
    val ws =
      AggregatedWorkspace(rawlsWorkspace, googleProjectId = None, azureCloudContext = None, policies = List.empty)

    val cp = ws.getCloudPlatform

    cp shouldBe WorkspaceCloudPlatform.Gcp
  }

  it should "return Azure for an Azure MC workspace" in {
    val ws = AggregatedWorkspace(mcWorkspace,
                                 googleProjectId = None,
                                 Some(AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake")),
                                 policies = List.empty
    )

    val cp = ws.getCloudPlatform

    cp shouldBe WorkspaceCloudPlatform.Azure
  }

  it should "return Gcp for a Gcp MC workspace" in {
    val ws = AggregatedWorkspace(mcWorkspace,
                                 Some(GoogleProjectId("project-id")),
                                 azureCloudContext = None,
                                 policies = List.empty
    )

    val cp = ws.getCloudPlatform
    cp shouldBe WorkspaceCloudPlatform.Gcp
  }

  it should "raise for an MC workspace that has cloud info for multiple clouds" in {
    val ws = AggregatedWorkspace(
      mcWorkspace,
      Some(GoogleProjectId("project-id")),
      Some(AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake")),
      policies = List.empty
    )

    val thrown = intercept[InvalidCloudContextException] {
      ws.getCloudPlatform
    }
    thrown.getMessage should include("expected exactly one set of cloud metadata for workspace")
  }

  it should "raise for an MC workspace that has no cloud info" in {
    val ws = AggregatedWorkspace(mcWorkspace, googleProjectId = None, azureCloudContext = None, policies = List.empty)

    val thrown = intercept[InvalidCloudContextException] {
      ws.getCloudPlatform
    }
    thrown.getMessage should include("expected exactly one set of cloud metadata for workspace")
  }

  private val deletingWs = buildMcWorkspace(
    "namespace",
    "name",
    workspaceId = UUID.randomUUID.toString,
    createdDate = DateTime.now(),
    lastModified = DateTime.now(),
    createdBy = "fake",
    attributes = Map.empty,
    WorkspaceState.Deleting
  )

  behavior of "getCloudPlatformHandlingNonReady"

  it should "return GCP for a rawls workspace" in {
    val ws =
      AggregatedWorkspace(rawlsWorkspace, googleProjectId = None, azureCloudContext = None, policies = List.empty)

    val cp = ws.getCloudPlatformHandlingNonReady

    cp shouldBe Some(WorkspaceCloudPlatform.Gcp)
  }

  it should "return Azure for an Azure MC workspace" in {
    val ws = AggregatedWorkspace(mcWorkspace,
                                 googleProjectId = None,
                                 Some(AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake")),
                                 policies = List.empty
    )

    val cp = ws.getCloudPlatformHandlingNonReady

    cp shouldBe Some(WorkspaceCloudPlatform.Azure)
  }

  it should "return Gcp for a Gcp MC workspace" in {
    val ws = AggregatedWorkspace(mcWorkspace,
                                 Some(GoogleProjectId("project-id")),
                                 azureCloudContext = None,
                                 policies = List.empty
    )

    val cp = ws.getCloudPlatformHandlingNonReady
    cp shouldBe Some(WorkspaceCloudPlatform.Gcp)
  }

  it should "raise for an MC workspace that has cloud info for multiple clouds" in {
    val ws = AggregatedWorkspace(
      deletingWs,
      Some(GoogleProjectId("project-id")),
      Some(AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake")),
      policies = List.empty
    )

    val thrown = intercept[InvalidCloudContextException] {
      ws.getCloudPlatformHandlingNonReady
    }
    thrown.getMessage should include("expected exactly one set of cloud metadata for workspace")
  }

  it should "raise for a ready MC workspace that has no cloud info" in {
    val ws = AggregatedWorkspace(mcWorkspace, googleProjectId = None, azureCloudContext = None, policies = List.empty)

    val thrown = intercept[InvalidCloudContextException] {
      ws.getCloudPlatformHandlingNonReady
    }
    thrown.getMessage should include("no cloud metadata for ready workspace")
  }

  it should "return None for a non-ready MC workspace that has no cloud info" in {
    val ws = AggregatedWorkspace(deletingWs, googleProjectId = None, azureCloudContext = None, policies = List.empty)

    val cp = ws.getCloudPlatformHandlingNonReady
    cp shouldBe None
  }
}
