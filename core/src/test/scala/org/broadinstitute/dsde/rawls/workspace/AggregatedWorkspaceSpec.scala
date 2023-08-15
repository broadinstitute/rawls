package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, Workspace, WorkspaceCloudPlatform}
import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec

import scala.language.postfixOps
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID

class AggregatedWorkspaceSpec extends AnyFlatSpec {
  private val gcpWorkspace = Workspace(
    "test-namespace",
    "test-name",
    "aWorkspaceId",
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  private val azWorkspace = Workspace.buildReadyMcWorkspace(
    "fake",
    "fakews",
    UUID.randomUUID.toString,
    DateTime.now(),
    DateTime.now(),
    "fake",
    Map.empty
  )

  behavior of "getCloudPlatform"

  it should "return GCP for rawls workspaces" in {
    val ws = AggregatedWorkspace(gcpWorkspace, None, List.empty)

    val cp = ws.getCloudPlatform

    cp shouldBe WorkspaceCloudPlatform.Gcp
  }

  it should "return Azure for Azure workspaces" in {
    val ws = AggregatedWorkspace(azWorkspace,
                                 Some(AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake")),
                                 List.empty
    )

    val cp = ws.getCloudPlatform

    cp shouldBe WorkspaceCloudPlatform.Azure
  }

  it should "raise for MC workspaces that do not have an azure cloud context" in {
    val ws = AggregatedWorkspace(azWorkspace, None, List.empty)

    intercept[InvalidCloudContextException] {
      ws.getCloudPlatform
    }
  }
}
