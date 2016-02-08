package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by dvoet on 2/8/16.
 */
class WorkspaceComponentSpec extends TestDriverComponent with WorkspaceComponent {
  import driver.api._

  private def insertTestGroups: Unit = {
    runAndWait(saveRawlsGroup(RawlsGroupRecord("reader", "reader@foo.com")))
    runAndWait(saveRawlsGroup(RawlsGroupRecord("writer", "writer@foo.com")))
    runAndWait(saveRawlsGroup(RawlsGroupRecord("owner", "owner@foo.com")))
    runAndWait(saveRawlsGroup(RawlsGroupRecord("reader2", "reader2@foo.com")))
    runAndWait(saveRawlsGroup(RawlsGroupRecord("writer2", "writer2@foo.com")))
    runAndWait(saveRawlsGroup(RawlsGroupRecord("owner2", "owner2@foo.com")))
  }

  "WorkspaceComponent" should "crud workspaces" in {
    insertTestGroups

    val workspaceId: UUID = UUID.randomUUID()

    val workspace: Workspace = Workspace(
      "test_namespace",
      "test_name",
      workspaceId.toString,
      "bucketname",
      DateTime.now(),
      DateTime.now(),
      "me",
      Map("attributeString" -> AttributeString("value"),
        "attributeBool" -> AttributeBoolean(true),
        "attributeNum" -> AttributeNumber(3.14159)),
      Map(WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName("reader")),
        WorkspaceAccessLevels.Write -> RawlsGroupRef(RawlsGroupName("writer")),
        WorkspaceAccessLevels.Owner -> RawlsGroupRef(RawlsGroupName("owner"))),
      false)

    assertResult(None) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertResult(workspace) {
      runAndWait(workspaceQuery.save(workspace))
    }

    assertResult(Option(workspace)) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertResult(Option(workspace)) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    val updatedWorkspace = workspace.copy(
      attributes = Map("attributeString" -> AttributeString("value2"),
      "attributeBool" -> AttributeBoolean(false)),
      accessLevels = Map(WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName("reader2")),
        WorkspaceAccessLevels.Write -> RawlsGroupRef(RawlsGroupName("writer2")),
        WorkspaceAccessLevels.Owner -> RawlsGroupRef(RawlsGroupName("owner2")))
    )

    assertResult(updatedWorkspace) {
      runAndWait(workspaceQuery.save(updatedWorkspace))
    }

    assertResult(Option(updatedWorkspace)) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertResult(Option(updatedWorkspace)) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertResult(true) {
      runAndWait(workspaceQuery.delete(workspace.toWorkspaceName))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertResult(false) {
      runAndWait(workspaceQuery.delete(workspace.toWorkspaceName))
    }
  }
}
