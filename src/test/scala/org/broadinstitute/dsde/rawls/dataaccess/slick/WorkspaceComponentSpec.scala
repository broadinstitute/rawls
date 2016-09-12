package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by dvoet on 2/8/16.
 */
class WorkspaceComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with WorkspaceComponent {
  import driver.api._

  private def saveRawlsGroup(name: String, email: String) = {
    runAndWait(rawlsGroupQuery.save(RawlsGroup(RawlsGroupName(name), RawlsGroupEmail(email), Set.empty, Set.empty)))
  }

  private def insertTestGroups: Unit = {
    saveRawlsGroup("reader", "reader@foo.com")
    saveRawlsGroup("writer", "writer@foo.com")
    saveRawlsGroup("owner", "owner@foo.com")
    saveRawlsGroup("reader2", "reader2@foo.com")
    saveRawlsGroup("writer2", "writer2@foo.com")
    saveRawlsGroup("owner2", "owner2@foo.com")
  }

  "WorkspaceComponent" should "crud workspaces" in withEmptyTestDatabase {
    insertTestGroups

    val workspaceId: UUID = UUID.randomUUID()

    val workspace: Workspace = Workspace(
      "test_namespace",
      "test_name",
      None,
      workspaceId.toString,
      "bucketname",
      currentTime(),
      currentTime(),
      "me",
      Map(
        defaultAttributeName("attributeString") -> AttributeString("value"),
        defaultAttributeName("attributeBool") -> AttributeBoolean(true),
        defaultAttributeName("attributeNum") -> AttributeNumber(3.14159)),
      Map(
        WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName("reader")),
        WorkspaceAccessLevels.Write -> RawlsGroupRef(RawlsGroupName("writer")),
        WorkspaceAccessLevels.Owner -> RawlsGroupRef(RawlsGroupName("owner"))),
      Map(
        WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName("reader")),
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

    assertResult(Seq(workspace)) {
      runAndWait(workspaceQuery.listByIds(Seq(workspaceId)))
    }

    assertResult(Option(workspace)) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    val updatedWorkspace = workspace.copy(
      attributes = Map(
        AttributeName("default", "attributeString") -> AttributeString("value2"),
        AttributeName("library", "attributeBool") -> AttributeBoolean(false)),
      accessLevels = Map(
        WorkspaceAccessLevels.Read -> RawlsGroupRef(RawlsGroupName("reader2")),
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
