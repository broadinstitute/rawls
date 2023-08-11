package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeNumber,
  AttributeString,
  GoogleProjectId,
  Workspace,
  WorkspaceState,
  WorkspaceType,
  WorkspaceVersions
}

import java.util.UUID

class EntityAttributeStatisticsSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with EntityAttributeStatisticsComponent
    with WorkspaceComponent
    with RawlsTestUtils {

  "EntityAttributeStatisticsComponent" should "create and get cached attribute statistics" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()

    val workspace: Workspace = Workspace(
      "test_namespace",
      "test_name",
      workspaceId.toString,
      "bucketname",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "me",
      Map(
        AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
        AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)
      ),
      false,
      WorkspaceVersions.V2,
      GoogleProjectId("test_google_project"),
      None,
      None,
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
    )

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val testStats = Map(
      "sample" -> Seq(AttributeName("namespace1", "value1"), AttributeName("namespace1", "value2")),
      "participant" -> Seq(AttributeName("namespace2", "value1"), AttributeName("namespace3", "value1"))
    )

    assertResult(2) {
      runAndWait(entityAttributeStatisticsQuery.batchInsert(workspaceId, testStats))
    }

    assertResult(testStats) {
      runAndWait(entityAttributeStatisticsQuery.getAll(workspaceId))
    }

  }

  it should "delete cached attribute statistics" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()

    val workspace: Workspace = Workspace(
      "test_namespace",
      "test_name",
      workspaceId.toString,
      "bucketname",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "me",
      Map(
        AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
        AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)
      ),
      false,
      WorkspaceVersions.V2,
      GoogleProjectId("test_google_project"),
      None,
      None,
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
    )

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val testStats = Map(
      "sample" -> Seq(AttributeName("namespace1", "value1"), AttributeName("namespace1", "value2")),
      "participant" -> Seq(AttributeName("namespace2", "value1"), AttributeName("namespace3", "value1"))
    )

    assertResult(2) {
      runAndWait(entityAttributeStatisticsQuery.batchInsert(workspaceId, testStats))
    }

    assertResult(4) {
      runAndWait(entityAttributeStatisticsQuery.deleteAllForWorkspace(workspaceId))
    }

    assertResult(Map.empty) {
      runAndWait(entityAttributeStatisticsQuery.getAll(workspaceId))
    }
  }

}
