package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeNumber,
  AttributeString,
  GoogleProjectId,
  Workspace,
  WorkspaceType,
  WorkspaceVersions
}

import java.util.UUID

class EntityTypeStatisticsComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with EntityTypeStatisticsComponent
    with WorkspaceComponent
    with RawlsTestUtils {

  "EntityTypeStatisticsComponent" should "create and get cached entity type statistics" in withEmptyTestDatabase {
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
      WorkspaceType.RawlsWorkspace
    )

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val testStats = Map(
      "sample" -> 1,
      "participant" -> 1709009
    )

    assertResult(2) {
      runAndWait(entityTypeStatisticsQuery.batchInsert(workspaceId, testStats))
    }

    assertResult(testStats) {
      runAndWait(entityTypeStatisticsQuery.getAll(workspaceId))
    }
  }

  it should "delete cached entity type statistics" in withEmptyTestDatabase {
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
      WorkspaceType.RawlsWorkspace
    )

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val testStats = Map(
      "sample" -> 1,
      "participant" -> 1709009
    )

    assertResult(2) {
      runAndWait(entityTypeStatisticsQuery.batchInsert(workspaceId, testStats))
    }

    assertResult(2) {
      runAndWait(entityTypeStatisticsQuery.deleteAllForWorkspace(workspaceId))
    }

    assertResult(Map.empty) {
      runAndWait(entityTypeStatisticsQuery.getAll(workspaceId))
    }
  }

}
