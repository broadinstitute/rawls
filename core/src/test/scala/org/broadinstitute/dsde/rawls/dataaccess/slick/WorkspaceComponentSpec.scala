package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model._

/**
 * Created by dvoet on 2/8/16.
 */
class WorkspaceComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with WorkspaceComponent with RawlsTestUtils {
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
        AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
        AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)),
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

    assertWorkspaceResult(workspace) {
      runAndWait(workspaceQuery.save(workspace))
    }

    assertWorkspaceResult(Option(workspace)) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertWorkspaceResult(Seq(workspace)) {
      runAndWait(workspaceQuery.listByIds(Seq(workspaceId)))
    }

    assertWorkspaceResult(Option(workspace)) {
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

    assertWorkspaceResult(updatedWorkspace) {
      runAndWait(workspaceQuery.save(updatedWorkspace))
    }

    assertWorkspaceResult(Option(updatedWorkspace)) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertWorkspaceResult(Option(updatedWorkspace)) {
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

  it should "list submission summary stats" in withDefaultTestDatabase {
    implicit def toWorkspaceId(ws: Workspace): UUID = UUID.fromString(ws.workspaceId)

    val wsIdNoSubmissions: UUID = testData.workspaceNoSubmissions
    assertResult(Map(wsIdNoSubmissions -> WorkspaceSubmissionStats(None, None, 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdNoSubmissions)))
    }

    val wsIdSuccessfulSubmission: UUID = testData.workspaceSuccessfulSubmission
    assertResult(Map(wsIdSuccessfulSubmission -> WorkspaceSubmissionStats(Some(testDate), None, 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdSuccessfulSubmission)))
    }

    val wsIdFailedSubmission: UUID = testData.workspaceFailedSubmission
    assertResult(Map(wsIdFailedSubmission -> WorkspaceSubmissionStats(None, Some(testDate), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdFailedSubmission)))
    }

    val wsIdSubmittedSubmission: UUID = testData.workspaceSubmittedSubmission
    assertResult(Map(wsIdSubmittedSubmission -> WorkspaceSubmissionStats(None, None, 1))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdSubmittedSubmission)))
    }

    // Note: a submission with both a successful and failed workflow is a failure
    val wsIdMixedSubmission: UUID = testData.workspaceMixedSubmissions
    assertResult(Map(wsIdMixedSubmission -> WorkspaceSubmissionStats(None, Some(testDate), 1))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdMixedSubmission)))
    }

    val wsIdTerminatedSubmission: UUID = testData.workspaceTerminatedSubmissions
    assertResult(Map(wsIdTerminatedSubmission -> WorkspaceSubmissionStats(Some(testDate), Some(testDate), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdTerminatedSubmission)))
    }
  }
}
