package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService

/**
 * Created by dvoet on 2/8/16.
 */
class WorkspaceComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with WorkspaceComponent with RawlsTestUtils {
  import driver.api._

  "WorkspaceComponent" should "crud workspaces" in withEmptyTestDatabase {
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
        AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)),
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
        AttributeName("library", "attributeBool") -> AttributeBoolean(false))
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

    // no submissions: 0 successful, 0 failed, 0 running
    val wsIdNoSubmissions: UUID = testData.workspaceNoSubmissions
    assertResult(Map(wsIdNoSubmissions -> WorkspaceSubmissionStats(None, None, 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdNoSubmissions)))
    }

    // successful submission: 1 successful, 0 failed, 0 running
    val wsIdSuccessfulSubmission: UUID = testData.workspaceSuccessfulSubmission
    assertResult(Map(wsIdSuccessfulSubmission -> WorkspaceSubmissionStats(Some(testDate), None, 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdSuccessfulSubmission)))
    }

    // failed submission: 0 successful, 1 failed, 0 running
    val wsIdFailedSubmission: UUID = testData.workspaceFailedSubmission
    assertResult(Map(wsIdFailedSubmission -> WorkspaceSubmissionStats(None, Some(testDate), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdFailedSubmission)))
    }

    // submitted submissions: 0 successful, 0 failed, 1 running
    val wsIdSubmittedSubmission: UUID = testData.workspaceSubmittedSubmission
    assertResult(Map(wsIdSubmittedSubmission -> WorkspaceSubmissionStats(None, None, 1))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdSubmittedSubmission)))
    }

    // mixed submissions: 0 successful, 1 failed, 1 running
    val wsIdMixedSubmission: UUID = testData.workspaceMixedSubmissions
    assertResult(Map(wsIdMixedSubmission -> WorkspaceSubmissionStats(None, Some(testDate), 1))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdMixedSubmission)))
    }

    // terminated submissions: 1 successful, 1 failed, 0 running
    val wsIdTerminatedSubmission: UUID = testData.workspaceTerminatedSubmissions
    assertResult(Map(wsIdTerminatedSubmission -> WorkspaceSubmissionStats(Some(testDate), Some(testDate), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdTerminatedSubmission)))
    }

    // interleaved submissions: 1 successful, 1 failed, 0 running
    val wsIdInterleavedSubmissions: UUID = testData.workspaceInterleavedSubmissions
    assertResult(Map(wsIdInterleavedSubmissions -> WorkspaceSubmissionStats(Option(testData.t4), Option(testData.t3), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdInterleavedSubmissions)))
    }
  }
}
