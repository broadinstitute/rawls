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
      Set.empty,
      workspaceId.toString,
      "bucketname",
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

  // bundle to hold methods we'll use across a few tests
  case class PermissionTestMethods(
               getPermission: (RawlsUserSubjectId, SlickWorkspaceContext) => ReadWriteAction[Boolean],
               insertUserPermission: (UUID, Seq[RawlsUserRef]) => ReadWriteAction[_],
               deleteUserPermission: (UUID, Seq[RawlsUserRef]) => ReadWriteAction[Int],
               insertGroupPermission: (UUID, Seq[RawlsGroupRef]) => ReadWriteAction[_]
              )

  // define tests as "testLabel" -> bundle-of-methods
  val canTests = Map(
    "canCompute" -> PermissionTestMethods(
      workspaceQuery.getUserComputePermissions,
      workspaceQuery.insertUserComputePermissions,
      workspaceQuery.deleteUserComputePermissions,
      workspaceQuery.insertGroupComputePermissions
    ),
    "canShare" -> PermissionTestMethods(
      workspaceQuery.getUserSharePermissions,
      workspaceQuery.insertUserSharePermissions,
      workspaceQuery.deleteUserSharePermissions,
      workspaceQuery.insertGroupSharePermissions
    ),
    "catalog" -> PermissionTestMethods(
      workspaceQuery.getUserCatalogPermissions,
      workspaceQuery.insertUserCatalogPermissions,
      workspaceQuery.deleteUserCatalogPermissions,
      workspaceQuery.insertGroupCatalogPermissions
    )
  )

  // execute the tests we just defined
  canTests foreach {
    case (label, methods) =>
      it should s"calculate $label permissions" in withMinimalTestDatabase { _ =>
        val testWs = minimalTestData.workspace
        val otherWs = minimalTestData.workspace2

        val testUser = RawlsUserRef(RawlsUserSubjectId("111222333"))
        val otherUser = RawlsUserRef(RawlsUserSubjectId("98989898"))
        runAndWait(rawlsUserQuery.createUser(RawlsUser(testUser.userSubjectId, RawlsUserEmail(testUser.userSubjectId.value))))
        runAndWait(rawlsUserQuery.createUser(RawlsUser(otherUser.userSubjectId, RawlsUserEmail(otherUser.userSubjectId.value))))

        val testGroup = RawlsGroupName("targetGroup")
        val otherGroup = RawlsGroupName("someOtherGroup")
        runAndWait(rawlsGroupQuery.save(RawlsGroup(testGroup, RawlsGroupEmail(testGroup.value), Set(testUser), Set())))
        runAndWait(rawlsGroupQuery.save(RawlsGroup(otherGroup, RawlsGroupEmail(otherGroup.value), Set(otherUser), Set())))

        assertResult(Set(RawlsGroupRef(testGroup)), "test fixtures not set up correctly for testUser/testGroup") {
          runAndWait(rawlsGroupQuery.listGroupsForUser(testUser))
        }

        assertResult(Set(RawlsGroupRef(otherGroup)), "test fixtures not set up correctly for otherUser/otherGroup") {
          runAndWait(rawlsGroupQuery.listGroupsForUser(otherUser))
        }

        def assertPermission(expected: Boolean, clue: String) =
          assertResult(expected, clue) { runAndWait(methods.getPermission(testUser.userSubjectId, SlickWorkspaceContext(testWs))) }

        // false if no users
        assertPermission(false, "no users")

        // false if another user has permissions but target user does not
        runAndWait(methods.insertUserPermission(UUID.fromString(testWs.workspaceId), Seq(otherUser)))
        assertPermission(false, "other user has permission")

        // false if target user has permissions elsewhere, but not on target workspace
        runAndWait(methods.insertUserPermission(UUID.fromString(otherWs.workspaceId), Seq(testUser)))
        assertPermission(false, "target user has permission elsewhere")

        // true if target user has permission
        runAndWait(methods.insertUserPermission(UUID.fromString(testWs.workspaceId), Seq(testUser)))
        assertPermission(true, "target user has permission")

        // true if target user has permissions, even if user is in a group that has permissions elsewhere
        runAndWait(methods.insertGroupPermission(UUID.fromString(otherWs.workspaceId), Seq(RawlsGroupRef(testGroup))))
        assertPermission(true, "target user has permission and is in a group that has permission elsewhere")

        // false if target user has neither user-table nor group-table permissions, even if target user has group-table permissions elsewhere
        runAndWait(methods.deleteUserPermission(UUID.fromString(testWs.workspaceId), Seq(testUser)))
        assertPermission(false, "target user has group permission elsewhere")

        // false even if some other user has group-table permissions
        runAndWait(methods.insertGroupPermission(UUID.fromString(testWs.workspaceId), Seq(RawlsGroupRef(otherGroup))))
        assertPermission(false, "other user has group permission")

        // true if user has group-table permissions
        runAndWait(methods.insertGroupPermission(UUID.fromString(testWs.workspaceId), Seq(RawlsGroupRef(testGroup))))
        assertPermission(true, "target user has group permission")
      }
  }

}
