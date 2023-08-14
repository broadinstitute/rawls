package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.BondServiceAccountEmail
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  RawlsUserEmail,
  Workspace,
  WorkspaceState,
  WorkspaceType,
  WorkspaceVersions
}

import java.util.UUID

class WorkspaceRequesterPaysComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  "WorkspaceRequesterPaysComponentSpec" should "crud" in withEmptyTestDatabase {
    val workspaceId: UUID = UUID.randomUUID()

    val workspace: Workspace = Workspace(
      "test_namespace",
      "test_name",
      workspaceId.toString,
      "bucketname",
      None,
      currentTime(),
      currentTime(),
      "me",
      Map.empty,
      false,
      WorkspaceVersions.V1,
      GoogleProjectId("google_project_id"),
      None,
      None,
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
    )

    runAndWait(workspaceQuery.createOrUpdate(workspace))

    val userEmail = RawlsUserEmail("foo@bar.com")
    val saEmail1 = BondServiceAccountEmail("sa1@bar.com")
    val saEmail2 = BondServiceAccountEmail("sa2@bar.com")
    val saEmail3 = BondServiceAccountEmail("sa3@bar.com")

    runAndWait(
      workspaceRequesterPaysQuery.userExistsInWorkspaceNamespaceAssociatedGoogleProject(workspace.namespace, userEmail)
    ) shouldBe false

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail2))
    ) shouldBe 2
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail2))
    ) shouldBe 0
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail3))
    ) shouldBe 1

    runAndWait(
      workspaceRequesterPaysQuery.userExistsInWorkspaceNamespaceAssociatedGoogleProject(workspace.namespace, userEmail)
    ) shouldBe true

    runAndWait(workspaceRequesterPaysQuery.deleteAllForUser(workspace.toWorkspaceName, userEmail)) shouldBe 3

    runAndWait(
      workspaceRequesterPaysQuery.userExistsInWorkspaceNamespaceAssociatedGoogleProject(workspace.namespace, userEmail)
    ) shouldBe false

  }
}
