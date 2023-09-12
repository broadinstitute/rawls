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
    isLocked = false,
    WorkspaceVersions.V2,
    GoogleProjectId("google_project_id"),
    None,
    None,
    None,
    Option(currentTime()),
    WorkspaceType.RawlsWorkspace,
    WorkspaceState.Ready
  )

  val userEmail: RawlsUserEmail = RawlsUserEmail("foo@bar.com")
  val saEmail1: BondServiceAccountEmail = BondServiceAccountEmail("sa1@bar.com")
  val saEmail2: BondServiceAccountEmail = BondServiceAccountEmail("sa2@bar.com")
  val saEmail3: BondServiceAccountEmail = BondServiceAccountEmail("sa3@bar.com")

  "deleteAllForWorkspace" should "delete all records for the workspace" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace))
    val user2Email: RawlsUserEmail = RawlsUserEmail("another-user@bar.com")

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail)
    ) shouldBe empty

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName,
                                                   userEmail,
                                                   Set(saEmail1, saEmail2, saEmail3)
      )
    ) shouldBe 3

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName,
                                                   user2Email,
                                                   Set(saEmail1, saEmail2, saEmail3)
      )
    ) shouldBe 3

    runAndWait(
      workspaceRequesterPaysQuery.listAllForWorkspace(workspace.toWorkspaceName)
    ).values.flatten.size shouldBe 6

    runAndWait(workspaceRequesterPaysQuery.deleteAllForWorkspace(workspace.workspaceIdAsUUID)) shouldBe 6

    runAndWait(
      workspaceRequesterPaysQuery.listAllForWorkspace(workspace.toWorkspaceName)
    ) shouldBe empty

  }

  "deleteAllForUser" should "delete all records for a given user" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail)
    ) shouldBe empty

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName,
                                                   userEmail,
                                                   Set(saEmail1, saEmail2, saEmail3)
      )
    ) shouldBe 3

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail)
    ) should not be empty

    runAndWait(workspaceRequesterPaysQuery.deleteAllForUser(workspace.toWorkspaceName, userEmail)) shouldBe 3

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail)
    ) shouldBe empty

  }

  "insertAllForUser" should "not insert duplicate records" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    runAndWait(
      workspaceRequesterPaysQuery.listAllForUser(workspace.toWorkspaceName, userEmail)
    ) shouldBe empty

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail2))
    ) shouldBe 2

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail2))
    ) shouldBe 0
  }

  it should "add new records for newly added users" in withEmptyTestDatabase {
    runAndWait(workspaceQuery.createOrUpdate(workspace))

    runAndWait(
      workspaceRequesterPaysQuery.listAllForWorkspace(workspace.toWorkspaceName)
    ) shouldBe empty

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail2))
    ) shouldBe 2

    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(workspace.toWorkspaceName, userEmail, Set(saEmail1, saEmail3))
    ) shouldBe 1

    val records = runAndWait(
      workspaceRequesterPaysQuery.listAllForWorkspace(workspace.toWorkspaceName)
    )
    records.values.head.size shouldBe 3
  }

}
