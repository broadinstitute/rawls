package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceState}
import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WorkspaceRepositorySpec extends AnyFlatSpec with TestDriverComponent {

  behavior of "getWorkspace"
  def makeWorkspace(): Workspace = Workspace.buildReadyMcWorkspace("fake-ns",
                                                      s"test-${UUID.randomUUID().toString}",
                                                      UUID.randomUUID().toString,
                                                      DateTime.now(),
                                                      DateTime.now(),
                                                      "fake@example.com",
                                                      Map.empty
  )

  it should "get the workspace if present" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)

    val result = Await.result(repo.getWorkspace(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(ws.workspaceId)(result.get.workspaceId)
  }

  it should "return none if the workspace is not present" in {
    val repo = new WorkspaceRepository(slickDataSource)

    val result = Await.result(repo.getWorkspace(UUID.randomUUID()), Duration.Inf)

    assertResult(None)(result)
  }

  behavior of "updateState"

  it should "Update the workspace state" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)

    Await.result(repo.updateState(ws.workspaceIdAsUUID, WorkspaceState.Deleting), Duration.Inf)
    val readback = Await.result(repo.getWorkspace(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(readback.get.state)(WorkspaceState.Deleting)
  }

  behavior of "deleteWorkspaceRecord"

  it should "delete the workspace" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)

    val result = Await.result(repo.deleteWorkspaceRecord(ws), Duration.Inf)
    val readback = Await.result(repo.getWorkspace(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(readback)(None)
    assertResult(result)(true)
  }
}
