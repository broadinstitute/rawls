package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceSettingRecord}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{
  GcpBucketLifecycleConfig,
  GcpBucketLifecycleRule,
  GcpBucketLifecycleAction,
  GcpBucketLifecycleCondition
}
import org.broadinstitute.dsde.rawls.model.{
  Workspace,
  WorkspaceName,
  WorkspaceSetting,
  WorkspaceSettingTypes,
  WorkspaceState
}
import org.joda.time.DateTime
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WorkspaceRepositorySpec
    extends AnyFlatSpec
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with TestDriverComponent {

  behavior of "createMCWorkspace"

  it should "throw an exception if a workspace with the same name already exists" in {
    val workspaceId = UUID.randomUUID()
    val namespace = "fake"
    val name = s"fake-name-${workspaceId.toString}"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceRepository = new WorkspaceRepository(slickDataSource)

    Await.result(workspaceRepository.createMCWorkspace(workspaceId, workspaceName, Map(), testContext), Duration.Inf)

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceRepository.createMCWorkspace(UUID.randomUUID(), workspaceName, Map(), testContext),
                   Duration.Inf
      )
    }
    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
    Await
      .result(slickDataSource.inTransaction(_.workspaceQuery.findByName(WorkspaceName(namespace, name))), Duration.Inf)
      .get
      .name shouldBe name
  }

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

  behavior of "setFailedState"

  it should "Update the workspace state and error message" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)

    Await.result(repo.setFailedState(ws.workspaceIdAsUUID, WorkspaceState.DeleteFailed, "reason workspace failed"),
                 Duration.Inf
    )

    val readback = Await.result(repo.getWorkspace(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(readback.get.state)(WorkspaceState.DeleteFailed)
    assertResult(readback.get.errorMessage)(Some("reason workspace failed"))
  }

  behavior of "deleteWorkspaceRecord"

  it should "delete the workspace" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)

    val result = Await.result(repo.deleteWorkspace(ws), Duration.Inf)
    val readback = Await.result(repo.getWorkspace(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(readback)(None)
    assertResult(result)(true)
  }

  behavior of "getWorkspaceSettings"

  it should "return the applied workspace settings" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)
    val appliedSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("applied"), Some(30)))
        )
      )
    )
    val pendingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("pending"), Some(31)))
        )
      )
    )

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(appliedSetting))
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketLifecycle,
            WorkspaceSettingRecord.SettingStatus.Pending,
            WorkspaceSettingRecord.SettingStatus.Applied
          )
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(pendingSetting))
        } yield ()
      },
      Duration.Inf
    )

    val result = Await.result(repo.getWorkspaceSettings(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(result)(List(appliedSetting))
  }

  behavior of "createWorkspaceSettingsRecords"

  it should "create pending workspace settings" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)
    val setting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("newSetting"), Some(30)))
        )
      )
    )

    Await.result(repo.createWorkspaceSettingsRecords(ws.workspaceIdAsUUID, List(setting)), Duration.Inf)

    val newSettings = Await.result(
      slickDataSource.inTransaction(
        _.workspaceSettingQuery.listSettingsForWorkspaceByStatus(ws.workspaceIdAsUUID,
                                                                 WorkspaceSettingRecord.SettingStatus.Pending
        )
      ),
      Duration.Inf
    )
    assertResult(newSettings)(List(setting))
  }

  it should "throw an exception if there are already pending settings" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)
    val setting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("newSetting"), Some(30)))
        )
      )
    )

    Await.result(slickDataSource.inTransaction(_.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(setting))),
                 Duration.Inf
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(repo.createWorkspaceSettingsRecords(ws.workspaceIdAsUUID, List(setting)), Duration.Inf)
    }
    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  behavior of "markWorkspaceSettingApplied"

  it should "mark Pending settings as Applied and Applied settings as Deleted" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("applied"), Some(30)))
        )
      )
    )
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("pending"), Some(31)))
        )
      )
    )

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(existingSetting))
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketLifecycle,
            WorkspaceSettingRecord.SettingStatus.Pending,
            WorkspaceSettingRecord.SettingStatus.Applied
          )
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(newSetting))
        } yield ()
      },
      Duration.Inf
    )

    Await.result(repo.markWorkspaceSettingApplied(ws.workspaceIdAsUUID, WorkspaceSettingTypes.GcpBucketLifecycle),
                 Duration.Inf
    )

    // existing settings should now be deleted
    val deletedSettings = Await.result(
      slickDataSource.inTransaction(
        _.workspaceSettingQuery.listSettingsForWorkspaceByStatus(ws.workspaceIdAsUUID,
                                                                 WorkspaceSettingRecord.SettingStatus.Deleted
        )
      ),
      Duration.Inf
    )
    assertResult(deletedSettings)(List(existingSetting))

    // new settings should now be applied
    val appliedSettings = Await.result(
      slickDataSource.inTransaction(
        _.workspaceSettingQuery.listSettingsForWorkspaceByStatus(ws.workspaceIdAsUUID,
                                                                 WorkspaceSettingRecord.SettingStatus.Applied
        )
      ),
      Duration.Inf
    )
    assertResult(appliedSettings)(List(newSetting))
  }

  behavior of "removePendingSetting"

  it should "delete the pending workspace setting" in {
    val repo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(repo.createWorkspace(ws), Duration.Inf)
    val setting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("newSetting"), Some(30)))
        )
      )
    )

    Await.result(slickDataSource.inTransaction(_.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(setting))),
                 Duration.Inf
    )

    Await.result(repo.removePendingSetting(ws.workspaceIdAsUUID, WorkspaceSettingTypes.GcpBucketLifecycle),
                 Duration.Inf
    )

    // There should be no workspace settings in the database. Failed pending settings are not kept.
    Await.result(
      slickDataSource.inTransaction(
        _.workspaceSettingQuery.listSettingsForWorkspaceByStatus(ws.workspaceIdAsUUID,
                                                                 WorkspaceSettingRecord.SettingStatus.Deleted
        )
      ),
      Duration.Inf
    ) shouldBe empty
    Await.result(
      slickDataSource.inTransaction(
        _.workspaceSettingQuery.listSettingsForWorkspaceByStatus(ws.workspaceIdAsUUID,
                                                                 WorkspaceSettingRecord.SettingStatus.Applied
        )
      ),
      Duration.Inf
    ) shouldBe empty
    Await.result(
      slickDataSource.inTransaction(
        _.workspaceSettingQuery.listSettingsForWorkspaceByStatus(ws.workspaceIdAsUUID,
                                                                 WorkspaceSettingRecord.SettingStatus.Pending
        )
      ),
      Duration.Inf
    ) shouldBe empty
  }
}
