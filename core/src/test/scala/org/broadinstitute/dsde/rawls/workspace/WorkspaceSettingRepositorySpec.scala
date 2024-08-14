package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceSettingRecord}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{
  GcpBucketLifecycleAction,
  GcpBucketLifecycleCondition,
  GcpBucketLifecycleConfig,
  GcpBucketLifecycleRule
}
import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceSetting, WorkspaceSettingTypes}
import org.joda.time.DateTime
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WorkspaceSettingRepositorySpec
    extends AnyFlatSpec
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with TestDriverComponent {

  def makeWorkspace(): Workspace = Workspace.buildReadyMcWorkspace("fake-ns",
                                                                   s"test-${UUID.randomUUID().toString}",
                                                                   UUID.randomUUID().toString,
                                                                   DateTime.now(),
                                                                   DateTime.now(),
                                                                   "fake@example.com",
                                                                   Map.empty
  )

  behavior of "getWorkspaceSettings"

  it should "return the applied workspace settings" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val appliedSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("applied")), Some(30))
          )
        )
      )
    )
    val pendingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("pending")), Some(31))
          )
        )
      )
    )

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                        List(appliedSetting),
                                                        userInfo.userSubjectId
          )
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketLifecycle,
            WorkspaceSettingRecord.SettingStatus.Pending,
            WorkspaceSettingRecord.SettingStatus.Applied
          )
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                        List(pendingSetting),
                                                        userInfo.userSubjectId
          )
        } yield ()
      },
      Duration.Inf
    )

    val result = Await.result(repo.getWorkspaceSettings(ws.workspaceIdAsUUID), Duration.Inf)

    assertResult(result)(List(appliedSetting))
  }

  behavior of "createWorkspaceSettingsRecords"

  it should "create pending workspace settings" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val setting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("newSetting")), Some(30))
          )
        )
      )
    )

    Await.result(repo.createWorkspaceSettingsRecords(ws.workspaceIdAsUUID, List(setting), userInfo.userSubjectId),
                 Duration.Inf
    )

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
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val setting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("newSetting")), Some(30))
          )
        )
      )
    )

    Await.result(slickDataSource.inTransaction(
                   _.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(setting), userInfo.userSubjectId)
                 ),
                 Duration.Inf
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(repo.createWorkspaceSettingsRecords(ws.workspaceIdAsUUID, List(setting), userInfo.userSubjectId),
                   Duration.Inf
      )
    }
    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  behavior of "markWorkspaceSettingApplied"

  it should "mark Pending settings as Applied and Applied settings as Deleted" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val existingSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("applied")), Some(30))
          )
        )
      )
    )
    val newSetting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("pending")), Some(31))
          )
        )
      )
    )

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                        List(existingSetting),
                                                        userInfo.userSubjectId
          )
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketLifecycle,
            WorkspaceSettingRecord.SettingStatus.Pending,
            WorkspaceSettingRecord.SettingStatus.Applied
          )
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(newSetting), userInfo.userSubjectId)
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
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val setting = WorkspaceSetting(
      WorkspaceSettingTypes.GcpBucketLifecycle,
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("newSetting")), Some(30))
          )
        )
      )
    )

    Await.result(slickDataSource.inTransaction(
                   _.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(setting), userInfo.userSubjectId)
                 ),
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
