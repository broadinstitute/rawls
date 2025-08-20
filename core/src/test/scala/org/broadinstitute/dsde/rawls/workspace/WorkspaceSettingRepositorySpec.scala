package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, WorkspaceSettingRecord}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{
  GcpBucketLifecycleAction,
  GcpBucketLifecycleCondition,
  GcpBucketLifecycleConfig,
  GcpBucketLifecycleRule,
  GcpBucketRequesterPaysConfig,
  GcpBucketSoftDeleteConfig,
  GcpLogBucketRetentionConfig,
  SeparateSubmissionFinalOutputsConfig
}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.{CompactDataTables, GcpBucketSoftDelete}
import org.broadinstitute.dsde.rawls.model.{
  GcpBucketLifecycleSetting,
  GcpBucketRequesterPaysSetting,
  GcpBucketSoftDeleteSetting,
  GcpLogBucketRetentionSetting,
  SeparateSubmissionFinalOutputsSetting,
  Workspace,
  WorkspaceSettingTypes
}
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
    val appliedSetting = GcpBucketLifecycleSetting(
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("applied")), Some(30))
          )
        )
      )
    )
    val pendingSetting = GcpBucketLifecycleSetting(
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

  // Helps ensure that WorkspaceSettingRecord.toWorkspaceSetting in WorkspaceSettingComponent.scala
  // is able to successfully create every type of workspace setting from a corresponding record
  for {
    workspaceSetting <- List(
      GcpBucketLifecycleSetting(GcpBucketLifecycleConfig(List())),
      GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(0)),
      GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(true)),
      GcpLogBucketRetentionSetting(GcpLogBucketRetentionConfig(60)),
      SeparateSubmissionFinalOutputsSetting(SeparateSubmissionFinalOutputsConfig(true))
    )
  }
    it should s"be able to get a ${workspaceSetting.getClass.getSimpleName}" in {
      val repo = new WorkspaceSettingRepository(slickDataSource)
      val workspaceRepo = new WorkspaceRepository(slickDataSource)
      val ws: Workspace = makeWorkspace()
      Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)

      Await.result(
        slickDataSource.inTransaction { dataAccess =>
          for {
            _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                          List(workspaceSetting),
                                                          userInfo.userSubjectId
            )
            _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
              ws.workspaceIdAsUUID,
              workspaceSetting.settingType,
              WorkspaceSettingRecord.SettingStatus.Pending,
              WorkspaceSettingRecord.SettingStatus.Applied
            )
          } yield ()
        },
        Duration.Inf
      )

      val result = Await.result(repo.getWorkspaceSettings(ws.workspaceIdAsUUID), Duration.Inf)

      assertResult(result)(List(workspaceSetting))
    }

  behavior of "getWorkspacesSettingsOfType"

  it should "return applied soft delete setting when soft delete type is requested" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val appliedSoftDeleteSetting = GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(0))
    val appliedRequesterPaysSetting = GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(true))
    val pendingSetting = GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(2_000_000))

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                        List(appliedSoftDeleteSetting, appliedRequesterPaysSetting),
                                                        userInfo.userSubjectId
          )
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketSoftDelete,
            WorkspaceSettingRecord.SettingStatus.Pending,
            WorkspaceSettingRecord.SettingStatus.Applied
          )
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketRequesterPays,
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

    val result = Await.result(repo.getWorkspaceSettingOfType(ws.workspaceIdAsUUID, GcpBucketSoftDelete), Duration.Inf)

    assertResult(result)(Some(appliedSoftDeleteSetting))
  }

  it should "return none if no setting of requested type is applied" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val appliedRequesterPaysSetting = GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(true))
    val pendingSetting = GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(2_000_000))

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                        List(appliedRequesterPaysSetting),
                                                        userInfo.userSubjectId
          )
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            WorkspaceSettingTypes.GcpBucketRequesterPays,
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

    val result = Await.result(repo.getWorkspaceSettingOfType(ws.workspaceIdAsUUID, GcpBucketSoftDelete), Duration.Inf)

    assertResult(result)(None)
  }

  behavior of "hasPendingSettings"

  it should "return true if there are pending settings" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val pendingSetting = GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(123))

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID, List(pendingSetting), userInfo.userSubjectId)
      },
      Duration.Inf
    )

    val result = Await.result(repo.hasPendingSettings(ws.workspaceIdAsUUID, GcpBucketSoftDelete), Duration.Inf)
    result shouldBe true
  }

  it should "return false if there are no pending settings" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val appliedSetting = GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(456))

    Await.result(
      slickDataSource.inTransaction { dataAccess =>
        for {
          _ <- dataAccess.workspaceSettingQuery.saveAll(ws.workspaceIdAsUUID,
                                                        List(appliedSetting),
                                                        userInfo.userSubjectId
          )
          _ <- dataAccess.workspaceSettingQuery.updateSettingStatus(
            ws.workspaceIdAsUUID,
            GcpBucketSoftDelete,
            WorkspaceSettingRecord.SettingStatus.Pending,
            WorkspaceSettingRecord.SettingStatus.Applied
          )
        } yield ()
      },
      Duration.Inf
    )

    val result = Await.result(repo.hasPendingSettings(ws.workspaceIdAsUUID, GcpBucketSoftDelete), Duration.Inf)
    result shouldBe false
  }

  behavior of "createWorkspaceSettingsRecords"

  it should "create pending workspace settings" in {
    val repo = new WorkspaceSettingRepository(slickDataSource)
    val workspaceRepo = new WorkspaceRepository(slickDataSource)
    val ws: Workspace = makeWorkspace()
    Await.result(workspaceRepo.createWorkspace(ws), Duration.Inf)
    val setting = GcpBucketLifecycleSetting(
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
    val setting = GcpBucketLifecycleSetting(
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
    val existingSetting = GcpBucketLifecycleSetting(
      GcpBucketLifecycleConfig(
        List(
          GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"),
                                 GcpBucketLifecycleCondition(Some(Set("applied")), Some(30))
          )
        )
      )
    )
    val newSetting = GcpBucketLifecycleSetting(
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
    val setting = GcpBucketLifecycleSetting(
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
