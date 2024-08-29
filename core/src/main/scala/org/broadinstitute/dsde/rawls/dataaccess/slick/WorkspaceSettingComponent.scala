package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.{
  GcpBucketLifecycleConfigFormat,
  GcpBucketSoftDeleteConfigFormat
}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{GcpBucketLifecycleConfig, GcpBucketSoftDeleteConfig}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.WorkspaceSettingType
import org.broadinstitute.dsde.rawls.model._

import java.sql.Timestamp
import java.util.{Date, UUID}

case class WorkspaceSettingRecord(settingType: String,
                                  workspaceId: UUID,
                                  config: String,
                                  status: String,
                                  createdTime: Timestamp,
                                  lastUpdated: Timestamp,
                                  userId: String
)

object WorkspaceSettingRecord {
  object SettingStatus extends SlickEnum {
    type SettingStatus = Value
    val Pending: Value = Value("Pending")
    val Applied: Value = Value("Applied")
    val Deleted: Value = Value("Deleted")
  }

  def toWorkspaceSettingRecord(workspaceId: UUID,
                               workspaceSettings: WorkspaceSetting,
                               userId: RawlsUserSubjectId
  ): WorkspaceSettingRecord = {
    import spray.json._
    import DefaultJsonProtocol._
    import WorkspaceJsonSupport._

    val currentTime = new Timestamp(new Date().getTime)
    val configString = workspaceSettings.config.toJson.compactPrint
    WorkspaceSettingRecord(
      workspaceSettings.settingType.toString,
      workspaceId,
      configString,
      WorkspaceSettingRecord.SettingStatus.Pending.toString,
      currentTime,
      currentTime,
      userId.value
    )
  }

  def toWorkspaceSetting(workspaceSettingRecord: WorkspaceSettingRecord): WorkspaceSetting = {
    import spray.json._

    val settingType = WorkspaceSettingTypes.withName(workspaceSettingRecord.settingType)
    settingType match {
      case WorkspaceSettingTypes.GcpBucketLifecycle =>
        GcpBucketLifecycleSetting(workspaceSettingRecord.config.parseJson.convertTo[GcpBucketLifecycleConfig])
      case WorkspaceSettingTypes.GcpBucketSoftDelete =>
        GcpBucketSoftDeleteSetting(workspaceSettingRecord.config.parseJson.convertTo[GcpBucketSoftDeleteConfig])
    }
  }
}

trait WorkspaceSettingComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._
  class WorkspaceSettingTable(tag: Tag) extends Table[WorkspaceSettingRecord](tag, "WORKSPACE_SETTINGS") {
    def settingType = column[String]("setting_type", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def config = column[String]("config")
    def status = column[String]("status", O.Length(254))
    def createdTime = column[Timestamp]("created_time")
    def lastUpdated = column[Timestamp]("last_updated")
    def userId = column[String]("user_id", O.Length(254))

    def * = (settingType, workspaceId, config, status, createdTime, lastUpdated, userId) <> (
      WorkspaceSettingRecord.tupled,
      WorkspaceSettingRecord.unapply
    )
  }

  object workspaceSettingQuery extends TableQuery(new WorkspaceSettingTable(_)) {
    def saveAll(workspaceId: UUID,
                workspaceSettings: List[WorkspaceSetting],
                userId: RawlsUserSubjectId
    ): ReadWriteAction[List[WorkspaceSetting]] = {
      val records = workspaceSettings.map(WorkspaceSettingRecord.toWorkspaceSettingRecord(workspaceId, _, userId))
      (workspaceSettingQuery ++= records).map(_ => workspaceSettings)
    }

    def updateSettingStatus(workspaceId: UUID,
                            settingType: WorkspaceSettingType,
                            currentStatus: WorkspaceSettingRecord.SettingStatus.SettingStatus,
                            newStatus: WorkspaceSettingRecord.SettingStatus.SettingStatus
    ): ReadWriteAction[Int] =
      workspaceSettingQuery
        .filter(record =>
          record.workspaceId === workspaceId && record.settingType === settingType.toString && record.status === currentStatus.toString
        )
        .map(rec => (rec.status, rec.lastUpdated))
        .update((newStatus.toString, new Timestamp(new Date().getTime)))

    def deleteSettingTypeForWorkspaceByStatus(workspaceId: UUID,
                                              settingType: WorkspaceSettingType,
                                              status: WorkspaceSettingRecord.SettingStatus.SettingStatus
    ): ReadWriteAction[Int] =
      filter(record =>
        record.workspaceId === workspaceId && record.settingType === settingType.toString && record.status === status.toString
      ).delete

    def listSettingsForWorkspaceByStatus(workspaceId: UUID,
                                         status: WorkspaceSettingRecord.SettingStatus.SettingStatus
    ): ReadAction[List[WorkspaceSetting]] =
      filter(rec => rec.workspaceId === workspaceId && rec.status === status.toString).result
        .map(_.map(WorkspaceSettingRecord.toWorkspaceSetting).toList)
  }
}
