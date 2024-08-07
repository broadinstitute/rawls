package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.GcpBucketLifecycleConfigFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.GcpBucketLifecycleConfig
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.WorkspaceSettingType
import org.broadinstitute.dsde.rawls.model._

import java.sql.Timestamp
import java.util.{Date, UUID}

case class WorkspaceSettingRecord(`type`: String,
                                  workspaceId: UUID,
                                  config: String,
                                  status: String,
                                  createdTime: Timestamp,
                                  lastUpdated: Timestamp
)

object WorkspaceSettingRecord {
  object SettingStatus extends SlickEnum {
    type SettingStatus = Value
    val Pending: Value = Value("Pending")
    val Applied: Value = Value("Applied")
    val Deleted: Value = Value("Deleted")
  }

  def toWorkspaceSettingRecord(workspaceId: UUID, workspaceSettings: WorkspaceSetting): WorkspaceSettingRecord = {
    import spray.json._
    import DefaultJsonProtocol._
    import WorkspaceJsonSupport._

    val currentTime = new Timestamp(new Date().getTime)
    val configString = workspaceSettings.config.toJson.compactPrint
    WorkspaceSettingRecord(workspaceSettings.`type`.toString,
                           workspaceId,
                           configString,
                           WorkspaceSettingRecord.SettingStatus.Pending.toString,
                           currentTime,
                           currentTime
    )
  }

  def toWorkspaceSettings(workspaceSettingRecord: WorkspaceSettingRecord): WorkspaceSetting = {
    import spray.json._

    val settingType = WorkspaceSettingTypes.withName(workspaceSettingRecord.`type`)
    val settingConfig = settingType match {
      case WorkspaceSettingTypes.GcpBucketLifecycle => workspaceSettingRecord.config.parseJson.convertTo[GcpBucketLifecycleConfig]
    }

    WorkspaceSetting(settingType, settingConfig)
  }
}

trait WorkspaceSettingComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._
  class WorkspaceSettingTable(tag: Tag) extends Table[WorkspaceSettingRecord](tag, "WORKSPACE_SETTINGS") {
    def `type` = column[String]("type", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def config = column[String]("config")
    def status = column[String]("status", O.Length(254))
    def createdTime = column[Timestamp]("created_time")
    def lastUpdated = column[Timestamp]("last_updated")

    def * = (`type`, workspaceId, config, status, createdTime, lastUpdated) <> (WorkspaceSettingRecord.tupled,
                                                                                WorkspaceSettingRecord.unapply
    )
  }

  object workspaceSettingQuery extends TableQuery(new WorkspaceSettingTable(_)) {
    def saveAll(workspaceId: UUID,
                workspaceSettings: List[WorkspaceSetting]
    ): ReadWriteAction[List[WorkspaceSetting]] = {
      val records = workspaceSettings.map(WorkspaceSettingRecord.toWorkspaceSettingRecord(workspaceId, _))
      (workspaceSettingQuery ++= records).map(_ => workspaceSettings)
    }

    def updateStatuses(workspaceId: UUID,
                       workspaceSettingTypes: List[WorkspaceSettingType],
                       currentStatus: WorkspaceSettingRecord.SettingStatus.SettingStatus,
                       newStatus: WorkspaceSettingRecord.SettingStatus.SettingStatus
    ): ReadWriteAction[Int] =
      workspaceSettingQuery
        .filter(record =>
          record.workspaceId === workspaceId && record.`type`.inSetBind(workspaceSettingTypes.map(_.toString)) && record.status === currentStatus.toString
        )
        .map(_.status)
        .update(newStatus.toString)

    def deleteSettingTypeForWorkspaceByStatus(workspaceId: UUID,
                                              settingType: WorkspaceSettingType,
                                              status: WorkspaceSettingRecord.SettingStatus.SettingStatus
    ): ReadWriteAction[Int] =
      filter(record =>
        record.workspaceId === workspaceId && record.`type` === settingType.toString && record.status === status.toString
      ).map(_.status).update(WorkspaceSettingRecord.SettingStatus.Deleted.toString)

    def listSettingsForWorkspaceByStatus(workspaceId: UUID,
                                         status: WorkspaceSettingRecord.SettingStatus.SettingStatus
    ): ReadAction[List[WorkspaceSetting]] =
      filter(rec => rec.workspaceId === workspaceId && rec.status === status.toString).result
        .map(_.map(WorkspaceSettingRecord.toWorkspaceSettings).toList)
  }
}
