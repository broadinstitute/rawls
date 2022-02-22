package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

case class WorkspaceFeatureFlagRecord(workspaceId: UUID,
                                      flagName: String,
                                      enabled: Boolean,
                                      config: Option[String])

trait WorkspaceFeatureFlagComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceFeatureFlagTable(tag: Tag) extends Table[WorkspaceFeatureFlagRecord](tag, "WORKSPACE_FEATURE_FLAG") {
    def workspaceId = column[UUID]("workspace_id", O.PrimaryKey)
    def flagName = column[String]("flagname", O.PrimaryKey, O.Length(100))
    def enabled = column[Boolean]("enabled")
    def config = column[Option[String]]("config", O.Length(4000))

    def * = (workspaceId, flagName, enabled, config) <> (WorkspaceFeatureFlagRecord.tupled, WorkspaceFeatureFlagRecord.unapply)
  }

  object workspaceFeatureFlagQuery extends TableQuery(new WorkspaceFeatureFlagTable(_)) {

    def save(workspaceId: UUID, flagname: String, enabled: Boolean, config: Option[String] = None): ReadWriteAction[WorkspaceFeatureFlagRecord] = {
      val flagRecord = WorkspaceFeatureFlagRecord(workspaceId, flagname, enabled, config)
      (workspaceFeatureFlagQuery += flagRecord).map (_ => flagRecord)
    }

    def saveAll(flags: List[WorkspaceFeatureFlagRecord]): ReadWriteAction[List[WorkspaceFeatureFlagRecord]] = {
      (workspaceFeatureFlagQuery ++= flags).map (_ => flags)
    }

    def saveOrUpdate(workspaceId: UUID, flagname: String, enabled: Boolean, config: Option[String] = None): ReadWriteAction[WorkspaceFeatureFlagRecord] = {
      val flagRecord = WorkspaceFeatureFlagRecord(workspaceId, flagname, enabled, config)
      workspaceFeatureFlagQuery.insertOrUpdate(flagRecord).map (_ => flagRecord)
    }

    def saveOrUpdateAll(flags: List[WorkspaceFeatureFlagRecord]): ReadWriteAction[List[WorkspaceFeatureFlagRecord]] = {
      // Slick does not support an insertOrUpdateAll??
      val upsertActions = flags.map { flagRecord =>
        workspaceFeatureFlagQuery.insertOrUpdate(flagRecord)
      }
      DBIO.sequence(upsertActions).map (_ => flags)
    }

    def listAllForWorkspace(workspaceId: UUID): ReadAction[Seq[WorkspaceFeatureFlagRecord]] = {
      filter(_.workspaceId === workspaceId).result
    }

    def listFlagsForWorkspace(workspaceId: UUID, flagnames: List[String]): ReadAction[Seq[WorkspaceFeatureFlagRecord]] = {
      filter(flag => flag.workspaceId === workspaceId && flag.flagName.inSetBind(flagnames)).result
    }

    def deleteAllForWorkspace(workspaceId: UUID): ReadWriteAction[Int] = {
      filter(_.workspaceId === workspaceId).delete
    }

    def deleteFlagsForWorkspace(workspaceId: UUID, flagnames: List[String]): ReadWriteAction[Int] = {
      filter(flag => flag.workspaceId === workspaceId && flag.flagName.inSetBind(flagnames)).delete
    }

  }
}