package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.WorkspaceFeatureFlag

import java.util.UUID

case class WorkspaceFeatureFlagRecord(workspaceId: UUID, flagName: String)

trait WorkspaceFeatureFlagComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceFeatureFlagTable(tag: Tag) extends Table[WorkspaceFeatureFlagRecord](tag, "WORKSPACE_FEATURE_FLAG") {
    def workspaceId = column[UUID]("workspace_id", O.PrimaryKey)
    def flagName = column[String]("flagname", O.PrimaryKey, O.Length(100))

    def * = (workspaceId, flagName) <> (WorkspaceFeatureFlagRecord.tupled, WorkspaceFeatureFlagRecord.unapply)
  }

  object workspaceFeatureFlagQuery extends TableQuery(new WorkspaceFeatureFlagTable(_)) {

    def save(workspaceId: UUID, flag: WorkspaceFeatureFlag): ReadWriteAction[WorkspaceFeatureFlag] = {
      val flagRecord = WorkspaceFeatureFlagRecord(workspaceId, flag.name)
      (workspaceFeatureFlagQuery += flagRecord).map(_ => flag)
    }

    def saveAll(workspaceId: UUID, flags: List[WorkspaceFeatureFlag]): ReadWriteAction[List[WorkspaceFeatureFlag]] = {
      val flagRecords = flags.map(f => WorkspaceFeatureFlagRecord(workspaceId, f.name))
      (workspaceFeatureFlagQuery ++= flagRecords).map(_ => flags)
    }

    def saveOrUpdate(workspaceId: UUID, flag: WorkspaceFeatureFlag): ReadWriteAction[WorkspaceFeatureFlag] = {
      val flagRecord = WorkspaceFeatureFlagRecord(workspaceId, flag.name)
      workspaceFeatureFlagQuery.insertOrUpdate(flagRecord).map(_ => flag)
    }

    def saveOrUpdateAll(workspaceId: UUID,
                        flags: List[WorkspaceFeatureFlag]
    ): ReadWriteAction[List[WorkspaceFeatureFlag]] = {
      // Slick does not support an insertOrUpdateAll??
      val upsertActions = flags.map { f =>
        workspaceFeatureFlagQuery.insertOrUpdate(WorkspaceFeatureFlagRecord(workspaceId, f.name))
      }
      DBIO.sequence(upsertActions).map(_ => flags)
    }

    def listAllForWorkspace(workspaceId: UUID): ReadAction[Seq[WorkspaceFeatureFlag]] =
      filter(_.workspaceId === workspaceId).result.map { recs =>
        recs.map(rec => WorkspaceFeatureFlag(rec.flagName))
      }

    def listFlagsForWorkspace(workspaceId: UUID,
                              flags: List[WorkspaceFeatureFlag]
    ): ReadAction[Seq[WorkspaceFeatureFlag]] =
      filter(flag => flag.workspaceId === workspaceId && flag.flagName.inSetBind(flags.map(_.name))).result.map {
        recs =>
          recs.map(rec => WorkspaceFeatureFlag(rec.flagName))
      }

    def deleteAllForWorkspace(workspaceId: UUID): ReadWriteAction[Int] =
      filter(_.workspaceId === workspaceId).delete

    def deleteFlagsForWorkspace(workspaceId: UUID, flagnames: List[String]): ReadWriteAction[Int] =
      filter(flag => flag.workspaceId === workspaceId && flag.flagName.inSetBind(flagnames)).delete

  }
}
