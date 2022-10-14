package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.lifted.ProvenShape
import org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

object WorkspaceManagerResourceJobType extends Enumeration {
  type WorkspaceManagerResourceJobType = Value
  val AzureLandingZoneResult: slick.WorkspaceManagerResourceJobType.Value = Value("AzureLandingZoneResult")
}

final case class WorkspaceManagerResourceMonitorRecord(
  jobControlId: UUID,
  jobType: String,
  workspaceId: Option[UUID],
  billingProjectId: Option[String],
  createdTime: Timestamp
)

trait WorkspaceManagerResourceMonitorRecordComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceManagerResourceMonitorRecordTable(tag: Tag)
      extends Table[WorkspaceManagerResourceMonitorRecord](tag, "WORKSPACE_MANAGER_RESOURCE_MONITOR_RECORD") {
    def jobControlId: Rep[UUID] = column[UUID]("JOB_CONTROL_ID", O.PrimaryKey)

    def jobType: Rep[String] = column[String]("JOB_TYPE")

    def workspaceId: Rep[Option[UUID]] = column[Option[UUID]]("WORKSPACE_ID")

    def billingProjectId: Rep[Option[String]] = column[Option[String]]("BILLING_PROJECT_ID")

    def createdTime: Rep[Timestamp] = column[Timestamp]("CREATED_TIME")

    override def * : ProvenShape[WorkspaceManagerResourceMonitorRecord] = (
      jobControlId,
      jobType,
      workspaceId,
      billingProjectId,
      createdTime
    ) <> ((WorkspaceManagerResourceMonitorRecord.apply _).tupled, WorkspaceManagerResourceMonitorRecord.unapply)
  }

  object WorkspaceManagerResourceMonitorRecordQuery
      extends TableQuery(new WorkspaceManagerResourceMonitorRecordTable(_)) {

    val query = TableQuery[WorkspaceManagerResourceMonitorRecordTable]

    def create(job: WorkspaceManagerResourceMonitorRecord): WriteAction[Unit] = (query += job).map(_ => ())

    def delete(job: WorkspaceManagerResourceMonitorRecord): ReadWriteAction[Boolean] =
      query.filter(_.jobControlId === job.jobControlId).delete.map(_ > 0)

    def getRecords: ReadAction[Seq[WorkspaceManagerResourceMonitorRecord]] = query.result
  }

}
