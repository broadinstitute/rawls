package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobStatus
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.RawlsBillingProjectName
import slick.lifted.ProvenShape

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object WorkspaceManagerResourceMonitorRecord {
  object JobType extends SlickEnum {
    type JobType = Value
    val AzureLandingZoneResult: Value = Value("AzureLandingZoneResult")
  }

  implicit sealed class JobStatus(val complete: Boolean)

  case object Complete extends JobStatus(true)

  case object Incomplete extends JobStatus(false)
}

trait WorkspaceManagerResourceJobRunner {
  val jobType: JobType
  // Returns true if this runner is finished with the job
  def run(job: WorkspaceManagerResourceMonitorRecord)(implicit executionContext: ExecutionContext): Future[JobStatus]
}

final case class WorkspaceManagerResourceMonitorRecord(
  jobControlId: UUID,
  jobType: JobType,
  workspaceId: Option[UUID],
  billingProjectId: Option[String],
  userEmail: Option[String],
  createdTime: Timestamp
)

trait WorkspaceManagerResourceMonitorRecordComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceManagerResourceMonitorRecordTable(tag: Tag)
      extends Table[WorkspaceManagerResourceMonitorRecord](tag, "WORKSPACE_MANAGER_RESOURCE_MONITOR_RECORD") {
    def jobControlId: Rep[UUID] = column[UUID]("JOB_CONTROL_ID", O.PrimaryKey)

    def jobType: Rep[JobType] = column[JobType]("JOB_TYPE")

    def workspaceId: Rep[Option[UUID]] = column[Option[UUID]]("WORKSPACE_ID")

    def billingProjectId: Rep[Option[String]] = column[Option[String]]("BILLING_PROJECT_ID")

    def userEmail: Rep[Option[String]] = column[Option[String]]("USER_EMAIL")

    def createdTime: Rep[Timestamp] = column[Timestamp]("CREATED_TIME")

    override def * : ProvenShape[WorkspaceManagerResourceMonitorRecord] = (
      jobControlId,
      jobType,
      workspaceId,
      billingProjectId,
      userEmail,
      createdTime
    ) <> ((WorkspaceManagerResourceMonitorRecord.apply _).tupled, WorkspaceManagerResourceMonitorRecord.unapply)
  }

  object WorkspaceManagerResourceMonitorRecordQuery
      extends TableQuery(new WorkspaceManagerResourceMonitorRecordTable(_)) {

    val query = TableQuery[WorkspaceManagerResourceMonitorRecordTable]

    def create(job: WorkspaceManagerResourceMonitorRecord): WriteAction[Unit] = (query += job).map(_ => ())

    def delete(job: WorkspaceManagerResourceMonitorRecord): ReadWriteAction[Boolean] =
      query.filter(_.jobControlId === job.jobControlId).delete.map(_ > 0)

    def selectByBillingProject(name: RawlsBillingProjectName): ReadAction[Seq[WorkspaceManagerResourceMonitorRecord]] =
      query.filter(_.billingProjectId === name.value).result

    def getRecords: ReadAction[Seq[WorkspaceManagerResourceMonitorRecord]] = query.result

  }

}
