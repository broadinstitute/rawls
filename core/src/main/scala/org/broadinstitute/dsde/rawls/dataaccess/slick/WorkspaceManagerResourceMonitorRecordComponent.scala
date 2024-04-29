package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobStatus
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.{
  cloneJobTypes,
  JobType
}
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsUserEmail}
import slick.lifted.ProvenShape
import spray.json._
import DefaultJsonProtocol._
import slick.ast.TypedType

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object WorkspaceManagerResourceMonitorRecord {
  object JobType extends SlickEnum {
    type JobType = Value
    val AzureLandingZoneResult: Value = Value("AzureLandingZoneResult")

    val GoogleBillingProjectDelete: Value = Value("GoogleBillingProjectDelete")
    val BpmBillingProjectDelete: Value = Value("AzureBillingProjectDelete")

    val WorkspaceDeleteInit: Value = Value("WorkspaceDeleteInit")
    val LeoAppDeletionPoll: Value = Value("LeoAppDeletionPoll")
    val LeoRuntimeDeletionPoll: Value = Value("LeoRuntimeDeletionPoll")
    val WSMWorkspaceDeletionPoll: Value = Value("WSMWorkspaceDeletionPoll")

    val deleteJobTypes: List[WorkspaceManagerResourceMonitorRecord.JobType.Value] = List(
      JobType.WorkspaceDeleteInit,
      JobType.LeoRuntimeDeletionPoll,
      JobType.LeoAppDeletionPoll,
      JobType.WSMWorkspaceDeletionPoll
    )

    val CloneWorkspaceInit: Value = Value("CloneWorkspaceInit")
    val CreateWdsAppInClonedWorkspace: Value = Value("CreateWdsAppInClonedWorkspace")
    val CloneWorkspaceContainerInit: Value = Value("CloneWorkspaceContainerInit")
    val CloneWorkspaceContainerResult: Value = Value("CloneWorkspaceContainerResult")
    val cloneJobTypes: List[WorkspaceManagerResourceMonitorRecord.JobType.Value] = List(
      JobType.CloneWorkspaceInit,
      JobType.CreateWdsAppInClonedWorkspace,
      JobType.CloneWorkspaceContainerInit,
      JobType.CloneWorkspaceContainerResult
    )
  }

  implicit sealed class JobStatus(val isDone: Boolean)

  case object Complete extends JobStatus(true)

  case object Incomplete extends JobStatus(false)

  def forAzureLandingZoneCreate(jobRecordId: UUID,
                                billingProjectName: RawlsBillingProjectName,
                                userEmail: RawlsUserEmail
  ): WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord(
      jobRecordId,
      JobType.AzureLandingZoneResult,
      workspaceId = None,
      Some(billingProjectName.value),
      Some(userEmail.value),
      Timestamp.from(Instant.now())
    )

  def forBillingProjectDelete(
    jobRecordId: UUID,
    billingProjectName: RawlsBillingProjectName,
    userEmail: RawlsUserEmail,
    jobType: JobType // one of: GoogleBillingProjectDelete, AzureBillingProjectDelete, or OtherBpmBillingProjectDelete
  ): WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
    jobRecordId,
    jobType,
    workspaceId = None,
    Some(billingProjectName.value),
    Some(userEmail.value),
    Timestamp.from(Instant.now())
  )

  def forCloneWorkspaceContainer(jobRecordId: UUID,
                                 workspaceId: UUID,
                                 userEmail: RawlsUserEmail
  ): WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord(
      jobRecordId,
      JobType.CloneWorkspaceContainerResult,
      workspaceId = Some(workspaceId),
      billingProjectId = None,
      userEmail = Some(userEmail.value),
      Timestamp.from(Instant.now())
    )

  def forCloneWorkspace(jobRecordId: UUID,
                        workspaceId: UUID,
                        userEmail: RawlsUserEmail,
                        args: Option[Map[String, String]],
                        jobType: JobType = JobType.CloneWorkspaceInit
  ): WorkspaceManagerResourceMonitorRecord = {
    if (!cloneJobTypes.contains(jobType)) {
      throw new IllegalArgumentException(
        s"Invalid JobType of $jobType for clone workspace job: Valid types are: ${cloneJobTypes.toString()}"
      )
    }
    WorkspaceManagerResourceMonitorRecord(
      jobRecordId,
      jobType,
      workspaceId = Some(workspaceId),
      billingProjectId = None,
      userEmail = Some(userEmail.value),
      Timestamp.from(Instant.now()),
      args
    )
  }

  def forWorkspaceDeletion(jobRecordId: UUID,
                           workspaceId: UUID,
                           userEmail: RawlsUserEmail
  ): WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord(
      jobRecordId,
      JobType.WorkspaceDeleteInit,
      workspaceId = Some(workspaceId),
      billingProjectId = None,
      userEmail = Some(userEmail.value),
      createdTime = Timestamp.from(Instant.now())
    )
}

trait WorkspaceManagerResourceJobRunner {
  // Returns Some(Outcome) if the job has completed
  def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
    executionContext: ExecutionContext
  ): Future[JobStatus]
}

// Avoid constructing directly - prefer one of the smart constructors in the
// companion object to ensure persisted state is well defined.
final case class WorkspaceManagerResourceMonitorRecord(
  jobControlId: UUID,
  jobType: JobType,
  workspaceId: Option[UUID],
  billingProjectId: Option[String],
  userEmail: Option[String],
  createdTime: Timestamp,
  args: Option[Map[String, String]] = None
)

trait WorkspaceManagerResourceMonitorRecordComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceManagerResourceMonitorRecordTable(tag: Tag)
      extends Table[WorkspaceManagerResourceMonitorRecord](tag, "WORKSPACE_MANAGER_RESOURCE_MONITOR_RECORD") {

    implicit val argsMapper: TypedType[Map[String, String]] = MappedColumnType.base[Map[String, String], String](
      map => map.toJson.compactPrint,
      str => str.parseJson.convertTo[Map[String, String]]
    )

    def jobControlId: Rep[UUID] = column[UUID]("JOB_CONTROL_ID", O.PrimaryKey)

    def jobType: Rep[JobType] = column[JobType]("JOB_TYPE")

    def workspaceId: Rep[Option[UUID]] = column[Option[UUID]]("WORKSPACE_ID")

    def billingProjectId: Rep[Option[String]] = column[Option[String]]("BILLING_PROJECT_ID")

    def userEmail: Rep[Option[String]] = column[Option[String]]("USER_EMAIL")

    def createdTime: Rep[Timestamp] = column[Timestamp]("CREATED_TIME")

    def args: Rep[Option[Map[String, String]]] = column[Option[Map[String, String]]]("ARGS")

    override def * : ProvenShape[WorkspaceManagerResourceMonitorRecord] = (
      jobControlId,
      jobType,
      workspaceId,
      billingProjectId,
      userEmail,
      createdTime,
      args
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

    def selectByWorkspaceId(workspaceId: UUID): ReadAction[Seq[WorkspaceManagerResourceMonitorRecord]] =
      query.filter(_.workspaceId === workspaceId).result

    def getRecords: ReadAction[Seq[WorkspaceManagerResourceMonitorRecord]] = query.result

    def updateJob(job: WorkspaceManagerResourceMonitorRecord): WriteAction[Int] = query.insertOrUpdate(job)

  }

}
