package org.broadinstitute.dsde.rawls.monitor.migration

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

import java.sql.Timestamp

final case class PpwStorageTransferJob(id: Long,
                                       jobName: GoogleStorageTransferService.JobName,
                                       migrationId: Long,
                                       created: Timestamp,
                                       updated: Timestamp,
                                       destBucket: GcsBucketName,
                                       originBucket: GcsBucketName,
                                       finished: Option[Timestamp],
                                       outcome: Option[Outcome]
)

private[migration] object PpwStorageTransferJob {
  type RecordType = (
    Long, // id
    String, // jobName
    Long, // migrationId
    Timestamp, // created
    Timestamp, // updated
    String, // destBucket
    String, // originBucket
    Option[Timestamp], // finished
    Option[String], // outcome
    Option[String] // message
  )

  def fromRecord(record: RecordType): Either[String, PpwStorageTransferJob] = record match {
    case (id, jobName, migrationId, created, updated, destBucket, originBucket, finished, outcome, message) =>
      Outcome.fromFields(outcome, message).map { outcome =>
        PpwStorageTransferJob(
          id,
          GoogleStorageTransferService.JobName(jobName),
          migrationId,
          created,
          updated,
          GcsBucketName(destBucket),
          GcsBucketName(originBucket),
          finished,
          outcome
        )
      }
  }

  def toRecord(job: PpwStorageTransferJob): RecordType = {
    val (outcome, message) = Outcome.toFields(job.outcome)
    (
      job.id,
      job.jobName.value,
      job.migrationId,
      job.created,
      job.updated,
      job.destBucket.value,
      job.originBucket.value,
      job.finished,
      outcome,
      message
    )
  }
}

object PpwStorageTransferJobs {

  import slick.jdbc.MySQLProfile.api._

  final class PpwStorageTransferJobs(tag: Tag)
      extends Table[PpwStorageTransferJob](tag, "PPW_STORAGE_TRANSFER_SERVICE_JOB") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def jobName = column[String]("JOB_NAME")
    def migrationId = column[Long]("MIGRATION_ID")
    def created = column[Timestamp]("CREATED")
    def updated = column[Timestamp]("UPDATED")
    def destBucket = column[String]("DEST_BUCKET")
    def originBucket = column[String]("ORIGIN_BUCKET")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")

    override def * = (
      id,
      jobName,
      migrationId,
      created,
      updated,
      destBucket,
      originBucket,
      finished,
      outcome,
      message
    ) <> (
      r => MigrationUtils.unsafeFromEither(PpwStorageTransferJob.fromRecord(r)),
      PpwStorageTransferJob.toRecord(_: PpwStorageTransferJob).some
    )
  }

  val storageTransferJobs = TableQuery[PpwStorageTransferJobs]
}
