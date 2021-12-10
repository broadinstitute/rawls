package org.broadinstitute.dsde.rawls.monitor.migration

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.monitor.migration.Shared.Outcome
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

import java.sql.Timestamp

private[migration]
final case class PpwStorageTransferJob(id: Long,
                                       jobName: GoogleStorageTransferService.JobName,
                                       migrationId: Long,
                                       created: Timestamp,
                                       destBucket: GcsBucketName,
                                       originBucket: GcsBucketName,
                                       finished: Option[Timestamp],
                                       outcome: Option[Outcome],
                                      )

private[migration]
object PpwStorageTransferJob {
  type RecordType = (Long, String, Long, Timestamp, String, String, Option[Timestamp], Option[String], Option[String])

  def fromRecord(record: RecordType): Either[String, PpwStorageTransferJob] = record match {
    case (id, jobName, migrationId, created, destBucket, originBucket, finished, outcome, message) =>
      Outcome.fromFields(outcome, message).map { outcome =>
        PpwStorageTransferJob(
          id,
          GoogleStorageTransferService.JobName(jobName),
          migrationId,
          created,
          GcsBucketName(destBucket),
          GcsBucketName(originBucket),
          finished,
          outcome
        )
      }
  }

  def toRecord(job: PpwStorageTransferJob): RecordType = {
    val (outcome, message) = job.outcome.map(Outcome.toTuple).getOrElse(None, None)
    (
      job.id,
      job.jobName.value,
      job.migrationId,
      job.created,
      job.destBucket.value,
      job.originBucket.value,
      job.finished,
      outcome,
      message
    )
  }
}

private[migration]
object PpwStorageTransferJobs {

  import slick.jdbc.MySQLProfile.api._

  final class PpwStorageTransferJobs(tag: Tag)
    extends Table[PpwStorageTransferJob](tag, "PPW_STORAGE_TRANSFER_SERVICE_JOB") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def jobName = column[String]("JOB_NAME")
    def migrationId = column[Long]("MIGRATION_ID")
    def created = column[Timestamp]("CREATED")
    def destBucket = column[String]("DEST_BUCKET")
    def originBucket = column[String]("ORIGIN_BUCKET")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")

    override def * =
      (id, jobName, migrationId, created, destBucket, originBucket, finished, outcome, message) <>
        (Shared.unsafeFromEither(PpwStorageTransferJob.fromRecord, _),
          PpwStorageTransferJob.toRecord(_: PpwStorageTransferJob).some)
  }

  val storageTransferJobs = TableQuery[PpwStorageTransferJobs]
}