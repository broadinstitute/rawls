package org.broadinstitute.dsde.rawls.monitor.migration

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp

final case class MultiregionalStorageTransferJob(id: Long,
                                                 jobName: GoogleStorageTransferService.JobName,
                                                 migrationId: Long,
                                                 created: Timestamp,
                                                 updated: Timestamp,
                                                 destBucket: GcsBucketName,
                                                 sourceBucket: GcsBucketName,
                                                 finished: Option[Timestamp],
                                                 outcome: Option[Outcome],
                                                 totalBytesToTransfer: Option[Long],
                                                 bytesTransferred: Option[Long],
                                                 totalObjectsToTransfer: Option[Long],
                                                 objectsTransferred: Option[Long],
                                                 googleProject: Option[GoogleProject]
)

private[migration] object MultiregionalStorageTransferJob {
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
    Option[String], // message,
    Option[Long], // totalBytesToTransfer
    Option[Long], // bytesTransferred
    Option[Long], // totalObjectsToTransfer
    Option[Long], // objectsTransferred,
    Option[String] // googleProject
  )

  def fromRecord(record: RecordType): Either[String, MultiregionalStorageTransferJob] = record match {
    case (id,
          jobName,
          migrationId,
          created,
          updated,
          destBucket,
          sourceBucket,
          finished,
          outcome,
          message,
          totalBytesToTransfer,
          bytesTransferred,
          totalObjectsToTransfer,
          objectsTransferred,
          googleProject
        ) =>
      Outcome.fromFields(outcome, message).map { outcome =>
        MultiregionalStorageTransferJob(
          id,
          GoogleStorageTransferService.JobName(jobName),
          migrationId,
          created,
          updated,
          GcsBucketName(destBucket),
          GcsBucketName(sourceBucket),
          finished,
          outcome,
          totalBytesToTransfer,
          bytesTransferred,
          totalObjectsToTransfer,
          objectsTransferred,
          googleProject.map(GoogleProject)
        )
      }
  }

  def toRecord(job: MultiregionalStorageTransferJob): RecordType = {
    val (outcome, message) = Outcome.toFields(job.outcome)
    (
      job.id,
      job.jobName.value,
      job.migrationId,
      job.created,
      job.updated,
      job.destBucket.value,
      job.sourceBucket.value,
      job.finished,
      outcome,
      message,
      job.totalBytesToTransfer,
      job.bytesTransferred,
      job.totalObjectsToTransfer,
      job.objectsTransferred,
      job.googleProject.map(_.value)
    )
  }
}

object MultiregionalStorageTransferJobs {

  import slick.jdbc.MySQLProfile.api._

  final class MultiregionalStorageTransferJobs(tag: Tag)
      extends Table[MultiregionalStorageTransferJob](tag,
                                                     "MULTIREGIONAL_BUCKET_MIGRATION_STORAGE_TRANSFER_SERVICE_JOB"
      ) {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def jobName = column[String]("JOB_NAME")
    def migrationId = column[Long]("MIGRATION_ID")
    def created = column[Timestamp]("CREATED")
    def updated = column[Timestamp]("UPDATED")
    def destBucket = column[String]("DESTINATION_BUCKET")
    def sourceBucket = column[String]("SOURCE_BUCKET")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")
    def totalBytesToTransfer = column[Option[Long]]("TOTAL_BYTES_TO_TRANSFER")
    def bytesTransferred = column[Option[Long]]("BYTES_TRANSFERRED")
    def totalObjectsToTransfer = column[Option[Long]]("TOTAL_OBJECTS_TO_TRANSFER")
    def objectsTransferred = column[Option[Long]]("OBJECTS_TRANSFERRED")
    def googleProject = column[Option[String]]("GOOGLE_PROJECT")

    override def * = (
      id,
      jobName,
      migrationId,
      created,
      updated,
      destBucket,
      sourceBucket,
      finished,
      outcome,
      message,
      totalBytesToTransfer,
      bytesTransferred,
      totalObjectsToTransfer,
      objectsTransferred,
      googleProject
    ) <> (
      r => MigrationUtils.unsafeFromEither(MultiregionalStorageTransferJob.fromRecord(r)),
      MultiregionalStorageTransferJob.toRecord(_: MultiregionalStorageTransferJob).some
    )
  }

  val storageTransferJobs = TableQuery[MultiregionalStorageTransferJobs]
}
