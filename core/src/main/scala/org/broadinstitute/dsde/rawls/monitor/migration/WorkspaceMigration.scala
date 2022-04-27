package org.broadinstitute.dsde.rawls.monitor.migration

import cats.implicits._
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.outcomeJsonFormat
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import spray.json.DefaultJsonProtocol._

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID


final case class WorkspaceMigrationDetails(id: Long,
                                           created: Instant,
                                           started: Option[Instant],
                                           updated: Instant,
                                           finished: Option[Instant],
                                           outcome: Option[Outcome]
                                          )

object WorkspaceMigrationDetails {
  def fromWorkspaceMigration(m: WorkspaceMigration, index: Int): WorkspaceMigrationDetails =
    WorkspaceMigrationDetails(
      index,
      m.created.toInstant,
      m.started.map(_.toInstant),
      m.updated.toInstant,
      m.finished.map(_.toInstant),
      m.outcome
    )

  implicit val workspaceMigrationDetailsJsonFormat = jsonFormat6(WorkspaceMigrationDetails.apply)
}

final case class WorkspaceMigration(id: Long,
                                    workspaceId: UUID,
                                    created: Timestamp,
                                    started: Option[Timestamp],
                                    updated: Timestamp,
                                    finished: Option[Timestamp],
                                    outcome: Option[Outcome],
                                    newGoogleProjectId: Option[GoogleProjectId],
                                    newGoogleProjectNumber: Option[GoogleProjectNumber],
                                    newGoogleProjectConfigured: Option[Timestamp],
                                    tmpBucketName: Option[GcsBucketName],
                                    tmpBucketCreated: Option[Timestamp],
                                    workspaceBucketTransferJobIssued: Option[Timestamp],
                                    workspaceBucketTransferred: Option[Timestamp],
                                    workspaceBucketDeleted: Option[Timestamp],
                                    finalBucketCreated: Option[Timestamp],
                                    tmpBucketTransferJobIssued: Option[Timestamp],
                                    tmpBucketTransferred: Option[Timestamp],
                                    tmpBucketDeleted: Option[Timestamp]
                                   )

private[migration]
object WorkspaceMigration {

  type RecordType = (
    Long,                 // id
      UUID,               // workspace uuid
      Timestamp,          // created
      Option[Timestamp],  // started
      Timestamp,          // updated
      Option[Timestamp],  // finished
      Option[String],     // outcome
      Option[String],     // message
      Option[String],     // newGoogleProjectId
      Option[String],     // newGoogleProjectNumber
      Option[Timestamp],  // newGoogleProjectConfigured
      Option[String],     // tmpBucketName
      Option[Timestamp],  // tmpBucketCreated
      Option[Timestamp],  // workspaceBucketTransferJobIssued
      Option[Timestamp],  // workspaceBucketTransferred
      Option[Timestamp],  // workspaceBucketDeleted
      Option[Timestamp],  // finalBucketCreated
      Option[Timestamp],  // tmpBucketTransferJobIssued
      Option[Timestamp],  // tmpBucketTransferred
      Option[Timestamp]   // tmpBucketDeleted
    )


  def fromRecord(record: RecordType): Either[String, WorkspaceMigration] = record match {
    case (
      id, workspaceId, created, started, updated, finished, outcome, message,
      newGoogleProjectId, newGoogleProjectNumber, newGoogleProjectConfigured,
      tmpBucketName, tmpBucketCreated,
      workspaceBucketTransferJobIssued, workspaceBucketTransferred, workspaceBucketDeleted,
      finalBucketCreated,
      tmpBucketTransferJobIssued, tmpBucketTransferred, tmpBucketDeleted
      ) => Outcome.fromFields(outcome, message).map { outcome =>
      WorkspaceMigration(
        id,
        workspaceId,
        created,
        started,
        updated,
        finished,
        outcome,
        newGoogleProjectId.map(GoogleProjectId),
        newGoogleProjectNumber.map(GoogleProjectNumber),
        newGoogleProjectConfigured,
        tmpBucketName.map(GcsBucketName),
        tmpBucketCreated,
        workspaceBucketTransferJobIssued,
        workspaceBucketTransferred,
        workspaceBucketDeleted,
        finalBucketCreated,
        tmpBucketTransferJobIssued,
        tmpBucketTransferred,
        tmpBucketDeleted
      )
    }
  }


  def toRecord(migration: WorkspaceMigration): RecordType = {
    val (outcome, message) = Outcome.toFields(migration.outcome)
    (
      migration.id,
      migration.workspaceId,
      migration.created,
      migration.started,
      migration.updated,
      migration.finished,
      outcome,
      message,
      migration.newGoogleProjectId.map(_.value),
      migration.newGoogleProjectNumber.map(_.value),
      migration.newGoogleProjectConfigured,
      migration.tmpBucketName.map(_.value),
      migration.tmpBucketCreated,
      migration.workspaceBucketTransferJobIssued,
      migration.workspaceBucketTransferred,
      migration.workspaceBucketDeleted,
      migration.finalBucketCreated,
      migration.tmpBucketTransferJobIssued,
      migration.tmpBucketTransferred,
      migration.tmpBucketDeleted
    )
  }
}

private[migration]
object WorkspaceMigrationHistory {

  import slick.jdbc.MySQLProfile.api._

  final class WorkspaceMigrationHistory(tag: Tag)
    extends Table[WorkspaceMigration](tag, "V1_WORKSPACE_MIGRATION_HISTORY") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workspaceId = column[UUID]("WORKSPACE_ID", O.SqlType("BINARY(16)"))
    def created = column[Timestamp]("CREATED")
    def started = column[Option[Timestamp]]("STARTED")
    def updated = column[Timestamp]("UPDATED")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")
    def newGoogleProjectId = column[Option[String]]("NEW_GOOGLE_PROJECT_ID")
    def newGoogleProjectNumber = column[Option[String]]("NEW_GOOGLE_PROJECT_NUMBER")
    def newGoogleProjectConfigured = column[Option[Timestamp]]("NEW_GOOGLE_PROJECT_CONFIGURED")
    def tmpBucket = column[Option[String]]("TMP_BUCKET")
    def tmpBucketCreated = column[Option[Timestamp]]("TMP_BUCKET_CREATED")
    def workspaceBucketTransferJobIssued = column[Option[Timestamp]]("WORKSPACE_BUCKET_TRANSFER_JOB_ISSUED")
    def workspaceBucketTransferred = column[Option[Timestamp]]("WORKSPACE_BUCKET_TRANSFERRED")
    def workspaceBucketDeleted = column[Option[Timestamp]]("WORKSPACE_BUCKET_DELETED")
    def finalBucketCreated = column[Option[Timestamp]]("FINAL_BUCKET_CREATED")
    def tmpBucketTransferJobIssued = column[Option[Timestamp]]("TMP_BUCKET_TRANSFER_JOB_ISSUED")
    def tmpBucketTransferred = column[Option[Timestamp]]("TMP_BUCKET_TRANSFERRED")
    def tmpBucketDeleted = column[Option[Timestamp]]("TMP_BUCKET_DELETED")

    override def * =
      (
        id, workspaceId, created, started, updated, finished, outcome, message,
        newGoogleProjectId, newGoogleProjectNumber, newGoogleProjectConfigured,
        tmpBucket, tmpBucketCreated,
        workspaceBucketTransferJobIssued, workspaceBucketTransferred, workspaceBucketDeleted,
        finalBucketCreated,
        tmpBucketTransferJobIssued, tmpBucketTransferred, tmpBucketDeleted
      ) <> (
        r => MigrationUtils.unsafeFromEither(WorkspaceMigration.fromRecord(r)),
        WorkspaceMigration.toRecord(_: WorkspaceMigration).some
      )
  }

  val workspaceMigrations = TableQuery[WorkspaceMigrationHistory]
}
