package org.broadinstitute.dsde.rawls.monitor.migration

import cats.implicits.catsSyntaxOptionId
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber}
import org.broadinstitute.dsde.rawls.monitor.migration.Shared.Outcome
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

import java.sql.Timestamp
import java.util.UUID


private[migration]
final case class WorkspaceMigration(id: Long,
                                    workspaceId: UUID,
                                    created: Timestamp,
                                    started: Option[Timestamp],
                                    finished: Option[Timestamp],
                                    outcome: Option[Outcome],
                                    newGoogleProjectId: Option[GoogleProjectId],
                                    newGoogleProjectNumber: Option[GoogleProjectNumber],
                                    newGoogleProjectConfigured: Option[Timestamp],
                                    tmpBucketName: Option[GcsBucketName],
                                    tmpBucketCreated: Option[Timestamp],
                                    finalBucketCreated: Option[Timestamp]
                                   )

private[migration]
object WorkspaceMigration {

  type RecordType = (
    Long, UUID, Timestamp, Option[Timestamp], Option[Timestamp], Option[String], Option[String],
      Option[String], Option[String], Option[Timestamp],
      Option[String], Option[Timestamp],
      Option[Timestamp]
    )


  def fromRecord(record: RecordType): Either[String, WorkspaceMigration] = record match {
    case (id, workspaceId, created, started, finished, outcome, message,
    newGoogleProjectId, newGoogleProjectNumber, newGoogleProjectConfigured,
    tmpBucketName, tmpBucketCreated,
    finalBucketCreated) => Outcome.fromFields(outcome, message).map { outcome =>
      WorkspaceMigration(
        id,
        workspaceId,
        created,
        started,
        finished,
        outcome,
        newGoogleProjectId.map(GoogleProjectId),
        newGoogleProjectNumber.map(GoogleProjectNumber),
        newGoogleProjectConfigured,
        tmpBucketName.map(GcsBucketName),
        tmpBucketCreated,
        finalBucketCreated
      )
    }
  }


  def toRecord(migration: WorkspaceMigration): RecordType = {
    val (outcome, message) = migration.outcome.map(Outcome.toTuple).getOrElse((None, None))
    (
      migration.id,
      migration.workspaceId,
      migration.created,
      migration.started,
      migration.finished,
      outcome,
      message,
      migration.newGoogleProjectId.map(_.value),
      migration.newGoogleProjectNumber.map(_.value),
      migration.newGoogleProjectConfigured,
      migration.tmpBucketName.map(_.value),
      migration.tmpBucketCreated,
      migration.finalBucketCreated
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
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")
    def newGoogleProjectId = column[Option[String]]("NEW_GOOGLE_PROJECT_ID")
    def newGoogleProjectNumber = column[Option[String]]("NEW_GOOGLE_PROJECT_NUMBER")
    def newGoogleProjectConfigured = column[Option[Timestamp]]("NEW_GOOGLE_PROJECT_CONFIGURED")
    def tmpBucket = column[Option[String]]("TMP_BUCKET")
    def tmpBucketCreated = column[Option[Timestamp]]("TMP_BUCKET_CREATED")
    def finalBucketCreated = column[Option[Timestamp]]("FINAL_BUCKET_CREATED")

    override def * =
      (id, workspaceId, created, started, finished, outcome, message,
        newGoogleProjectId, newGoogleProjectNumber, newGoogleProjectConfigured,
        tmpBucket, tmpBucketCreated,
        finalBucketCreated
      ) <>
        (Shared.unsafeFromEither(WorkspaceMigration.fromRecord, _),
          WorkspaceMigration.toRecord(_: WorkspaceMigration).some)
  }

  val workspaceMigrations = TableQuery[WorkspaceMigrationHistory]
}