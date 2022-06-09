package org.broadinstitute.dsde.rawls.monitor.migration

import org.broadinstitute.dsde.rawls.dataaccess.slick.{DriverComponent, RawSqlQuery, ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.outcomeJsonFormat
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.workbench.model.ValueObject
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import slick.jdbc.{GetResult, PositionedParameters, SQLActionBuilder, SetParameter}
import spray.json.DefaultJsonProtocol._

import java.sql.{SQLType, Timestamp}
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
      index.toLong,
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
                                    tmpBucketDeleted: Option[Timestamp],
                                    requesterPaysEnabled: Boolean,
                                    unlockOnCompletion: Boolean,
                                    workspaceBucketIamRemoved: Option[Timestamp],
                                   )

trait WorkspaceMigrationHistory extends RawSqlQuery {
  this: DriverComponent =>

  import driver.api._

  object workspaceMigrationQuery {

    val tableName = "V1_WORKSPACE_MIGRATION_HISTORY"

    val id = MigrationColumnName("id")
    val workspaceId = MigrationColumnName("WORKSPACE_ID")
    val created = MigrationColumnName("CREATED")
    val started = MigrationColumnName("STARTED")
    val updated = MigrationColumnName("UPDATED")
    val finished = MigrationColumnName("FINISHED")
    val outcome = MigrationColumnName("OUTCOME")
    val message = MigrationColumnName("MESSAGE")
    val newGoogleProjectId = MigrationColumnName("NEW_GOOGLE_PROJECT_ID")
    val newGoogleProjectNumber = MigrationColumnName("NEW_GOOGLE_PROJECT_NUMBER")
    val newGoogleProjectConfigured = MigrationColumnName("NEW_GOOGLE_PROJECT_CONFIGURED")
    val tmpBucket = MigrationColumnName("TMP_BUCKET")
    val tmpBucketCreated = MigrationColumnName("TMP_BUCKET_CREATED")
    val workspaceBucketTransferJobIssued = MigrationColumnName("WORKSPACE_BUCKET_TRANSFER_JOB_ISSUED")
    val workspaceBucketTransferred = MigrationColumnName("WORKSPACE_BUCKET_TRANSFERRED")
    val workspaceBucketDeleted = MigrationColumnName("WORKSPACE_BUCKET_DELETED")
    val finalBucketCreated = MigrationColumnName("FINAL_BUCKET_CREATED")
    val tmpBucketTransferJobIssued = MigrationColumnName("TMP_BUCKET_TRANSFER_JOB_ISSUED")
    val tmpBucketTransferred = MigrationColumnName("TMP_BUCKET_TRANSFERRED")
    val tmpBucketDeleted = MigrationColumnName("TMP_BUCKET_DELETED")
    val requesterPaysEnabled = MigrationColumnName("REQUESTER_PAYS_ENABLED")
    val unlockOnCompletion = MigrationColumnName("UNLOCK_ON_COMPLETION")
    val workspaceBucketIamRemoved = MigrationColumnName("WORKSPACE_BUCKET_IAM_REMOVED")

    val allColumns = s"""$id,
                        |$workspaceId,
                        |$created,
                        |$started,
                        |$updated,
                        |$finished,
                        |$outcome,
                        |$message,
                        |$newGoogleProjectId,
                        |$newGoogleProjectNumber,
                        |$newGoogleProjectConfigured,
                        |$tmpBucket,
                        |$tmpBucketCreated,
                        |$workspaceBucketTransferJobIssued,
                        |$workspaceBucketTransferred,
                        |$workspaceBucketDeleted,
                        |$finalBucketCreated,
                        |$tmpBucketTransferJobIssued,
                        |$tmpBucketTransferred,
                        |$tmpBucketDeleted,
                        |$requesterPaysEnabled,
                        |$unlockOnCompletion,
                        |$workspaceBucketIamRemoved""".stripMargin

    implicit val getGoogleProjectId = GetResult(r => Option(r.nextString).map(GoogleProjectId))
    implicit val getGoogleProjectNumber = GetResult(r => Option(r.nextString).map(GoogleProjectNumber))
    implicit val getGcsBucketName = GetResult(r => Option(r.nextString).map(GcsBucketName))
    implicit val getWorkspaceMigration = GetResult(r => WorkspaceMigration(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, Outcome.tupled((r.<<, r.<<)), r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    object SetAnyParameter extends SetParameter[Any] {
      def apply(v: Any, pp: PositionedParameters) {
        v match {
          case n: Long => pp.setLong(n)
          case s: String => pp.setString(s)
          case t: Timestamp => pp.setTimestamp(t)
          case vo: ValueObject => pp.setString(vo.value)
          case b: Boolean => pp.setBoolean(b)
          case Some(opt: Any) => SetAnyParameter(opt, pp)
          case _ => throw new IllegalArgumentException(s"illegal parameter type ${v.getClass}, value $v")
        }
      }
    }

    def update(migrationId: Long, columnName: MigrationColumnName, value: Any): WriteAction[Int] = {
      implicit val setAnyParameter = SetAnyParameter
      sqlu"update #$tableName set #$columnName = $value where #$id = $migrationId"
    }

    def update2(migrationId: Long,
                columnName1: MigrationColumnName, value1: Any,
                columnName2: MigrationColumnName, value2: Any): WriteAction[Int] = {
      implicit val setAnyParameter = SetAnyParameter
      sqlu"update #$tableName set #$columnName1 = $value1, #$columnName2 = $value2 where #$id = $migrationId"
    }

    def update3(migrationId: Long,
                columnName1: MigrationColumnName, value1: Any,
                columnName2: MigrationColumnName, value2: Any,
                columnName3: MigrationColumnName, value3: Any): WriteAction[Int] = {
      implicit val setAnyParameter = SetAnyParameter
      sqlu"update #$tableName set #$columnName1 = $value1, #$columnName2 = $value2, #$columnName3 = $value3 where #$id = $migrationId"
    }

    def migrationFinished(migrationId: Long, now: Timestamp, outcomeVal: Outcome): WriteAction[Int] = {
      val (status, messageVal) = Outcome.toTuple(outcomeVal)
      sqlu"update #$tableName set #$finished = $now, #$outcome = $status, #$message = $messageVal where #$id = $migrationId"
    }

    final def isInQueueToMigrate(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceId = ${workspace.workspaceIdAsUUID} and #$started is not null".as[Int].map(_.head > 0)

    final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceId = ${workspace.workspaceIdAsUUID} and #$started is not null and #$finished is null".as[Int].map(_.head > 0)

    final def schedule(workspace: Workspace, unlockOnCompletionBool: Boolean): WriteAction[Int] = {
      sqlu"insert into #$tableName (#$workspaceId, #$unlockOnCompletion) values (${workspace.workspaceIdAsUUID}, $unlockOnCompletionBool)"
    }

    final def getMigrationAttempts(workspace: Workspace): ReadAction[List[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceId = ${workspace.workspaceIdAsUUID} order by #$id".as[WorkspaceMigration].map(_.toList)

    final def selectMigrations(conditions: SQLActionBuilder): ReadAction[Vector[WorkspaceMigration]] = {
      val startingSql = sql"select #$allColumns from #$tableName where #$finished is null and "
      concatSqlActions(startingSql, conditions, sql" order by #$updated asc limit 1").as[WorkspaceMigration]
    }

    final def getAttempt(workspaceUuid: UUID): ReadAction[Option[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceId = $workspaceUuid order by #$id desc limit 1".as[WorkspaceMigration].headOption

    final def getAttempt(id: Long): ReadAction[Option[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$id = $id".as[WorkspaceMigration].headOption

    final def truncate: WriteAction[Int] = sqlu"delete from #$tableName"

    val startCondition = sql"#$started is null"
    val removeWorkspaceBucketIamCondition = sql"#$started is not null and #$workspaceBucketIamRemoved is null"
    val claimAndConfigureGoogleProjectCondition = sql"#$workspaceBucketIamRemoved is not null and #$newGoogleProjectConfigured is null"
    val createTempBucketConditionCondition = sql"#$newGoogleProjectConfigured is not null and #$tmpBucketCreated is null"
    val issueTransferJobToTmpBucketCondition = sql"#$tmpBucketCreated is not null and #$workspaceBucketTransferJobIssued is null"
    val deleteWorkspaceBucketCondition = sql"#$workspaceBucketTransferred is not null and #$workspaceBucketDeleted is null"
    val createFinalWorkspaceBucketCondition = sql"#$workspaceBucketDeleted is not null and #$finalBucketCreated is null"
    val issueTransferJobToFinalWorkspaceBucketCondition = sql"#$finalBucketCreated is not null and #$tmpBucketTransferJobIssued is null"
    val deleteTemporaryBucketCondition = sql"#$tmpBucketTransferred is not null and #$tmpBucketDeleted is null"
    val restoreIamPoliciesAndUpdateWorkspaceRecordCondition = sql"#$tmpBucketDeleted is not null"

    def transferJobSucceededCondition(migrationId: Long) = sql"#$id = $migrationId"
  }
}

final case class MigrationColumnName(value: String) extends ValueObject