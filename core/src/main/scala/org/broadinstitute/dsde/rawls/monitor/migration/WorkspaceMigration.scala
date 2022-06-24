package org.broadinstitute.dsde.rawls.monitor.migration

import org.broadinstitute.dsde.rawls.dataaccess.slick.{DriverComponent, RawSqlQuery, ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.outcomeJsonFormat
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.{Outcome, unsafeFromEither}
import org.broadinstitute.dsde.workbench.model.ValueObject
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import slick.jdbc.{GetResult, PositionedParameters, SQLActionBuilder, SetParameter}
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

    private val tableName = "V1_WORKSPACE_MIGRATION_HISTORY"

    val idCol = MigrationColumnName("id")
    val workspaceIdCol = MigrationColumnName("WORKSPACE_ID")
    val createdCol = MigrationColumnName("CREATED")
    val startedCol = MigrationColumnName("STARTED")
    val updatedCol = MigrationColumnName("UPDATED")
    val finishedCol = MigrationColumnName("FINISHED")
    val outcomeCol = MigrationColumnName("OUTCOME")
    val messageCol = MigrationColumnName("MESSAGE")
    val newGoogleProjectIdCol = MigrationColumnName("NEW_GOOGLE_PROJECT_ID")
    val newGoogleProjectNumberCol = MigrationColumnName("NEW_GOOGLE_PROJECT_NUMBER")
    val newGoogleProjectConfiguredCol = MigrationColumnName("NEW_GOOGLE_PROJECT_CONFIGURED")
    val tmpBucketCol = MigrationColumnName("TMP_BUCKET")
    val tmpBucketCreatedCol = MigrationColumnName("TMP_BUCKET_CREATED")
    val workspaceBucketTransferJobIssuedCol = MigrationColumnName("WORKSPACE_BUCKET_TRANSFER_JOB_ISSUED")
    val workspaceBucketTransferredCol = MigrationColumnName("WORKSPACE_BUCKET_TRANSFERRED")
    val workspaceBucketDeletedCol = MigrationColumnName("WORKSPACE_BUCKET_DELETED")
    val finalBucketCreatedCol = MigrationColumnName("FINAL_BUCKET_CREATED")
    val tmpBucketTransferJobIssuedCol = MigrationColumnName("TMP_BUCKET_TRANSFER_JOB_ISSUED")
    val tmpBucketTransferredCol = MigrationColumnName("TMP_BUCKET_TRANSFERRED")
    val tmpBucketDeletedCol = MigrationColumnName("TMP_BUCKET_DELETED")
    val requesterPaysEnabledCol = MigrationColumnName("REQUESTER_PAYS_ENABLED")
    val unlockOnCompletionCol = MigrationColumnName("UNLOCK_ON_COMPLETION")
    val workspaceBucketIamRemovedCol = MigrationColumnName("WORKSPACE_BUCKET_IAM_REMOVED")

    /** this order matches what is expected by getWorkspaceMigration below */
    private val allColumnsInOrder = List (
      idCol,
      workspaceIdCol,
      createdCol,
      startedCol,
      updatedCol,
      finishedCol,
      outcomeCol,
      messageCol,
      newGoogleProjectIdCol,
      newGoogleProjectNumberCol,
      newGoogleProjectConfiguredCol,
      tmpBucketCol,
      tmpBucketCreatedCol,
      workspaceBucketTransferJobIssuedCol,
      workspaceBucketTransferredCol,
      workspaceBucketDeletedCol,
      finalBucketCreatedCol,
      tmpBucketTransferJobIssuedCol,
      tmpBucketTransferredCol,
      tmpBucketDeletedCol,
      requesterPaysEnabledCol,
      unlockOnCompletionCol,
      workspaceBucketIamRemovedCol,
    )

    private val allColumns = allColumnsInOrder.mkString(",")

    private implicit val getGoogleProjectId = GetResult(r => Option(r.nextString).map(GoogleProjectId))
    private implicit val getGoogleProjectNumber = GetResult(r => Option(r.nextString).map(GoogleProjectNumber))
    private implicit val getGcsBucketName = GetResult(r => Option(r.nextString).map(GcsBucketName))
    /** the order of elements in the result set is expected to match allColumnsInOrder above */
    private implicit val getWorkspaceMigration = GetResult(r => WorkspaceMigration(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, unsafeFromEither(Outcome.fromFields(r.<<, r.<<)), r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    private object SetAnyParameter extends SetParameter[Any] {
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
      sqlu"update #$tableName set #$columnName = $value where #$idCol = $migrationId"
    }

    def update2(migrationId: Long,
                columnName1: MigrationColumnName, value1: Any,
                columnName2: MigrationColumnName, value2: Any): WriteAction[Int] = {
      implicit val setAnyParameter = SetAnyParameter
      sqlu"update #$tableName set #$columnName1 = $value1, #$columnName2 = $value2 where #$idCol = $migrationId"
    }

    def update3(migrationId: Long,
                columnName1: MigrationColumnName, value1: Any,
                columnName2: MigrationColumnName, value2: Any,
                columnName3: MigrationColumnName, value3: Any): WriteAction[Int] = {
      implicit val setAnyParameter = SetAnyParameter
      sqlu"update #$tableName set #$columnName1 = $value1, #$columnName2 = $value2, #$columnName3 = $value3 where #$idCol = $migrationId"
    }

    def update8(migrationId: Long,
                columnName1: MigrationColumnName, value1: Any,
                columnName2: MigrationColumnName, value2: Any,
                columnName3: MigrationColumnName, value3: Any,
                columnName4: MigrationColumnName, value4: Any,
                columnName5: MigrationColumnName, value5: Any,
                columnName6: MigrationColumnName, value6: Any,
                columnName7: MigrationColumnName, value7: Any,
                columnName8: MigrationColumnName, value8: Any
               ): WriteAction[Int] = {
      implicit val setAnyParameter = SetAnyParameter
      sqlu"""
        update #$tableName
        set
            #$columnName1 = $value1,
            #$columnName2 = $value2,
            #$columnName3 = $value3,
            #$columnName4 = $value4,
            #$columnName5 = $value5,
            #$columnName6 = $value6,
            #$columnName7 = $value7,
            #$columnName8 = $value8
        where #$idCol = $migrationId
      """
    }



    def migrationFinished(migrationId: Long, now: Timestamp, outcome: Outcome): WriteAction[Int] = {
      val (status, message) = Outcome.toTuple(outcome)
      sqlu"update #$tableName set #$finishedCol = $now, #$outcomeCol = $status, #$messageCol = $message where #$idCol = $migrationId"
    }

    final def isInQueueToMigrate(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$startedCol is null".as[Int].map(_.head > 0)

    final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$startedCol is not null and #$finishedCol is null".as[Int].map(_.head > 0)

    final def schedule(workspace: Workspace, unlockOnCompletionBool: Boolean): WriteAction[Int] = {
      sqlu"insert into #$tableName (#$workspaceIdCol, #$unlockOnCompletionCol) values (${workspace.workspaceIdAsUUID}, $unlockOnCompletionBool)"
    }

    final def getMigrationAttempts(workspace: Workspace): ReadAction[List[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} order by #$idCol".as[WorkspaceMigration].map(_.toList)

    final def selectMigrations(conditions: SQLActionBuilder): ReadAction[Vector[WorkspaceMigration]] = {
      val startingSql = sql"select #$allColumns from #$tableName where #$finishedCol is null and "
      concatSqlActions(startingSql, conditions, sql" order by #$updatedCol asc limit 1").as[WorkspaceMigration]
    }

    final def getAttempt(workspaceUuid: UUID): ReadAction[Option[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = $workspaceUuid order by #$idCol desc limit 1".as[WorkspaceMigration].headOption

    final def getAttempt(migrationId: Long): ReadAction[Option[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$idCol = $migrationId".as[WorkspaceMigration].headOption

    final def truncate: WriteAction[Int] = sqlu"delete from #$tableName"

    val startCondition = sql"#$startedCol is null"
    val removeWorkspaceBucketIamCondition = sql"#$startedCol is not null and #$workspaceBucketIamRemovedCol is null"
    val configureGoogleProjectCondition = sql"#$workspaceBucketIamRemovedCol is not null and #$newGoogleProjectConfiguredCol is null"
    val createTempBucketConditionCondition = sql"#$newGoogleProjectConfiguredCol is not null and #$tmpBucketCreatedCol is null"
    val issueTransferJobToTmpBucketCondition = sql"#$tmpBucketCreatedCol is not null and #$workspaceBucketTransferJobIssuedCol is null"
    val deleteWorkspaceBucketCondition = sql"#$workspaceBucketTransferredCol is not null and #$workspaceBucketDeletedCol is null"
    val createFinalWorkspaceBucketCondition = sql"#$workspaceBucketDeletedCol is not null and #$finalBucketCreatedCol is null"
    val issueTransferJobToFinalWorkspaceBucketCondition = sql"#$finalBucketCreatedCol is not null and #$tmpBucketTransferJobIssuedCol is null"
    val deleteTemporaryBucketCondition = sql"#$tmpBucketTransferredCol is not null and #$tmpBucketDeletedCol is null"
    val restoreIamPoliciesAndUpdateWorkspaceRecordCondition = sql"#$tmpBucketDeletedCol is not null"

    def withMigrationId(migrationId: Long) = sql"#$idCol = $migrationId"
  }
}

final case class MigrationColumnName(value: String) extends ValueObject