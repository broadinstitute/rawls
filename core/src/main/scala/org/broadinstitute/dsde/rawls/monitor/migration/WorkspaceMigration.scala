package org.broadinstitute.dsde.rawls.monitor.migration

import akka.http.scaladsl.model.StatusCodes
import cats.MonadThrow
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.WorkspaceVersions.V1
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, GoogleProjectNumber, SubmissionStatuses, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.{Outcome, unsafeFromEither}
import org.broadinstitute.dsde.workbench.model.ValueObject
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import slick.jdbc.{GetResult, SQLActionBuilder, SetParameter}
import spray.json.DefaultJsonProtocol._

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID


final case class WorkspaceMigrationMetadata
(id: Long, // cardinal of migration attempts for the workspace and not the unique id of the record in the db
 created: Instant,
 started: Option[Instant],
 updated: Instant,
 finished: Option[Instant],
 outcome: Option[Outcome]
)

object WorkspaceMigrationMetadata {
  def fromWorkspaceMigration(m: WorkspaceMigration, index: Int): WorkspaceMigrationMetadata =
    WorkspaceMigrationMetadata(
      index.toLong,
      m.created.toInstant,
      m.started.map(_.toInstant),
      m.updated.toInstant,
      m.finished.map(_.toInstant),
      m.outcome
    )

  implicit val workspaceMigrationDetailsJsonFormat = jsonFormat6(WorkspaceMigrationMetadata.apply)
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
    private val allColumnsInOrder = List(
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

    private def getValueObjectOption[T](f: String => T): GetResult[Option[T]] =
      GetResult(_.nextStringOption().map(f))

    private implicit val getGoogleProjectId = getValueObjectOption(GoogleProjectId)
    private implicit val getGoogleProjectNumber = getValueObjectOption(GoogleProjectNumber)
    private implicit val getGcsBucketName = getValueObjectOption(GcsBucketName)
    private implicit val getInstant = GetResult(_.nextTimestamp().toInstant)
    private implicit val getInstantOption = GetResult(_.nextTimestampOption().map(_.toInstant))
    private implicit val getOutcomeOption: GetResult[Option[Outcome]] =
      GetResult(r => unsafeFromEither(Outcome.fromFields(r.nextStringOption(), r.nextStringOption())))
    /** the order of elements in the result set is expected to match allColumnsInOrder above */
    private implicit val getWorkspaceMigration = GetResult(r => WorkspaceMigration(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    private implicit val getWorkspaceMigrationDetails =
      GetResult(r => WorkspaceMigrationMetadata(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))


    def update[A](migrationId: Long, columnName: MigrationColumnName, value: A)
                 (implicit setA: SetParameter[A]): WriteAction[Int] =
      sqlu"update #$tableName set #$columnName = $value where #$idCol = $migrationId"

    def update2[A, B](migrationId: Long,
                      columnName1: MigrationColumnName, value1: A,
                      columnName2: MigrationColumnName, value2: B)
                     (implicit
                      setA: SetParameter[A],
                      setB: SetParameter[B]
                     ): WriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1, #$columnName2 = $value2
        where #$idCol = $migrationId
      """

    def update3[A, B, C](migrationId: Long,
                         columnName1: MigrationColumnName, value1: A,
                         columnName2: MigrationColumnName, value2: B,
                         columnName3: MigrationColumnName, value3: C)
                        (implicit
                         setA: SetParameter[A],
                         setB: SetParameter[B],
                         setC: SetParameter[C]
                        ): WriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1, #$columnName2 = $value2, #$columnName3 = $value3
        where #$idCol = $migrationId
      """

    def update8[A, B, C, D, E, F, G, H](migrationId: Long,
                                        columnName1: MigrationColumnName, value1: A,
                                        columnName2: MigrationColumnName, value2: B,
                                        columnName3: MigrationColumnName, value3: C,
                                        columnName4: MigrationColumnName, value4: D,
                                        columnName5: MigrationColumnName, value5: E,
                                        columnName6: MigrationColumnName, value6: F,
                                        columnName7: MigrationColumnName, value7: G,
                                        columnName8: MigrationColumnName, value8: H
                                       )
                                       (implicit
                                        setA: SetParameter[A],
                                        setB: SetParameter[B],
                                        setC: SetParameter[C],
                                        setD: SetParameter[D],
                                        setE: SetParameter[E],
                                        setF: SetParameter[F],
                                        setG: SetParameter[G],
                                        setH: SetParameter[H]
                                       ): WriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1,
            #$columnName2 = $value2,
            #$columnName3 = $value3,
            #$columnName4 = $value4,
            #$columnName5 = $value5,
            #$columnName6 = $value6,
            #$columnName7 = $value7,
            #$columnName8 = $value8
        where #$idCol = $migrationId
      """


    def migrationFinished(migrationId: Long, now: Timestamp, outcome: Outcome): WriteAction[Int] = {
      val (status, message) = Outcome.toTuple(outcome)
      update3(migrationId,
        finishedCol, now,
        outcomeCol, status,
        messageCol, message
      )
    }

    final def isInQueueToMigrate(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$startedCol is null".as[Int].map(_.head > 0)

    final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$startedCol is not null and #$finishedCol is null".as[Int].map(_.head > 0)

    final def schedule(workspace: Workspace): ReadWriteAction[WorkspaceMigrationMetadata] =
      MonadThrow[ReadWriteAction].raiseUnless(workspace.workspaceVersion == V1)(
        new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest,
          "This Workspace cannot be migrated - only V1 Workspaces are supported."
        ))
      ) >>
        sqlu"insert into #$tableName (#$workspaceIdCol) values (${workspace.workspaceIdAsUUID})" >>
        sql"""
            select b.normalized_id, a.#$createdCol, a.#$startedCol, a.#$updatedCol, a.#$finishedCol, a.#$outcomeCol, a.#$messageCol
            from #$tableName a
            join (select count(*) - 1 as normalized_id, max(#$idCol) as last_id from #$tableName group by #$workspaceIdCol) b
            on a.#$idCol = b.last_id
            where a.#$idCol = LAST_INSERT_ID()
        """.as[WorkspaceMigrationMetadata].head

    final def getMigrationAttempts(workspace: Workspace): ReadAction[List[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} order by #$idCol".as[WorkspaceMigration].map(_.toList)

    final def selectMigrationsWhere(conditions: SQLActionBuilder): ReadAction[Vector[WorkspaceMigration]] = {
      val startingSql = sql"select #$allColumns from #$tableName where #$finishedCol is null and "
      concatSqlActions(startingSql, conditions, sql" order by #$updatedCol asc limit 1").as[WorkspaceMigration]
    }

    final def getAttempt(workspaceUuid: UUID): ReadAction[Option[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = $workspaceUuid order by #$idCol desc limit 1".as[WorkspaceMigration].headOption

    final def getAttempt(migrationId: Long): ReadAction[Option[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$idCol = $migrationId".as[WorkspaceMigration].headOption

    final def truncate: WriteAction[Int] = sqlu"delete from #$tableName"

    // The following three definitions are `ReadWriteAction`s to make the types line up more easily
    // in their uses
    def getNumActiveMigrations: ReadWriteAction[Int] =
      sql"select count(*) from #$tableName where #$startedCol is not null and #$finishedCol is null".as[Int].head

    // The following query uses raw parameters. In this particular case it's safe to do as the
    // values of the `activeStatuses` are known and controlled by us. In general one should use
    // bind parameters for user input to avoid sql injection attacks.
    def nextMigration: ReadWriteAction[Option[(Long, UUID, Boolean)]] =
      sql"""
        select m.#$idCol, m.#$workspaceIdCol, w.is_locked from #$tableName m
        join (select id, is_locked from WORKSPACE) as w on (w.id = m.#$workspaceIdCol)
        where m.#$startedCol is null
        /* exclude workspaces with active submissions */
        and not exists (
            select workspace_id, status from SUBMISSION
            where status in #${SubmissionStatuses.activeStatuses.mkString("('", "','", "')")}
            and workspace_id = m.workspace_id
        )
        /* don't start any new migrations until the sts rate-limit has been cleared */
        and not exists (
            select * from #$tableName m2
            where m2.#$outcomeCol = 'Failure' and m2.message like $rateLimitedErrorMessage
        )
        order by m.#$idCol limit 1
        """.as[(Long, UUID, Boolean)].headOption

    def nextRateLimitedMigration: ReadWriteAction[Option[(Long, Timestamp)]] =
      sql"""
        select m.#$idCol, m.#$finishedCol from #$tableName m
        where m.#$outcomeCol = 'Failure' and m.message like $rateLimitedErrorMessage
        order by m.#$idCol
        """.as[(Long, Timestamp)].headOption

    val removeWorkspaceBucketIamCondition = selectMigrationsWhere(
      sql"#$startedCol is not null and #$workspaceBucketIamRemovedCol is null"
    )
    val configureGoogleProjectCondition = selectMigrationsWhere(
      sql"#$workspaceBucketIamRemovedCol is not null and #$newGoogleProjectConfiguredCol is null"
    )
    val createTempBucketConditionCondition = selectMigrationsWhere(
      sql"#$newGoogleProjectConfiguredCol is not null and #$tmpBucketCreatedCol is null"
    )
    val issueTransferJobToTmpBucketCondition = selectMigrationsWhere(
      sql"#$tmpBucketCreatedCol is not null and #$workspaceBucketTransferJobIssuedCol is null"
    )
    val deleteWorkspaceBucketCondition = selectMigrationsWhere(
      sql"#$workspaceBucketTransferredCol is not null and #$workspaceBucketDeletedCol is null"
    )
    val createFinalWorkspaceBucketCondition = selectMigrationsWhere(
      sql"#$workspaceBucketDeletedCol is not null and #$finalBucketCreatedCol is null"
    )
    val issueTransferJobToFinalWorkspaceBucketCondition = selectMigrationsWhere(
      sql"#$finalBucketCreatedCol is not null and #$tmpBucketTransferJobIssuedCol is null"
    )
    val deleteTemporaryBucketCondition = selectMigrationsWhere(
      sql"#$tmpBucketTransferredCol is not null and #$tmpBucketDeletedCol is null"
    )
    val restoreIamPoliciesAndUpdateWorkspaceRecordCondition = selectMigrationsWhere(
      sql"#$tmpBucketDeletedCol is not null"
    )

    def withMigrationId(migrationId: Long) = selectMigrationsWhere(sql"#$idCol = $migrationId")

    val rateLimitedErrorMessage: String =
      "%RESOURCE_EXHAUSTED: Quota exceeded for quota metric 'Create requests' " +
        "and limit 'Create requests per day' of service 'storagetransfer.googleapis.com'%"
  }
}

final case class MigrationColumnName(value: String) extends ValueObject