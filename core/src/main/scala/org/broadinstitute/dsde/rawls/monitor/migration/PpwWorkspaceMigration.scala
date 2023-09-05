package org.broadinstitute.dsde.rawls.monitor.migration

import akka.http.scaladsl.model.StatusCodes
import cats.MonadThrow
import cats.data.OptionT
import cats.implicits.{catsSyntaxFunction1FlatMap, toFoldableOps}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.WorkspaceVersions.V1
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  GoogleProjectId,
  GoogleProjectNumber,
  SubmissionStatuses,
  Workspace,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.{unsafeFromEither, Outcome}
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.ValueObject
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import slick.jdbc.{GetResult, SQLActionBuilder, SetParameter}
import spray.json.DefaultJsonProtocol._

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

final case class WorkspaceMigrationMetadata(
  id: Long, // cardinal of migration attempts for the workspace and not the unique id of the record in the db
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
                                    workspaceBucketTransferIamConfigured: Option[Timestamp],
                                    workspaceBucketTransferJobIssued: Option[Timestamp],
                                    workspaceBucketTransferred: Option[Timestamp],
                                    workspaceBucketDeleted: Option[Timestamp],
                                    finalBucketCreated: Option[Timestamp],
                                    tmpBucketTransferIamConfigured: Option[Timestamp],
                                    tmpBucketTransferJobIssued: Option[Timestamp],
                                    tmpBucketTransferred: Option[Timestamp],
                                    tmpBucketDeleted: Option[Timestamp],
                                    requesterPaysEnabled: Boolean,
                                    unlockOnCompletion: Boolean,
                                    workspaceBucketIamRemoved: Option[Timestamp]
)

final case class MigrationRetry(id: Long, migrationId: Long, numRetries: Long)

object FailureModes {
  // when issuing storage transfer jobs. caused by delays in propagating iam policy changes
  val noBucketPermissionsFailure: String =
    "%FAILED_PRECONDITION: Service account project-%@storage-transfer-service.iam.gserviceaccount.com " +
      "does not have required permissions%"

  // sts rates-limits us if we run too many concurrent jobs or exceed a threshold number of requests
  val stsRateLimitedFailure: String =
    "%RESOURCE_EXHAUSTED: Quota exceeded for quota metric 'Create requests' " +
      "and limit 'Create requests per day' of service 'storagetransfer.googleapis.com'%"

  val bucketNotFoundFailure: String = "The specified bucket does not exist."

  // transfer operations fail midway
  val noObjectPermissionsFailure: String =
    "%PERMISSION_DENIED%project-%@storage-transfer-service.iam.gserviceaccount.com " +
      "does not have storage.objects.% access to the Google Cloud Storage%"

  val gcsUnavailableFailure: String =
    "%UNAVAILABLE:%Additional details: GCS is temporarily unavailable."
}

trait WorkspaceMigrationHistory extends DriverComponent with RawSqlQuery {
  this: WorkspaceComponent =>

  import driver.api._

  implicit def setValueObject[A <: ValueObject]: SetParameter[A] =
    SetParameter((v, pp) => pp.setString(v.value))

  implicit def setOptionValueObject[A <: ValueObject]: SetParameter[Option[A]] =
    SetParameter((v, pp) => pp.setStringOption(v.map(_.value)))

  def getOption[T](f: String => T): GetResult[Option[T]] =
    GetResult(_.nextStringOption().map(f))

  implicit val getGoogleProjectId = getOption(GoogleProjectId)
  implicit val getGoogleProjectNumber = getOption(GoogleProjectNumber)
  implicit val getGcsBucketName = getOption(GcsBucketName)
  implicit val getInstant = GetResult(_.nextTimestamp().toInstant)
  implicit val getInstantOption = GetResult(_.nextTimestampOption().map(_.toInstant))

  object workspaceMigrationQuery
      extends RawTableQuery[Long]("V1_WORKSPACE_MIGRATION_HISTORY", primaryKey = ColumnName("id")) {

    val idCol = primaryKey
    val workspaceIdCol = ColumnName[UUID]("WORKSPACE_ID")
    val createdCol = ColumnName[Timestamp]("CREATED")
    val startedCol = ColumnName[Option[Timestamp]]("STARTED")
    val updatedCol = ColumnName[Timestamp]("UPDATED")
    val finishedCol = ColumnName[Option[Timestamp]]("FINISHED")
    val outcomeCol = ColumnName[Option[String]]("OUTCOME")
    val messageCol = ColumnName[Option[String]]("MESSAGE")
    val newGoogleProjectIdCol = ColumnName[Option[GoogleProjectId]]("NEW_GOOGLE_PROJECT_ID")
    val newGoogleProjectNumberCol = ColumnName[Option[GoogleProjectNumber]]("NEW_GOOGLE_PROJECT_NUMBER")
    val newGoogleProjectConfiguredCol = ColumnName[Option[Timestamp]]("NEW_GOOGLE_PROJECT_CONFIGURED")
    val tmpBucketCol = ColumnName[Option[GcsBucketName]]("TMP_BUCKET")
    val tmpBucketCreatedCol = ColumnName[Option[Timestamp]]("TMP_BUCKET_CREATED")
    val workspaceBucketTransferIamConfiguredCol =
      ColumnName[Option[Timestamp]]("WORKSPACE_BUCKET_TRANSFER_IAM_CONFIGURED")
    val workspaceBucketTransferJobIssuedCol = ColumnName[Option[Timestamp]]("WORKSPACE_BUCKET_TRANSFER_JOB_ISSUED")
    val workspaceBucketTransferredCol = ColumnName[Option[Timestamp]]("WORKSPACE_BUCKET_TRANSFERRED")
    val workspaceBucketDeletedCol = ColumnName[Option[Timestamp]]("WORKSPACE_BUCKET_DELETED")
    val finalBucketCreatedCol = ColumnName[Option[Timestamp]]("FINAL_BUCKET_CREATED")
    val tmpBucketTransferIamConfiguredCol = ColumnName[Option[Timestamp]]("TMP_BUCKET_TRANSFER_IAM_CONFIGURED")
    val tmpBucketTransferJobIssuedCol = ColumnName[Option[Timestamp]]("TMP_BUCKET_TRANSFER_JOB_ISSUED")
    val tmpBucketTransferredCol = ColumnName[Option[Timestamp]]("TMP_BUCKET_TRANSFERRED")
    val tmpBucketDeletedCol = ColumnName[Option[Timestamp]]("TMP_BUCKET_DELETED")
    val requesterPaysEnabledCol = ColumnName[Boolean]("REQUESTER_PAYS_ENABLED")
    val unlockOnCompletionCol = ColumnName[Boolean]("UNLOCK_ON_COMPLETION")
    val workspaceBucketIamRemovedCol = ColumnName[Option[Timestamp]]("WORKSPACE_BUCKET_IAM_REMOVED")

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
      workspaceBucketTransferIamConfiguredCol,
      workspaceBucketTransferJobIssuedCol,
      workspaceBucketTransferredCol,
      workspaceBucketDeletedCol,
      finalBucketCreatedCol,
      tmpBucketTransferIamConfiguredCol,
      tmpBucketTransferJobIssuedCol,
      tmpBucketTransferredCol,
      tmpBucketDeletedCol,
      requesterPaysEnabledCol,
      unlockOnCompletionCol,
      workspaceBucketIamRemovedCol
    )

    private val allColumns = allColumnsInOrder.mkString(",")

    implicit val getOutcomeOption: GetResult[Option[Outcome]] =
      GetResult(r => unsafeFromEither(Outcome.fromFields(r.nextStringOption(), r.nextStringOption())))

    /** the order of elements in the result set is expected to match allColumnsInOrder above */
    implicit private val getWorkspaceMigration = GetResult(r =>
      WorkspaceMigration(r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<,
                         r.<<
      )
    )

    implicit private val getWorkspaceMigrationDetails =
      GetResult(r => WorkspaceMigrationMetadata(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    def setMigrationFinished(migrationId: Long, now: Timestamp, outcome: Outcome): ReadWriteAction[Int] = {
      val (status, message) = Outcome.toTuple(outcome)
      update3(migrationId, finishedCol, Some(now), outcomeCol, Some(status), messageCol, message)
    }

    final def isPendingMigration(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$finishedCol is null"
        .as[Int]
        .map(_.head > 0)

    final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$startedCol is not null and #$finishedCol is null"
        .as[Int]
        .map(_.head > 0)

    final def scheduleAndGetMetadata: WorkspaceName => ReadWriteAction[WorkspaceMigrationMetadata] =
      (schedule _) >=> getMetadata

    final def schedule(workspaceName: WorkspaceName): ReadWriteAction[Long] =
      for {
        workspaceOpt <- workspaceQuery.findByName(workspaceName)
        workspace <- MonadThrow[ReadWriteAction].fromOption(workspaceOpt, NoSuchWorkspaceException(workspaceName))

        _ <- MonadThrow[ReadWriteAction].raiseUnless(workspace.workspaceVersion == V1) {
          new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest,
                        s"'$workspaceName' cannot be migrated because it is not a V1 workspace."
            )
          )
        }

        isPending <- isPendingMigration(workspace)
        _ <- MonadThrow[ReadWriteAction].raiseWhen(isPending) {
          new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, s"Workspace '$workspaceName' is already pending migration.")
          )
        }

        _ <- sqlu"insert into #$tableName (#$workspaceIdCol) values (${workspace.workspaceIdAsUUID})"
        id <- sql"select LAST_INSERT_ID()".as[Long].head
      } yield id

    final def getMetadata(migrationId: Long): ReadAction[WorkspaceMigrationMetadata] =
      sql"""
        select b.normalized_id, a.#$createdCol, a.#$startedCol, a.#$updatedCol, a.#$finishedCol, a.#$outcomeCol, a.#$messageCol
        from #$tableName a
        join (select count(*) - 1 as normalized_id, max(#$idCol) as last_id from #$tableName group by #$workspaceIdCol) b
        on a.#$idCol = b.last_id
        where a.#$idCol = $migrationId
        """
        .as[WorkspaceMigrationMetadata]
        .headOption
        .map(
          _.getOrElse(
            throw new NoSuchElementException(s"No workspace migration with id = '$migrationId'.'")
          )
        )

    final def getMigrationAttempts(workspace: Workspace): ReadAction[List[WorkspaceMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} order by #$idCol"
        .as[WorkspaceMigration]
        .map(_.toList)

    final def selectMigrationsWhere(conditions: SQLActionBuilder): ReadAction[Vector[WorkspaceMigration]] = {
      val startingSql = sql"select #$allColumns from #$tableName where #$finishedCol is null and "
      concatSqlActions(startingSql, conditions, sql" order by #$updatedCol asc limit 1").as[WorkspaceMigration]
    }

    final def getAttempt(workspaceUuid: UUID) = OptionT[ReadWriteAction, WorkspaceMigration] {
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = $workspaceUuid order by #$idCol desc limit 1"
        .as[WorkspaceMigration]
        .headOption
    }

    final def getAttempt(migrationId: Long) = OptionT[ReadWriteAction, WorkspaceMigration] {
      sql"select #$allColumns from #$tableName where #$idCol = $migrationId".as[WorkspaceMigration].headOption
    }

    // Resource-limited migrations are those requiring new google projects and storage transfer jobs
    final def getNumActiveResourceLimitedMigrations: ReadWriteAction[Int] =
      sql"""
        select count(*) from #$tableName m
        join (select id, namespace from WORKSPACE) as w on (w.id = m.#$workspaceIdCol)
        where m.#$startedCol is not null and m.#$finishedCol is null and exists (
            /* Only-child migrations are excluded from the max concurrent migrations quota. */
            select null from WORKSPACE where id <> w.id and namespace = w.namespace
        )
        """.as[Int].head

    // The following query uses raw parameters. In this particular case it's safe to do as the
    // values of the `activeStatuses` are known and controlled by us. In general one should use
    // bind parameters for user input to avoid sql injection attacks.
    final def nextMigration(onlyChild: Boolean) = OptionT[ReadWriteAction, (Long, UUID, String)] {
      concatSqlActions(
        sql"""
            select m.#$idCol, m.#$workspaceIdCol, CONCAT_WS("/", w.namespace, w.name) from #$tableName m
            join (select id, namespace, name, is_locked from WORKSPACE) as w on (w.id = m.#$workspaceIdCol)
            where m.#$startedCol is null
            /* exclude workspaces with active submissions */
            and not exists (
                select workspace_id, status from SUBMISSION
                where status in #${SubmissionStatuses.activeStatuses.mkString("('", "','", "')")}
                and workspace_id = m.workspace_id
            )
            """,
        if (onlyChild) sql"""and not exists (select null from WORKSPACE where namespace = w.namespace and id <> w.id)"""
        else sql"",
        sql"order by m.#$idCol limit 1"
      ).as[(Long, UUID, String)].headOption
    }

    def getWorkspaceName(migrationId: Long) = OptionT[ReadWriteAction, WorkspaceName] {
      sql"""
          select w.namespace, w.name from WORKSPACE w
          where w.id in (select #$workspaceIdCol from #$tableName m where m.id = $migrationId)
          """.as[(String, String)].headOption.map(_.map(WorkspaceName.tupled))
    }

    final def wasLockedByPreviousMigration(workspaceId: UUID): ReadWriteAction[Boolean] =
      sql"""
        select exists(
            select null from #$tableName
            where #$workspaceIdCol = $workspaceId and #$unlockOnCompletionCol = b'1'
        )
        """.as[Boolean].head

    val removeWorkspaceBucketIamCondition = selectMigrationsWhere(
      sql"#$startedCol is not null and #$workspaceBucketIamRemovedCol is null"
    )
    val configureGoogleProjectCondition = selectMigrationsWhere(
      sql"#$workspaceBucketIamRemovedCol is not null and #$newGoogleProjectConfiguredCol is null"
    )
    val createTempBucketConditionCondition = selectMigrationsWhere(
      sql"#$newGoogleProjectConfiguredCol is not null and #$tmpBucketCreatedCol is null"
    )
    val configureWorkspaceBucketTransferIam = selectMigrationsWhere(
      sql"#$tmpBucketCreatedCol is not null and #$workspaceBucketTransferIamConfiguredCol is null"
    )
    val issueTransferJobToTmpBucketCondition = selectMigrationsWhere(
      sql"#$workspaceBucketTransferIamConfiguredCol is not null and #$workspaceBucketTransferJobIssuedCol is null"
    )
    val deleteWorkspaceBucketCondition = selectMigrationsWhere(
      sql"#$workspaceBucketTransferredCol is not null and #$workspaceBucketDeletedCol is null"
    )
    val createFinalWorkspaceBucketCondition = selectMigrationsWhere(
      sql"#$workspaceBucketDeletedCol is not null and #$finalBucketCreatedCol is null"
    )
    val configureTmpBucketTransferIam = selectMigrationsWhere(
      sql"#$finalBucketCreatedCol is not null and #$tmpBucketTransferIamConfiguredCol is null"
    )
    val issueTransferJobToFinalWorkspaceBucketCondition = selectMigrationsWhere(
      sql"#$tmpBucketTransferIamConfiguredCol is not null and #$tmpBucketTransferJobIssuedCol is null"
    )
    val deleteTemporaryBucketCondition = selectMigrationsWhere(
      sql"#$tmpBucketTransferredCol is not null and #$tmpBucketDeletedCol is null"
    )
    val restoreIamPoliciesAndUpdateWorkspaceRecordCondition = selectMigrationsWhere(
      sql"#$tmpBucketDeletedCol is not null"
    )

    def withMigrationId(migrationId: Long) = selectMigrationsWhere(sql"#$idCol = $migrationId")
  }

  object migrationRetryQuery
      extends RawTableQuery[Long]("V1_WORKSPACE_MIGRATION_RETRIES", primaryKey = ColumnName("ID")) {
    val idCol = primaryKey
    val migrationIdCol = ColumnName[Long]("MIGRATION_ID")
    val retriesCol = ColumnName[Long]("RETRIES")

    val allColumns: String = List(idCol, migrationIdCol, retriesCol).mkString(",")

    implicit private val getWorkspaceRetry = GetResult(r => MigrationRetry(r.<<, r.<<, r.<<))

    final def isPipelineBlocked(maxRetries: Int): ReadWriteAction[Boolean] =
      nextFailureLike(
        maxRetries,
        FailureModes.stsRateLimitedFailure,
        FailureModes.gcsUnavailableFailure
      ).isDefined

    final def nextFailureLike(maxRetries: Int, failureMessage: String, others: String*) =
      OptionT[ReadWriteAction, (Long, String)] {
        def matchesAtLeastOneFailureMessage = (migration: String) =>
          concatSqlActions(
            sql"(",
            reduceSqlActionsWithDelim(
              (failureMessage +: others).map(msg => sql"#$migration.message like $msg"),
              sql" or "
            ),
            sql")"
          )

        def noLaterMigrationAttemptExists = (migration: String) => sql"""not exists (
          select null from #${workspaceMigrationQuery.tableName}
          where workspace_id = #$migration.workspace_id and id > #$migration.id
        )"""

        def notExceededMaxRetries = (migration: String) => sql"""not exists (
          select null from #$tableName
          where #$migrationIdCol = #$migration.id and #$retriesCol >= $maxRetries
        )"""

        reduceSqlActionsWithDelim(
          Seq(
            sql"select m.id, CONCAT_WS('/', w.namespace, w.name) from #${workspaceMigrationQuery.tableName} m",
            sql"join (select id, namespace, name from WORKSPACE) as w on (w.id = m.workspace_id)",
            sql"where",
            reduceSqlActionsWithDelim(
              Seq(
                sql"m.outcome <=> 'Failure'",
                matchesAtLeastOneFailureMessage("m"),
                noLaterMigrationAttemptExists("m"),
                notExceededMaxRetries("m")
              ),
              sql" and "
            ),
            sql" order by m.updated limit 1"
          ),
          sql" "
        ).as[(Long, String)].headOption
      }

    final def getOrCreate(migrationId: Long): ReadWriteAction[MigrationRetry] =
      sqlu"insert ignore into #$tableName (#$migrationIdCol) values ($migrationId)" >>
        sql"select #$allColumns from #$tableName where #$migrationIdCol = $migrationId".as[MigrationRetry].head
  }

  case class RawTableQuery[PrimaryKey](tableName: String, primaryKey: ColumnName[PrimaryKey])(implicit
    setKey: SetParameter[PrimaryKey]
  ) {

    def update[A](key: PrimaryKey, columnName: ColumnName[A], value: A)(implicit
      setA: SetParameter[A]
    ): ReadWriteAction[Int] =
      sqlu"update #$tableName set #$columnName = $value where #$primaryKey = $key"

    def update2[A, B](key: PrimaryKey, columnName1: ColumnName[A], value1: A, columnName2: ColumnName[B], value2: B)(
      implicit
      setA: SetParameter[A],
      setB: SetParameter[B]
    ): ReadWriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1, #$columnName2 = $value2
        where #$primaryKey = $key
      """

    def update3[A, B, C](key: PrimaryKey,
                         columnName1: ColumnName[A],
                         value1: A,
                         columnName2: ColumnName[B],
                         value2: B,
                         columnName3: ColumnName[C],
                         value3: C
    )(implicit
      setA: SetParameter[A],
      setB: SetParameter[B],
      setC: SetParameter[C]
    ): ReadWriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1, #$columnName2 = $value2, #$columnName3 = $value3
        where #$primaryKey = $key
      """

    def update5[A, B, C, D, E](key: PrimaryKey,
                               columnName1: ColumnName[A],
                               value1: A,
                               columnName2: ColumnName[B],
                               value2: B,
                               columnName3: ColumnName[C],
                               value3: C,
                               columnName4: ColumnName[D],
                               value4: D,
                               columnName5: ColumnName[E],
                               value5: E
    )(implicit
      setA: SetParameter[A],
      setB: SetParameter[B],
      setC: SetParameter[C],
      setD: SetParameter[D],
      setE: SetParameter[E]
    ): ReadWriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1,
            #$columnName2 = $value2,
            #$columnName3 = $value3,
            #$columnName4 = $value4,
            #$columnName5 = $value5
        where #$primaryKey = $key
      """

    def update10[A, B, C, D, E, F, G, H, I, J](key: PrimaryKey,
                                               columnName1: ColumnName[A],
                                               value1: A,
                                               columnName2: ColumnName[B],
                                               value2: B,
                                               columnName3: ColumnName[C],
                                               value3: C,
                                               columnName4: ColumnName[D],
                                               value4: D,
                                               columnName5: ColumnName[E],
                                               value5: E,
                                               columnName6: ColumnName[F],
                                               value6: F,
                                               columnName7: ColumnName[G],
                                               value7: G,
                                               columnName8: ColumnName[H],
                                               value8: H,
                                               columnName9: ColumnName[I],
                                               value9: I,
                                               columnName10: ColumnName[J],
                                               value10: J
    )(implicit
      setA: SetParameter[A],
      setB: SetParameter[B],
      setC: SetParameter[C],
      setD: SetParameter[D],
      setE: SetParameter[E],
      setF: SetParameter[F],
      setG: SetParameter[G],
      setH: SetParameter[H],
      setI: SetParameter[I],
      setJ: SetParameter[J]
    ): ReadWriteAction[Int] =
      sqlu"""
        update #$tableName
        set #$columnName1 = $value1,
            #$columnName2 = $value2,
            #$columnName3 = $value3,
            #$columnName4 = $value4,
            #$columnName5 = $value5,
            #$columnName6 = $value6,
            #$columnName7 = $value7,
            #$columnName8 = $value8,
            #$columnName9 = $value9,
            #$columnName10 = $value10
        where #$primaryKey = $key
      """

    def delete: WriteAction[Int] =
      sqlu"""delete from #$tableName"""
  }
}

final case class ColumnName[-A](value: String) extends ValueObject
