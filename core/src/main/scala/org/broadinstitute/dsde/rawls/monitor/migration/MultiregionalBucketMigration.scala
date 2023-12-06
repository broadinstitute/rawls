package org.broadinstitute.dsde.rawls.monitor.migration

import akka.http.scaladsl.model.StatusCodes
import cats.MonadThrow
import cats.data.OptionT
import com.google.storagetransfer.v1.proto.TransferTypes.TransferOperation
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  GoogleProjectId,
  GoogleProjectNumber,
  SubmissionStatuses,
  Workspace,
  WorkspaceName,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.Success
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.{unsafeFromEither, Outcome}
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationStep.MultiregionalBucketMigrationStep
import org.broadinstitute.dsde.workbench.model.ValueObject
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.InstantFormat
import slick.jdbc.{GetResult, SQLActionBuilder, SetParameter}
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

final case class MultiregionalBucketMigrationMetadata(
  id: Long, // cardinal of migration attempts for the workspace and not the unique id of the record in the db
  created: Instant,
  started: Option[Instant],
  updated: Instant,
  finished: Option[Instant],
  outcome: Option[Outcome]
)

object MultiregionalBucketMigrationMetadata {
  def fromMultiregionalBucketMigration(m: MultiregionalBucketMigration,
                                       index: Int
  ): MultiregionalBucketMigrationMetadata =
    MultiregionalBucketMigrationMetadata(
      index.toLong,
      m.created.toInstant,
      m.started.map(_.toInstant),
      m.updated.toInstant,
      m.finished.map(_.toInstant),
      m.outcome
    )
}

final case class MultiregionalBucketMigration(id: Long,
                                              workspaceId: UUID,
                                              created: Timestamp,
                                              started: Option[Timestamp],
                                              updated: Timestamp,
                                              finished: Option[Timestamp],
                                              outcome: Option[Outcome],
                                              workspaceBucketIamRemoved: Option[Timestamp],
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
                                              requesterPaysEnabled: Boolean
)

final case class MultiregionalBucketMigrationRetry(id: Long, migrationId: Long, numRetries: Long)

object MultiregionalBucketMigrationFailureModes {
  // when issuing storage transfer jobs. caused by delays in propagating iam policy changes
  val noBucketPermissionsFailure: String =
    "%FAILED_PRECONDITION: Service account project-%@storage-transfer-service.iam.gserviceaccount.com " +
      "does not have required permissions%"

  // sts rates-limits us if we run too many concurrent jobs or exceed a threshold number of requests
  val stsRateLimitedFailure: String =
    "%RESOURCE_EXHAUSTED: Quota exceeded for quota metric 'Create requests' " +
      "and limit 'Create requests per day' of service 'storagetransfer.googleapis.com'%"

  // transfer operations fail midway
  val noObjectPermissionsFailure: String =
    "%PERMISSION_DENIED%project-%@storage-transfer-service.iam.gserviceaccount.com " +
      "does not have storage.objects.% access to the Google Cloud Storage%"

  val gcsUnavailableFailure: String =
    "%UNAVAILABLE:%Additional details: GCS is temporarily unavailable."

  val stsSANotFoundFailure: String =
    "%NOT_FOUND%project-%@storage-transfer-service.iam.gserviceaccount.com does not exist."
}

object MultiregionalBucketMigrationStep extends Enumeration {
  type MultiregionalBucketMigrationStep = Value
  val ScheduledForMigration, PreparingTransferToTempBucket, TransferringToTempBucket, PreparingTransferToFinalBucket,
    TransferringToFinalBucket, FinishingUp, Finished = Value

  def fromMultiregionalBucketMigration(migration: MultiregionalBucketMigration): MultiregionalBucketMigrationStep =
    migration match {
      case m if m.finished.isDefined                                             => Finished
      case m if m.tmpBucketTransferred.isDefined || m.tmpBucketDeleted.isDefined => FinishingUp
      case m if m.tmpBucketTransferJobIssued.isDefined                           => TransferringToFinalBucket
      case m
          if m.workspaceBucketTransferred.isDefined || m.workspaceBucketDeleted.isDefined || m.finalBucketCreated.isDefined || m.tmpBucketTransferIamConfigured.isDefined =>
        PreparingTransferToFinalBucket
      case m if m.workspaceBucketTransferJobIssued.isDefined => TransferringToTempBucket
      case m
          if m.started.isDefined || m.workspaceBucketIamRemoved.isDefined || m.tmpBucketCreated.isDefined || m.workspaceBucketTransferIamConfigured.isDefined =>
        PreparingTransferToTempBucket
      case _ => ScheduledForMigration
    }
}
final case class STSJobProgress(totalBytesToTransfer: Long,
                                bytesTransferred: Long,
                                totalObjectsToTransfer: Long,
                                objectsTransferred: Long
)

object STSJobProgress {
  def fromMultiregionalStorageTransferJob(
    jobOpt: Option[MultiregionalStorageTransferJob]
  ): Option[STSJobProgress] = jobOpt.collect { job =>
    (job.totalBytesToTransfer, job.bytesTransferred, job.totalObjectsToTransfer, job.objectsTransferred) match {
      case (Some(totalBytesToTransfer),
            Some(bytesTransferred),
            Some(totalObjectsToTransfer),
            Some(objectsTransferred)
          ) =>
        STSJobProgress(totalBytesToTransfer, bytesTransferred, totalObjectsToTransfer, objectsTransferred)
    }
  }

  def fromTransferOperation(operation: TransferOperation): Option[STSJobProgress] =
    for {
      op <- Option(operation)
      counters <- Option(op.getCounters)
      totalBytesToTransfer <- Option(counters.getBytesFoundFromSource)
      bytesTransferred <- Option(counters.getBytesCopiedToSink)
      totalObjectsToTransfer <- Option(counters.getObjectsFoundFromSource)
      objectsTransferred <- Option(counters.getObjectsCopiedToSink)
    } yield STSJobProgress(totalBytesToTransfer, bytesTransferred, totalObjectsToTransfer, objectsTransferred)
}

final case class MultiregionalBucketMigrationProgress(
  migrationStep: MultiregionalBucketMigrationStep,
  outcome: Option[Outcome],
  tempBucketTransferProgress: Option[STSJobProgress],
  finalBucketTransferProgress: Option[STSJobProgress]
)

object MultiregionalBucketMigrationJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val MultiregionalBucketMigrationDetailsJsonFormat = jsonFormat6(MultiregionalBucketMigrationMetadata.apply)

  implicit val STSJobProgressJsonFormat: RootJsonFormat[STSJobProgress] = jsonFormat4(STSJobProgress.apply)
  implicit object MultiregionalBucketMigrationStepJsonFormat extends RootJsonFormat[MultiregionalBucketMigrationStep] {
    override def write(obj: MultiregionalBucketMigrationStep) =
      JsString(obj.toString)

    override def read(json: JsValue): MultiregionalBucketMigrationStep = json match {
      case JsString(s) => MultiregionalBucketMigrationStep.withName(s)
      case _           => throw DeserializationException("unsupported migration step found")
    }
  }

  implicit val MultiregionalBucketMigrationProgressJsonFormat: RootJsonFormat[MultiregionalBucketMigrationProgress] =
    jsonFormat4(MultiregionalBucketMigrationProgress.apply)
}

trait MultiregionalBucketMigrationHistory extends DriverComponent with RawSqlQuery {
  this: WorkspaceComponent =>

  import driver.api._

  implicit def setMRBValueObject[A <: ValueObject]: SetParameter[A] =
    SetParameter((v, pp) => pp.setString(v.value))

  implicit def setMRBOptionValueObject[A <: ValueObject]: SetParameter[Option[A]] =
    SetParameter((v, pp) => pp.setStringOption(v.map(_.value)))

  def getMRBOption[T](f: String => T): GetResult[Option[T]] =
    GetResult(_.nextStringOption().map(f))

  implicit val getMRBGoogleProjectId = getMRBOption(GoogleProjectId)
  implicit val getMRBGoogleProjectNumber = getMRBOption(GoogleProjectNumber)
  implicit val getMRBGcsBucketName = getMRBOption(GcsBucketName)
  implicit val getMRBInstant = GetResult(_.nextTimestamp().toInstant)
  implicit val getMRBInstantOption = GetResult(_.nextTimestampOption().map(_.toInstant))

  object multiregionalBucketMigrationQuery
      extends RawMRBTableQuery[Long]("MULTIREGIONAL_BUCKET_MIGRATION_HISTORY", primaryKey = ColumnName("id")) {

    val idCol = primaryKey
    val workspaceIdCol = ColumnName[UUID]("WORKSPACE_ID")
    val createdCol = ColumnName[Timestamp]("CREATED")
    val startedCol = ColumnName[Option[Timestamp]]("STARTED")
    val updatedCol = ColumnName[Timestamp]("UPDATED")
    val finishedCol = ColumnName[Option[Timestamp]]("FINISHED")
    val outcomeCol = ColumnName[Option[String]]("OUTCOME")
    val workspaceBucketIamRemovedCol = ColumnName[Option[Timestamp]]("WORKSPACE_BUCKET_IAM_REMOVED")
    val messageCol = ColumnName[Option[String]]("MESSAGE")
    val tmpBucketCol = ColumnName[Option[GcsBucketName]]("TMP_BUCKET_NAME")
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

    /** this order matches what is expected by getMultiregionalBucketMigration below */
    private val allColumnsInOrder = List(
      idCol,
      workspaceIdCol,
      createdCol,
      startedCol,
      updatedCol,
      finishedCol,
      outcomeCol,
      messageCol,
      workspaceBucketIamRemovedCol,
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
      requesterPaysEnabledCol
    )

    private val allColumns = allColumnsInOrder.mkString(",")

    implicit val getOutcomeOption: GetResult[Option[Outcome]] =
      GetResult(r => unsafeFromEither(Outcome.fromFields(r.nextStringOption(), r.nextStringOption())))

    /** the order of elements in the result set is expected to match allColumnsInOrder above */
    implicit private val getMultiregionalBucketMigration = GetResult(r =>
      MultiregionalBucketMigration(r.<<,
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

    implicit private val getMultiregionalBucketMigrationDetails =
      GetResult(r => MultiregionalBucketMigrationMetadata(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    def setMigrationFinished(migrationId: Long, now: Timestamp, outcome: Outcome): ReadWriteAction[Int] = {
      val (status, message) = Outcome.toTuple(outcome)
      update3(migrationId, finishedCol, Some(now), outcomeCol, Some(status), messageCol, message)
    }

    final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
      sql"select count(*) from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} and #$startedCol is not null and #$finishedCol is null"
        .as[Int]
        .map(_.head > 0)

    final def scheduleAndGetMetadata
      : (Workspace, Option[String]) => ReadWriteAction[MultiregionalBucketMigrationMetadata] =
      schedule(_, _).flatMap(getMetadata)

    final def schedule(workspace: Workspace, location: Option[String]): ReadWriteAction[Long] =
      for {
        maybePastBucketMigration <- getAttempt(workspace.workspaceIdAsUUID).value
        id <- maybePastBucketMigration match {
          case None          => scheduleFirstMigrationAttempt(workspace, location)
          case Some(attempt) => restartMigrationAttempt(attempt, workspace.toWorkspaceName)
        }
      } yield id

    private def scheduleFirstMigrationAttempt(workspace: Workspace, location: Option[String]): ReadWriteAction[Long] =
      for {
        _ <- MonadThrow[ReadWriteAction].raiseWhen(workspace.isLocked) {
          new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest,
                        s"'${workspace.toWorkspaceName}' bucket cannot be migrated as it is locked."
            )
          )
        }

        _ <- MonadThrow[ReadWriteAction].raiseWhen(workspace.workspaceType == WorkspaceType.McWorkspace) {
          new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest,
                        s"'${workspace.toWorkspaceName}' bucket cannot be migrated as it is not a Google workspace."
            )
          )
        }

        _ <- MonadThrow[ReadWriteAction].raiseWhen(!location.getOrElse("").equals("US")) {
          new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              s"workspace ${workspace.toWorkspaceName} bucket ${workspace.bucketName} is not in the US multi-region and is therefore ineligible for migration"
            )
          )
        }

        _ <- sqlu"insert into #$tableName (#$workspaceIdCol) values (${workspace.workspaceIdAsUUID})"
        id <- sql"select LAST_INSERT_ID()".as[Long].head
      } yield id

    private def restartMigrationAttempt(pastAttempt: MultiregionalBucketMigration,
                                        workspaceName: WorkspaceName
    ): ReadWriteAction[Long] =
      for {
        _ <- MonadThrow[ReadWriteAction].raiseWhen(pastAttempt.outcome.getOrElse(Success).isSuccess) {
          new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              s"Workspace '$workspaceName' ${if (pastAttempt.outcome.isDefined) "has already had its bucket migrated successfully"
                else "is already pending bucket migration"}."
            )
          )
        }

        // If the last step completed was issuing the STS job, clear out the timestamp for the relevant
        // transferJobIssued col and the corresponding bucketTransferIamConfigured col for safe measure.
        // This will cause the pipeline to issue a new STS job after reconfiguring IAM. We have to do
        // this or the pipeline will just re-poll on the original failed STS job and the migration will
        // fail again.
        //
        // For all other steps in the migration, just clear out the finished, outcome, and message cols
        // so that the migration actor will begin migrating the workspace again at the step that failed.
        _ <- (pastAttempt.workspaceBucketTransferJobIssued,
              pastAttempt.workspaceBucketTransferred,
              pastAttempt.tmpBucketTransferJobIssued,
              pastAttempt.tmpBucketTransferred
        ) match {
          case (Some(_), None, _, _) =>
            // Last completed step was issuing the transfer job from original workspace bucket to the
            // temp bucket. Pipeline will reconfigure IAM and then issue a new transfer job
            multiregionalBucketMigrationQuery.update5(
              pastAttempt.id,
              finishedCol,
              Option.empty[Timestamp],
              messageCol,
              Option.empty[String],
              outcomeCol,
              Option.empty[String],
              workspaceBucketTransferIamConfiguredCol,
              Option.empty[Timestamp],
              workspaceBucketTransferJobIssuedCol,
              Option.empty[Timestamp]
            )
          case (_, _, Some(_), None) =>
            // Last completed step was issuing the transfer job from temp bucket to the new workspace
            // bucket. Pipeline will reconfigure IAM and then issue a new transfer job
            multiregionalBucketMigrationQuery.update5(
              pastAttempt.id,
              finishedCol,
              Option.empty[Timestamp],
              messageCol,
              Option.empty[String],
              outcomeCol,
              Option.empty[String],
              tmpBucketTransferIamConfiguredCol,
              Option.empty[Timestamp],
              tmpBucketTransferJobIssuedCol,
              Option.empty[Timestamp]
            )
          case _ =>
            // For all other steps, just clear out finishedCol, messageCol, and outcomeCol to restart
            // the migration from the failed step
            multiregionalBucketMigrationQuery.update3(pastAttempt.id,
                                                      finishedCol,
                                                      Option.empty[Timestamp],
                                                      messageCol,
                                                      Option.empty[String],
                                                      outcomeCol,
                                                      Option.empty[String]
            )
        }
      } yield pastAttempt.id

    final def getMetadata(migrationId: Long): ReadAction[MultiregionalBucketMigrationMetadata] =
      sql"""
        select b.normalized_id, a.#$createdCol, a.#$startedCol, a.#$updatedCol, a.#$finishedCol, a.#$outcomeCol, a.#$messageCol
        from #$tableName a
        join (select count(*) - 1 as normalized_id, max(#$idCol) as last_id from #$tableName group by #$workspaceIdCol) b
        on a.#$idCol = b.last_id
        where a.#$idCol = $migrationId
        """
        .as[MultiregionalBucketMigrationMetadata]
        .headOption
        .map(
          _.getOrElse(
            throw new NoSuchElementException(s"No bucket migration with id = '$migrationId'.'")
          )
        )

    final def getMigrationAttempts(workspace: Workspace): ReadAction[List[MultiregionalBucketMigration]] =
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = ${workspace.workspaceIdAsUUID} order by #$idCol"
        .as[MultiregionalBucketMigration]
        .map(_.toList)

    final def selectMigrationsWhere(conditions: SQLActionBuilder): ReadAction[Vector[MultiregionalBucketMigration]] = {
      val startingSql = sql"select #$allColumns from #$tableName where #$finishedCol is null and "
      concatSqlActions(startingSql, conditions, sql" order by #$updatedCol asc limit 1")
        .as[MultiregionalBucketMigration]
    }

    final def getAttempt(workspaceUuid: UUID) = OptionT[ReadWriteAction, MultiregionalBucketMigration] {
      sql"select #$allColumns from #$tableName where #$workspaceIdCol = $workspaceUuid order by #$idCol desc limit 1"
        .as[MultiregionalBucketMigration]
        .headOption
    }

    final def getAttempt(migrationId: Long) = OptionT[ReadWriteAction, MultiregionalBucketMigration] {
      sql"select #$allColumns from #$tableName where #$idCol = $migrationId".as[MultiregionalBucketMigration].headOption
    }

// Resource-limited migrations are those requiring new google projects and storage transfer jobs
    final def getNumActiveResourceLimitedMigrations: ReadWriteAction[Int] =
      sql"""
        select count(*) from #$tableName m
        join (select id, namespace from WORKSPACE) as w on (w.id = m.#$workspaceIdCol)
        where m.#$startedCol is not null and m.#$finishedCol is null
        """.as[Int].head

// The following query uses raw parameters. In this particular case it's safe to do as the
// values of the `activeStatuses` are known and controlled by us. In general one should use
// bind parameters for user input to avoid sql injection attacks.
    final def nextMigration() = OptionT[ReadWriteAction, (Long, UUID, String)] {
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
        sql"order by m.#$idCol limit 1"
      ).as[(Long, UUID, String)].headOption
    }

    def getWorkspaceName(migrationId: Long) = OptionT[ReadWriteAction, WorkspaceName] {
      sql"""
          select w.namespace, w.name from WORKSPACE w
          where w.id in (select #$workspaceIdCol from #$tableName m where m.id = $migrationId)
          """.as[(String, String)].headOption.map(_.map(WorkspaceName.tupled))
    }

    val removeWorkspaceBucketIamCondition = selectMigrationsWhere(
      sql"#$startedCol is not null and #$workspaceBucketIamRemovedCol is null"
    )
    val createTempBucketConditionCondition = selectMigrationsWhere(
      sql"#$workspaceBucketIamRemovedCol is not null and #$tmpBucketCreatedCol is null"
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
    val restoreIamPoliciesCondition = selectMigrationsWhere(
      sql"#$tmpBucketDeletedCol is not null"
    )

    def withMigrationId(migrationId: Long) = selectMigrationsWhere(sql"#$idCol = $migrationId")
  }

  object multiregionalBucketMigrationRetryQuery
      extends RawMRBTableQuery[Long]("MULTIREGIONAL_BUCKET_MIGRATION_RETRIES", primaryKey = ColumnName("ID")) {
    val idCol = primaryKey
    val migrationIdCol = ColumnName[Long]("MIGRATION_ID")
    val retriesCol = ColumnName[Long]("RETRIES")

    val allColumns: String = List(idCol, migrationIdCol, retriesCol).mkString(",")

    implicit private val getMultiregionalBucketMigrationRetry =
      GetResult(r => MultiregionalBucketMigrationRetry(r.<<, r.<<, r.<<))

    final def isPipelineBlocked(maxRetries: Int): ReadWriteAction[Boolean] =
      nextFailureLike(
        maxRetries,
        MultiregionalBucketMigrationFailureModes.stsRateLimitedFailure,
        MultiregionalBucketMigrationFailureModes.gcsUnavailableFailure
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
          select null from #${multiregionalBucketMigrationQuery.tableName}
          where workspace_id = #$migration.workspace_id and id > #$migration.id
        )"""

        def notExceededMaxRetries = (migration: String) => sql"""not exists (
          select null from #$tableName
          where #$migrationIdCol = #$migration.id and #$retriesCol >= $maxRetries
        )"""

        reduceSqlActionsWithDelim(
          Seq(
            sql"select m.id, CONCAT_WS('/', w.namespace, w.name) from #${multiregionalBucketMigrationQuery.tableName} m",
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

    final def getOrCreate(migrationId: Long): ReadWriteAction[MultiregionalBucketMigrationRetry] =
      sqlu"insert ignore into #$tableName (#$migrationIdCol) values ($migrationId)" >>
        sql"select #$allColumns from #$tableName where #$migrationIdCol = $migrationId"
          .as[MultiregionalBucketMigrationRetry]
          .head
  }

  case class RawMRBTableQuery[PrimaryKey](tableName: String, primaryKey: ColumnName[PrimaryKey])(implicit
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
