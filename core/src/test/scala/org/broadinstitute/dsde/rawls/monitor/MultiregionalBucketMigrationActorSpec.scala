package org.broadinstitute.dsde.rawls.monitor

import cats.data.{NonEmptyList, OptionT}
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.storage.{Acl, BucketInfo, Storage}
import com.google.storagetransfer.v1.proto.TransferTypes.{TransferJob, TransferOperation}
import io.grpc.{Status, StatusRuntimeException}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome._
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationActor._
import org.broadinstitute.dsde.rawls.monitor.migration.{FailureModes, MultiregionalStorageTransferJob}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceServiceSpec
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobName, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.google2.mock.{BaseFakeGoogleStorage, MockGoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageTransferService, StorageRole}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.scalactic.source
import org.scalatest.concurrent.Eventually
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues, Succeeded}
import slick.jdbc.MySQLProfile.api._
import spray.json.{JsObject, JsString}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.annotation.nowarn
import scala.language.postfixOps
import scala.util.Random

class MultiregionalBucketMigrationActorSpec extends AnyFlatSpecLike with Matchers with Eventually with OptionValues {

  implicit val logger = new ConsoleLogger("unit_test", LogLevel(false, false, true, true))
  implicit val ec = IORuntime.global.compute
  implicit val timestampOrdering = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
  implicit class FailureMessageOps(outcome: Outcome)(implicit pos: source.Position) {
    def failureMessage: String = outcome match {
      case Failure(message) => message
      case _ =>
        throw new TestFailedException(_ => Some(s"""Expected "Failure", instead got "${outcome}"."""), None, pos)
    }
  }

  // This is a horrible hack to avoid refactoring the tangled mess in the WorkspaceServiceSpec.
  val spec = new WorkspaceServiceSpec()

  object testData {
    val billingProject = spec.testData.billingProject.copy(
      projectName = RawlsBillingProjectName("test-billing-project")
    )

    val billingProject2 = billingProject.copy(
      projectName = RawlsBillingProjectName("another-test-billing-project")
    )

    val workspace = spec.testData.workspace.copy(
      namespace = billingProject.projectName.value,
      workspaceId = UUID.randomUUID().toString,
      googleProjectId = billingProject.googleProjectId
    )

    val workspace2 = workspace.copy(
      name = UUID.randomUUID().toString,
      workspaceId = UUID.randomUUID().toString
    )

    val workspace3 = workspace.copy(
      namespace = billingProject2.projectName.value,
      workspaceId = UUID.randomUUID().toString
    )
  }

  def runMigrationTest(test: MigrateAction[Assertion]): Assertion =
    spec.withEmptyTestDatabase { dataSource =>
      spec.withServices(dataSource, spec.testData.userOwner) { services =>
        services.gcsDAO
          .getServiceAccountUserInfo()
          .io
          .flatMap { userInfo =>
            (populateDb *> test)
              .run(
                MigrationDeps(
                  services.slickDataSource,
                  GoogleProject("fake-google-project"),
                  maxConcurrentAttempts = 0,
                  maxRetries = 1,
                  defaultBucketLocation = "us-central1",
                  services.workspaceServiceConstructor(RawlsRequestContext(userInfo)),
                  MockStorageService(),
                  MockStorageTransferService(),
                  userInfo,
                  services.gcsDAO,
                  new MockGoogleIamDAO,
                  services.samDAO
                )
              )
              .value
          }
          .unsafeRunSync
          .getOrElse(throw new AssertionError("The test exited prematurely."))
      }
    }

  def allowOne[A]: MigrateAction[A] => MigrateAction[A] =
    MigrateAction.local(_.copy(maxConcurrentAttempts = 1))

  case class MockStorageTransferService() extends MockGoogleStorageTransferService[IO] {
    override def getStsServiceAccount(project: GoogleProject): IO[ServiceAccount] =
      IO.pure {
        ServiceAccount(
          ServiceAccountSubjectId("fake-storage-transfer-service"),
          WorkbenchEmail("project-1234@storage-transfer-service.iam.gserviceaccount.com"),
          ServiceAccountDisplayName("Fake Google Storage Transfer Service")
        )
      }

    override def createTransferJob(jobName: JobName,
                                   jobDescription: String,
                                   projectToBill: GoogleProject,
                                   sourceBucket: GcsBucketName,
                                   destinationBucket: GcsBucketName,
                                   schedule: JobTransferSchedule,
                                   options: Option[GoogleStorageTransferService.JobTransferOptions]
    ): IO[TransferJob] =
      IO.pure {
        TransferJob.newBuilder
          .setName(s"$jobName")
          .setDescription(jobDescription)
          .setProjectId(s"$projectToBill")
          .build
      }

    override def getTransferJob(jobName: JobName, project: GoogleProject): IO[TransferJob] =
      IO.delay {
        val operationName =
          s"transferOperations/${jobName.value.replace("/", "-")}-" +
            s"${Random.nextInt(Integer.MAX_VALUE)}"
        TransferJob.newBuilder
          .setName(jobName.value)
          .setProjectId(project.value)
          .setStatus(TransferJob.Status.ENABLED)
          .setLatestOperationName(operationName)
          .build
      }

    override def getTransferOperation(
      operationName: GoogleStorageTransferService.OperationName
    ): IO[TransferOperation] =
      IO.delay {
        val now = Instant.now
        val timestamp = com.google.protobuf.Timestamp.newBuilder
          .setSeconds(now.getEpochSecond)
          .setNanos(now.getNano)
          .build
        val jobName = "(?:transferOperations/)(?<name>.+)(?=-\\d+$)".r
          .findFirstMatchIn(operationName.value)
          .value
          .group("name")
          .replaceFirst("-", "/")
        TransferOperation.newBuilder
          .setName(operationName.value)
          .setTransferJobName(jobName)
          .setProjectId("fake-google-project")
          .setStatus(TransferOperation.Status.SUCCESS)
          .setEndTime(timestamp)
          .build
      }
  }

  case class MockStorageService() extends BaseFakeGoogleStorage {
    override def getBucket(googleProject: GoogleProject,
                           bucketName: GcsBucketName,
                           bucketGetOptions: List[Storage.BucketGetOption],
                           traceId: Option[TraceId]
    ): IO[Option[BucketInfo]] =
      IO.pure(BucketInfo.newBuilder(bucketName.value).setRequesterPays(true).build().some)

    override def removeIamPolicy(bucketName: GcsBucketName,
                                 rolesToRemove: Map[StorageRole, NonEmptyList[Identity]],
                                 traceId: Option[TraceId],
                                 retryConfig: RetryConfig,
                                 bucketSourceOptions: List[Storage.BucketSourceOption]
    ): fs2.Stream[IO, Unit] =
      fs2.Stream.emit()
  }

  def populateDb: MigrateAction[Unit] =
    inTransaction(_.rawlsBillingProjectQuery.create(testData.billingProject)).void

  def createAndScheduleWorkspace(workspace: Workspace): ReadWriteAction[Long] =
    spec.workspaceQuery.createOrUpdate(workspace) *>
      spec.multiregionalBucketMigrationQuery.schedule(workspace.toWorkspaceName)

  def writeStarted(workspaceId: UUID): ReadWriteAction[Unit] =
    spec.multiregionalBucketMigrationQuery
      .getAttempt(workspaceId)
      .value
      .flatMap(_.traverse_ { attempt =>
        spec.multiregionalBucketMigrationQuery
          .update(attempt.id, spec.multiregionalBucketMigrationQuery.startedCol, Timestamp.from(Instant.now()).some)
      })

  "isMigrating" should "return false when a workspace is not being migrated" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(
        spec.multiregionalBucketMigrationQuery.isMigrating(spec.minimalTestData.v1Workspace)
      ) shouldBe false
    }

  "schedule" should "error when a workspace is scheduled concurrently" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(spec.multiregionalBucketMigrationQuery.schedule(spec.minimalTestData.v1Workspace.toWorkspaceName))
      assertThrows[RawlsExceptionWithErrorReport] {
        spec.runAndWait(
          spec.multiregionalBucketMigrationQuery.schedule(spec.minimalTestData.v1Workspace.toWorkspaceName)
        )
      }
    }

  it should "return normalized ids rather than real ids" in
    spec.withMinimalTestDatabase { _ =>
      import spec.minimalTestData
      import spec.multiregionalBucketMigrationQuery.{getAttempt, scheduleAndGetMetadata, setMigrationFinished}
      spec.runAndWait {
        for {
          a <- scheduleAndGetMetadata(minimalTestData.v1Workspace.toWorkspaceName)
          b <- scheduleAndGetMetadata(minimalTestData.v1Workspace2.toWorkspaceName)
          attempt <- getAttempt(minimalTestData.v1Workspace.workspaceIdAsUUID).value
          _ <- setMigrationFinished(attempt.value.id, Timestamp.from(Instant.now()), Success)
        } yield {
          a.id shouldBe 0
          b.id shouldBe 0
        }
      }
      spec.runAndWait(scheduleAndGetMetadata(minimalTestData.v1Workspace.toWorkspaceName)).id shouldBe 1
    }

  "updated" should "automagically get bumped to the current timestamp when the record is updated" in
    runMigrationTest {
      for {
        before <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.workspace)) *>
            dataAccess.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        }

        now <- nowTimestamp
        after <- inTransactionT { dataAccess =>
          OptionT.liftF(
            dataAccess.multiregionalBucketMigrationQuery.update(
              before.id,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketTransferIamConfiguredCol,
              now.some
            )
          ) *>
            dataAccess.multiregionalBucketMigrationQuery.getAttempt(before.id)
        }
      } yield before.updated should be < after.updated
    }

  "migrate" should "start a queued migration attempt and lock the workspace" in
    runMigrationTest {
      for {
        migrationId <- inTransaction { _ =>
          createAndScheduleWorkspace(testData.workspace)
        }

        _ <- migrate
        (attempt, workspace) <- inTransactionT { dataAccess =>
          for {
            attempt <- dataAccess.multiregionalBucketMigrationQuery.getAttempt(migrationId)
            workspace <- OptionT[ReadWriteAction, Workspace] {
              dataAccess.workspaceQuery.findById(testData.workspace.workspaceId)
            }
          } yield (attempt, workspace)
        }
      } yield {
        attempt.started shouldBe defined
        workspace.isLocked shouldBe true
      }
    }

  it should "not start more than the configured number of concurrent resource-limited migration attempts" in
    runMigrationTest {
      val workspaces = List(testData.workspace, testData.workspace2)
      @nowarn("msg=not.*?exhaustive")
      val test = for {
        _ <- inTransaction(_ => workspaces.traverse_(createAndScheduleWorkspace))
        _ <- allowOne(migrate *> migrate)
        Some(Seq(attempt1, attempt2)) <- inTransaction { dataAccess =>
          workspaces
            .traverse(w => dataAccess.multiregionalBucketMigrationQuery.getAttempt(w.workspaceIdAsUUID).value)
            .map(_.sequence)
        }
      } yield {
        attempt1.started shouldBe defined
        attempt2.started shouldBe empty
      }
      test
    }

  it should "start more than the configured number of concurrent only-child migration attempts" in
    runMigrationTest {
      for {
        _ <- inTransaction(_ => createAndScheduleWorkspace(testData.workspace))
        _ <- MigrateAction.local(_.copy(maxConcurrentAttempts = 0))(migrate)
        attempt <- inTransactionT {
          _.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        }
      } yield attempt.started shouldBe defined
    }

  it should "not start migrating a workspace with an active submission" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataAccess =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            dataAccess.methodConfigurationQuery.create(spec.testData.v1Workspace, spec.testData.agoraMethodConfig) >>
            dataAccess.entityQuery.save(
              spec.testData.v1Workspace,
              Seq(
                spec.testData.aliquot1,
                spec.testData.sample1,
                spec.testData.sample2,
                spec.testData.sample3,
                spec.testData.sset1,
                spec.testData.indiv1
              )
            ) >>
            dataAccess.submissionQuery.create(spec.testData.v1Workspace, spec.testData.submission1)(
              _ => spec.metrics.counter("test"),
              _ => None
            )
        }

        _ <- migrate
        attempt <- inTransactionT {
          _.multiregionalBucketMigrationQuery.getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield attempt.started shouldBe empty
    }

  it should "not start any new resource-limited migrations when transfer jobs are being rate-limited" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.multiregionalBucketMigrationQuery._
          for {
            _ <- dataAccess.rawlsBillingProjectQuery.create(testData.billingProject2)
            _ <- List(testData.workspace, testData.workspace2, testData.workspace3)
              .traverse_(createAndScheduleWorkspace)
            attempt <- getAttempt(testData.workspace.workspaceIdAsUUID).value
            _ <- attempt.traverse_ { a =>
              update(a.id, startedCol, Some(now)) *> setMigrationFinished(
                a.id,
                now,
                Failure(
                  "io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED: Quota exceeded for quota metric " +
                    "'Create requests' and limit 'Create requests per day' of service 'storagetransfer.googleapis.com' " +
                    "for consumer 'project_number:635957978953'."
                )
              )
            }
          } yield ()
        }

        _ <- migrate *> migrate

        (w2Attempt, w3Attempt) <- inTransactionT { dataAccess =>
          for {
            w2 <- dataAccess.multiregionalBucketMigrationQuery.getAttempt(testData.workspace2.workspaceIdAsUUID)
            w3 <- dataAccess.multiregionalBucketMigrationQuery.getAttempt(testData.workspace3.workspaceIdAsUUID)
          } yield (w2, w3)
        }

      } yield {
        w2Attempt.started shouldBe empty
        w3Attempt.started shouldBe defined // only-child workspaces are exempt
      }
    }

  it should "remove bucket permissions and record requester pays" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        migrationId <- inTransaction { dataAccess =>
          for {
            migrationId <- createAndScheduleWorkspace(testData.workspace)
            _ <- dataAccess.multiregionalBucketMigrationQuery.update(
              migrationId,
              dataAccess.multiregionalBucketMigrationQuery.startedCol,
              now.some
            )
          } yield migrationId
        }
        _ <- migrate

        attempt <- inTransactionT(_.multiregionalBucketMigrationQuery.getAttempt(migrationId))
      } yield {
        attempt.requesterPaysEnabled shouldBe true
        attempt.workspaceBucketIamRemoved shouldBe defined
      }
    }

  it should "fail the migration when there's an error on the workspace billing account" in
    runMigrationTest {
      val workspace = testData.workspace.copy(
        errorMessage = "oh noes :(".some,
        name = UUID.randomUUID.toString,
        workspaceId = UUID.randomUUID.toString
      )

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >>
            writeStarted(workspace.workspaceIdAsUUID)
        }

        _ <- migrate

        migration <- inTransactionT {
          _.multiregionalBucketMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("an error exists on workspace")
      }
    }

  it should "fail the migration when there's no billing account on the workspace" in
    runMigrationTest {
      val workspace = testData.workspace.copy(
        currentBillingAccountOnGoogleProject = None,
        name = UUID.randomUUID.toString,
        workspaceId = UUID.randomUUID.toString
      )

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >>
            writeStarted(workspace.workspaceIdAsUUID)
        }

        _ <- migrate

        migration <- inTransactionT { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("no billing account on workspace")
      }
    }

  it should "fail the migration when the billing account on the billing project is invalid" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataAccess =>
          import dataAccess.{executionContext => _, _}
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            _ <- writeStarted(testData.workspace.workspaceIdAsUUID)
            _ <- rawlsBillingProjectQuery
              .withProjectName(RawlsBillingProjectName(testData.workspace.namespace))
              .setInvalidBillingAccount(true)
          } yield ()
        }

        _ <- migrate

        migration <- inTransactionT {
          _.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("invalid billing account on billing project")
      }
    }

  it should "fail the migration when the billing account on the workspace does not match the billing account on the billing project" in
    runMigrationTest {
      val workspace = testData.workspace.copy(
        currentBillingAccountOnGoogleProject =
          testData.workspace.currentBillingAccountOnGoogleProject.map { billingAccount =>
            RawlsBillingAccountName(billingAccount.value ++ UUID.randomUUID.toString)
          },
        name = UUID.randomUUID.toString,
        workspaceId = UUID.randomUUID.toString
      )

      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >>
            writeStarted(workspace.workspaceIdAsUUID)
        }

        _ <- migrate
        migration <- inTransactionT { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include(
          "billing account on workspace differs from billing account on billing project"
        )
      }
    }

  it should "restart jobs when Gcs is unavailable" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            attempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            _ <- dataAccess.multiregionalBucketMigrationQuery.update(
              attempt.get.id,
              dataAccess.multiregionalBucketMigrationQuery.workspaceBucketIamRemovedCol,
              now.some
            )
          } yield ()
        }

        error = new StatusRuntimeException(
          Status.UNAVAILABLE.withDescription(
            "io.grpc.StatusRuntimeException: UNAVAILABLE: Failed to obtain the location " +
              s"of the GCS bucket ${testData.workspace.bucketName} " +
              "Additional details: GCS is temporarily unavailable."
          )
        )

        mockStorageService = new MockStorageService {
          override def insertBucket(googleProject: GoogleProject,
                                    bucketName: GcsBucketName,
                                    acl: Option[NonEmptyList[Acl]],
                                    labels: Map[String, String],
                                    traceId: Option[TraceId],
                                    bucketPolicyOnlyEnabled: Boolean,
                                    logBucket: Option[GcsBucketName],
                                    retryConfig: RetryConfig,
                                    location: Option[String],
                                    bucketTargetOptions: List[Storage.BucketTargetOption],
                                    autoclassEnabled: Boolean
          ): fs2.Stream[IO, Unit] =
            fs2.Stream.raiseError[IO](error)
        }

        _ <- MigrateAction.local(_.copy(storageService = mockStorageService))(migrate)
        _ <- inTransactionT {
          _.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        }.map { migration =>
          migration.finished shouldBe defined
          migration.outcome shouldBe Some(Failure(error.getMessage))
        }

        _ <- allowOne(restartFailuresLike(FailureModes.gcsUnavailableFailure))
        _ <- migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            retries <- dataAccess.multiregionalBucketMigrationRetryQuery.getOrCreate(migration.id)
          } yield {
            migration.finished shouldBe empty
            migration.outcome shouldBe empty
            migration.tmpBucketCreated shouldBe defined
            retries.numRetries shouldBe 1
          }
          test
        }
      } yield succeed
    }

  it should "issue configure the workspace and tmp bucket iam policies for storage transfer" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            attempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              attempt.get.id,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketCreatedCol,
              now.some,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        _ <- migrate
        migration <- inTransactionT(
          _.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        )
      } yield migration.workspaceBucketTransferIamConfigured shouldBe defined
    }

  it should "issue a storage transfer job from the workspace bucket to the tmp bucket" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            attempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              attempt.get.id,
              dataAccess.multiregionalBucketMigrationQuery.workspaceBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }
        _ <- migrate
        (migration, transferJob) <- inTransactionT { dataAccess =>
          for {
            migration <- dataAccess.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
            transferJob <- OptionT[ReadWriteAction, MultiregionalStorageTransferJob] {
              storageTransferJobs
                .filter(_.migrationId === migration.id)
                .take(1)
                .result
                .headOption
            }
          } yield (migration, transferJob)
        }
      } yield {
        transferJob.sourceBucket.value shouldBe testData.workspace.bucketName
        transferJob.destBucket.value shouldBe "tmp-bucket-name"
        migration.workspaceBucketTransferJobIssued shouldBe defined
      }
    }

  it should "re-issue a storage transfer job when it receives permissions precondition failures" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            attempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              attempt.get.id,
              dataAccess.multiregionalBucketMigrationQuery.workspaceBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        mockSts = new MockStorageTransferService {
          override def createTransferJob(jobName: JobName,
                                         jobDescription: String,
                                         projectToBill: GoogleProject,
                                         sourceBucket: GcsBucketName,
                                         destinationBucket: GcsBucketName,
                                         schedule: JobTransferSchedule,
                                         options: Option[GoogleStorageTransferService.JobTransferOptions]
          ) =
            getStsServiceAccount(projectToBill).flatMap { serviceAccount =>
              IO.raiseError(
                new StatusRuntimeException(
                  Status.FAILED_PRECONDITION.withDescription(
                    s"Service account ${serviceAccount.email} does not have required " +
                      "permissions {storage.objects.create, storage.objects.list} " +
                      s"for bucket $destinationBucket."
                  )
                )
              )
            }
        }

        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(migrate)
        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            transferJobs <- storageTransferJobs.filter(_.migrationId === migration.id).result
          } yield {
            transferJobs shouldBe empty
            migration.tmpBucketTransferJobIssued shouldBe empty
            migration.outcome shouldBe defined
          }
          test
        }

        _ <- allowOne(restartFailuresLike(FailureModes.noBucketPermissionsFailure))
        _ <- migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            Some(transferJob) <- storageTransferJobs.filter(_.migrationId === migration.id).result.headOption
          } yield {
            transferJob.sourceBucket.value shouldBe testData.workspace.bucketName
            transferJob.destBucket.value shouldBe "tmp-bucket-name"
            migration.workspaceBucketTransferJobIssued shouldBe defined
          }
          test
        }
      } yield succeed
    }

  it should "restart rate-limited transfer jobs after the configured amount of time has elapsed" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            attempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              attempt.get.id,
              dataAccess.multiregionalBucketMigrationQuery.workspaceBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        error = new StatusRuntimeException(
          Status.RESOURCE_EXHAUSTED.withDescription(
            "Quota exceeded for quota metric 'Create requests' and limit " +
              "'Create requests per day' of service 'storagetransfer.googleapis.com' " +
              "for consumer 'project_number:000000000000'."
          )
        )

        mockSts = new MockStorageTransferService {
          override def createTransferJob(jobName: JobName,
                                         jobDescription: String,
                                         projectToBill: GoogleProject,
                                         sourceBucket: GcsBucketName,
                                         destinationBucket: GcsBucketName,
                                         schedule: JobTransferSchedule,
                                         options: Option[GoogleStorageTransferService.JobTransferOptions]
          ) =
            IO.raiseError(error)
        }

        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(migrate)
        _ <- inTransactionT {
          _.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        }.map { migration =>
          migration.finished shouldBe defined
          migration.outcome shouldBe Some(Failure(error.getMessage))
        }

        _ <- allowOne(restartFailuresLike(FailureModes.stsRateLimitedFailure))
        _ <- migrate

        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(migration) <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            retries <- dataAccess.multiregionalBucketMigrationRetryQuery.getOrCreate(migration.id)
            Some(transferJob) <- storageTransferJobs.filter(_.migrationId === migration.id).result.headOption
          } yield {
            migration.finished shouldBe empty
            migration.outcome shouldBe empty
            migration.workspaceBucketTransferJobIssued shouldBe defined
            retries.numRetries shouldBe 1
            transferJob.sourceBucket.value shouldBe testData.workspace.bucketName
            transferJob.destBucket.value shouldBe "tmp-bucket-name"
          }
          test
        }
      } yield succeed
    }

  it should "not retry a failed migration when the maximum number of retries has been exceeded" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          for {
            _ <- createAndScheduleWorkspace(testData.workspace)
            attempt <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            _ <- dataAccess.multiregionalBucketMigrationQuery.update2(
              attempt.get.id,
              dataAccess.multiregionalBucketMigrationQuery.workspaceBucketTransferIamConfiguredCol,
              now.some,
              dataAccess.multiregionalBucketMigrationQuery.tmpBucketCol,
              GcsBucketName("tmp-bucket-name").some
            )
          } yield ()
        }

        mockSts = new MockStorageTransferService {
          override def createTransferJob(jobName: JobName,
                                         jobDescription: String,
                                         projectToBill: GoogleProject,
                                         sourceBucket: GcsBucketName,
                                         destinationBucket: GcsBucketName,
                                         schedule: JobTransferSchedule,
                                         options: Option[GoogleStorageTransferService.JobTransferOptions]
          ) =
            getStsServiceAccount(projectToBill).flatMap { serviceAccount =>
              IO.raiseError(
                new StatusRuntimeException(
                  Status.FAILED_PRECONDITION.withDescription(
                    s"Service account ${serviceAccount.email} does not have required " +
                      "permissions {storage.objects.create, storage.objects.list} " +
                      s"for bucket $destinationBucket."
                  )
                )
              )
            }
        }

        _ <- MigrateAction.local(_.copy(storageTransferService = mockSts))(migrate)

        before <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val update = for {
            Some(migration) <- dataAccess.multiregionalBucketMigrationQuery
              .getAttempt(testData.workspace.workspaceIdAsUUID)
              .value
            retry <- dataAccess.multiregionalBucketMigrationRetryQuery.getOrCreate(migration.id)
            _ <- dataAccess.multiregionalBucketMigrationRetryQuery.update(
              retry.id,
              dataAccess.multiregionalBucketMigrationRetryQuery.retriesCol,
              1L
            )
          } yield migration
          update
        }

        _ <- migrate
        _ <- inTransaction { dataAccess =>
          @nowarn("msg=not.*?exhaustive")
          val test = for {
            Some(after) <- dataAccess.multiregionalBucketMigrationQuery.getAttempt(before.id).value
            transferJobs <- storageTransferJobs.filter(_.migrationId === before.id).result
          } yield {
            transferJobs shouldBe empty
            after shouldBe before
          }
          test
        }
      } yield succeed
    }

  it should "not prevent a workspace from being deleted if the migration was retried" in
    runMigrationTest {
      for {
        _ <- inTransaction { dataAccess =>
          createAndScheduleWorkspace(testData.workspace) >>=
            dataAccess.multiregionalBucketMigrationRetryQuery.getOrCreate
        }
        wsService <- MigrateAction.asks(_.workspaceService)
        _ <- MigrateAction.fromFuture {
          wsService.deleteWorkspace(testData.workspace.toWorkspaceName)
        }
      } yield Succeeded
    }

  "issueBucketTransferJob" should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        // just need a unique migration id
        migration <- inTransactionT { dataAccess =>
          OptionT.liftF(createAndScheduleWorkspace(testData.workspace)) *>
            dataAccess.multiregionalBucketMigrationQuery.getAttempt(testData.workspace.workspaceIdAsUUID)
        }

        workspaceBucketName = GcsBucketName("workspace-bucket-name")
        tmpBucketName = GcsBucketName("tmp-bucket-name")
        job <- startBucketTransferJob(migration, testData.workspace, workspaceBucketName, tmpBucketName)
        transferJob <- inTransactionT { _ =>
          OptionT[ReadWriteAction, MultiregionalStorageTransferJob] {
            storageTransferJobs
              .filter(_.jobName === job.getName)
              .take(1)
              .result
              .headOption
          }
        }
      } yield {
        transferJob.jobName.value shouldBe job.getName
        transferJob.migrationId shouldBe migration.id
        transferJob.sourceBucket shouldBe workspaceBucketName
        transferJob.destBucket shouldBe tmpBucketName
      }
    }
}
