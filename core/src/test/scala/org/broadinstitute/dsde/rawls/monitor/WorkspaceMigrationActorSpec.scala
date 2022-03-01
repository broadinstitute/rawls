package org.broadinstitute.dsde.rawls.monitor

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.services.compute.ComputeScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{Acl, Storage}
import com.google.cloud.{Identity, Policy}
import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.mock.{MockGoogleStorageService, MockGoogleStorageTransferService}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, UserInfo, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationActor._
import org.broadinstitute.dsde.rawls.monitor.migration.{PpwStorageTransferJob, WorkspaceMigration}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceServiceSpec
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobName, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.scalactic.source
import org.scalatest.Inspectors.forAll
import org.scalatest.concurrent.Eventually
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}
import slick.jdbc.MySQLProfile.api._
import spray.json.{JsObject, JsString}

import java.io.FileInputStream
import java.sql.{SQLException, Timestamp}
import java.util.{Collections, UUID}
import scala.language.postfixOps

class WorkspaceMigrationActorSpec
  extends AnyFlatSpecLike
    with Matchers
    with Eventually
    with OptionValues {

  implicit val logger = new ConsoleLogger("unit_test", LogLevel(false, false, true, true))
  implicit val ec = IORuntime.global.compute
  implicit val timestampOrdering = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }
  implicit class FailureMessageOps(outcome: Outcome)(implicit pos: source.Position) {
    def failureMessage: String = outcome match {
      case Failure(message) => message
      case _ => throw new TestFailedException(_ => Some(s"""Expected "Failure", instead got "${outcome}"."""), None, pos)
    }
  }

  // This is a horrible hack to avoid refactoring the tangled mess in the WorkspaceServiceSpec.
  val spec = new WorkspaceServiceSpec()

  val fakeGoogleProjectUsedForMigrationExpenses = GoogleProject("fake-google-project")


  def runMigrationTest(test: MigrateAction[Assertion]): Assertion =
    spec.withTestDataServices { services =>
      test.run {
        MigrationDeps(
          services.slickDataSource,
          fakeGoogleProjectUsedForMigrationExpenses,
          services.workspaceService,
          MockStorageService(),
          MockStorageTransferService(),
          services.samDAO,
          UserInfo(services.user.userEmail, OAuth2BearerToken("foo"), 0, services.user.userSubjectId)
        )
      }
        .value
        .unsafeRunSync
        .getOrElse(throw new AssertionError("The test exited prematurely."))
    }


  case class MockStorageTransferService() extends MockGoogleStorageTransferService[IO] {
    override def getStsServiceAccount(project: GoogleProject): IO[ServiceAccount] =
      IO.pure {
        ServiceAccount(
          ServiceAccountSubjectId("fake-storage-transfer-service"),
          WorkbenchEmail(s"fake-storage-transfer-service@${project}.iam.gserviceaccount.com"),
          ServiceAccountDisplayName("Fake Google Storage Transfer Service")
        )
      }

    override def createTransferJob(jobName: JobName, jobDescription: String, projectToBill: GoogleProject, originBucket: GcsBucketName, destinationBucket: GcsBucketName, schedule: JobTransferSchedule): IO[TransferJob] =
      IO.pure {
        TransferJob.newBuilder
          .setName(s"${jobName}")
          .setDescription(jobDescription)
          .setProjectId(s"${projectToBill}")
          .build
      }

    override def listTransferOperations(jobName: JobName, project: GoogleProject): IO[Seq[Operation]] =
      IO.pure {
        Seq(Operation.newBuilder.setDone(true).build)
      }
  }


  case class MockStorageService() extends MockGoogleStorageService[IO] {
    override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[Storage.BucketSourceOption], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Boolean] =
      fs2.Stream.emit(true)

    override def insertBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], labels: Map[String, String], traceId: Option[TraceId], bucketPolicyOnlyEnabled: Boolean, logBucket: Option[GcsBucketName], retryConfig: RetryConfig, location: Option[String]): fs2.Stream[IO, Unit] =
      fs2.Stream.emit()

    override def getIamPolicy(bucketName: GcsBucketName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Policy] =
      fs2.Stream.emit(Policy.newBuilder.build)

    override def setIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] =
      fs2.Stream.emit()

    override def overrideIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Policy] =
      fs2.Stream.emit(Policy.newBuilder.build)
  }


  def getAttempt(workspaceUuid: UUID): ReadWriteAction[Option[WorkspaceMigration]] =
    workspaceMigrations
      .filter(_.workspaceId === workspaceUuid)
      .sortBy(_.id.desc)
      .take(1)
      .result
      .headOption


  def createAndScheduleWorkspace(workspace: Workspace): ReadWriteAction[Unit] =
    spec.workspaceQuery.createOrUpdate(workspace) >> schedule(workspace)


  "isMigrating" should "return false when a workspace is not being migrated" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(isMigrating(spec.minimalTestData.v1Workspace)) shouldBe false
    }


  "schedule" should "error when a workspace is scheduled concurrently" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(schedule(spec.minimalTestData.v1Workspace)) shouldBe()
      assertThrows[SQLException] {
        spec.runAndWait(schedule(spec.minimalTestData.v1Workspace))
      }
    }


  "updated" should "automagically get bumped to the current timestamp when the record is updated" in
    runMigrationTest {
      for {
        before <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        now <- nowTimestamp
        after <- inTransactionT { _ =>
          val migration = workspaceMigrations
            .filter(_.id === before.id)

          migration
            .map(_.newGoogleProjectConfigured)
            .update(now.some) >> migration.result.headOption
        }
      } yield before.updated should be < after.updated
    }


  "migrate" should "start a queued migration attempt" in
    runMigrationTest {
      for {
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace)
        }

        _ <- migrate
        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield migration.started shouldBe defined
    }


  it should "claim and configure a fresh google project when a migration has been started" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >> workspaceMigrations
            .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
            .map(_.started)
            .update(now.some)
        }

        _ <- migrate

        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.newGoogleProjectId shouldBe defined
        migration.newGoogleProjectNumber shouldBe defined
        migration.newGoogleProjectConfigured shouldBe defined
      }
    }


  it should "fail the migration when there's an error on the workspace billing account" in
      runMigrationTest {
        for {
          now <- nowTimestamp
          workspace = spec.testData.v1Workspace.copy(
            billingAccountErrorMessage = "oh noes :(".some,
            name = UUID.randomUUID.toString,
            workspaceId = UUID.randomUUID.toString
          )

          _ <- inTransaction { _ =>
            createAndScheduleWorkspace(workspace) >> workspaceMigrations
              .filter(_.workspaceId === workspace.workspaceIdAsUUID)
              .map(_.started)
              .update(now.some)
          }

          _ <- migrate

          migration <- inTransactionT { _ =>
            getAttempt(workspace.workspaceIdAsUUID)
          }
        } yield {
          migration.finished shouldBe defined
          migration.outcome.value.failureMessage should include("billing account error exists on workspace")
        }
      }


  it should "fail the migration when there's no billing account on the workspace" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        workspace = spec.testData.v1Workspace.copy(
          currentBillingAccountOnGoogleProject = None,
          name = UUID.randomUUID.toString,
          workspaceId = UUID.randomUUID.toString
        )

        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >> workspaceMigrations
            .filter(_.workspaceId === workspace.workspaceIdAsUUID)
            .map(_.started)
            .update(now.some)
        }

        _ <- migrate

        migration <- inTransactionT { _ =>
          getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("no billing account on workspace")
      }
    }


  it should "fail the migration when the billing account on the billing project is invalid" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          DBIO.seq(
            createAndScheduleWorkspace(spec.testData.v1Workspace),
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(_.started)
              .update(now.some),
            dataAccess
              .rawlsBillingProjectQuery
              .filter(_.projectName === spec.testData.v1Workspace.namespace)
              .map(_.invalidBillingAccount)
              .update(true)
          )
        }

        _ <- migrate

        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("invalid billing account on billing project")
      }
    }


  it should "fail the migration when the billing account on the workspace does not match the billing account on the billing project" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        workspace = spec.testData.v1Workspace.copy(
          currentBillingAccountOnGoogleProject =
            spec.testData.v1Workspace.currentBillingAccountOnGoogleProject.map { billingAccount =>
              RawlsBillingAccountName(billingAccount.value ++ UUID.randomUUID.toString)
            },
          name = UUID.randomUUID.toString,
          workspaceId = UUID.randomUUID.toString
        )

        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(workspace) >> workspaceMigrations
            .filter(_.workspaceId === workspace.workspaceIdAsUUID)
            .map(_.started)
            .update(now.some)
        }

        _ <- migrate
        migration <- inTransactionT { _ =>
          getAttempt(workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome.value.failureMessage should include("billing account on workspace differs from billing account on billing project")
      }
    }


  // test is run manually until we figure out how to integration test without dockerising
  it should "create a new bucket in the same region as the workspace bucket" ignore {
    val sourceProject = "general-dev-billing-account"
    val destProject = "terra-dev-7af423b8"

    val v1Workspace = spec.testData.v1Workspace.copy(
      workspaceId = UUID.randomUUID.toString,
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = "az-leotest"
    )

    val test = for {
      now <- nowTimestamp
      _ <- inTransaction { _ =>
        createAndScheduleWorkspace(v1Workspace) >> workspaceMigrations
          .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID)
          .map(m => (m.newGoogleProjectConfigured, m.newGoogleProjectId))
          .update((now.some, destProject.some))
      }

      _ <- migrate

      migration <- inTransactionT { _ =>
        getAttempt(v1Workspace.workspaceIdAsUUID)
      }

      storageService <- MigrateAction.asks(_.storageService)
      bucket <- MigrateAction.liftIO {
        storageService.getBucket(GoogleProject(destProject), migration.tmpBucketName.get) <*
          storageService
            .deleteBucket(GoogleProject(destProject), migration.tmpBucketName.get, isRecursive = true)
            .compile
            .drain
      }
    } yield {
      bucket shouldBe defined
      migration.tmpBucketCreated shouldBe defined
    }

    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"

    runMigrationTest(ReaderT { env =>
      OptionT {
        GoogleStorageService.resource[IO](pathToCredentialJson, None, serviceProject.some).use {
          googleStorageService => test.run(
            env.copy(
              googleProjectToBill = serviceProject,
              storageService = googleStorageService,
              userInfo = UserInfo.buildFromTokens(
                ServiceAccountCredentials
                  .fromStream(new FileInputStream(pathToCredentialJson))
                  .createScoped(Collections.singleton(ComputeScopes.CLOUD_PLATFORM))
              )
            )
          ).value
        }
      }
    })
  }


  it should "issue a storage transfer job from the workspace bucket to the tmp bucket" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >> workspaceMigrations
            .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
            .map(m => (m.tmpBucketCreated, m.newGoogleProjectId, m.tmpBucket))
            .update((now.some, "new-google-project".some, "tmp-bucket-name".some))
        }

        _ <- migrate
        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
        transferJob <- inTransactionT { _ =>
          storageTransferJobs
            .filter(_.migrationId === migration.id)
            .take(1)
            .result
            .headOption
        }
      } yield {
        transferJob.originBucket.value shouldBe spec.testData.v1Workspace.bucketName
        transferJob.destBucket.value shouldBe "tmp-bucket-name"
        migration.workspaceBucketTransferJobIssued shouldBe defined
      }
    }


  it should "delete the workspace bucket and record when it was deleted" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >> workspaceMigrations
            .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
            .map(_.workspaceBucketTransferred)
            .update(now.some)
        }

        _ <- migrate
        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield migration.workspaceBucketDeleted shouldBe defined
    }


  // test is run manually until we figure out how to integration test without dockerising
  it should "create a new bucket in the same region as the tmp workspace bucket" ignore {
    val destProject = "general-dev-billing-account"
    val dstBucketName = "migration-test-" + UUID.randomUUID.toString.replace("-", "")

    val v1Workspace = spec.testData.v1Workspace.copy(
      namespace = "test-namespace",
      workspaceId = UUID.randomUUID.toString,
      bucketName = dstBucketName
    )

    val test = for {
      now <- nowTimestamp
      _ <- inTransaction { _ =>
        createAndScheduleWorkspace(v1Workspace) >> workspaceMigrations
          .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID)
          .map(m => (m.workspaceBucketDeleted, m.newGoogleProjectId, m.tmpBucket))
          .update((now.some, destProject.some, "az-leotest".some))
      }

      _ <- migrate
      migration <- inTransactionT { _ =>
        getAttempt(v1Workspace.workspaceIdAsUUID)
      }
      storageService <- MigrateAction.asks(_.storageService)
      bucket <- MigrateAction.liftIO {
        storageService.getBucket(GoogleProject(destProject), GcsBucketName(dstBucketName)) <*
          storageService
            .deleteBucket(GoogleProject(destProject), GcsBucketName(dstBucketName), isRecursive = true)
            .compile
            .drain
      }
    } yield {
      bucket shouldBe defined
      migration.finalBucketCreated shouldBe defined
    }

    val serviceProject = GoogleProject(destProject)
    val pathToCredentialJson = "config/rawls-account.json"

    runMigrationTest(ReaderT { env =>
      OptionT {
        GoogleStorageService.resource[IO](pathToCredentialJson, None, serviceProject.some).use {
          googleStorageService => test.run(
            env.copy(
              googleProjectToBill = serviceProject,
              storageService = googleStorageService,
              userInfo = UserInfo.buildFromTokens(
                ServiceAccountCredentials
                  .fromStream(new FileInputStream(pathToCredentialJson))
                  .createScoped(Collections.singleton(ComputeScopes.CLOUD_PLATFORM))
              )
            )
          ).value
        }
      }
    })
  }


  it should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >> workspaceMigrations
            .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
            .map(m => (m.finalBucketCreated, m.newGoogleProjectId, m.tmpBucket))
            .update((now.some, "new-google-project".some, "tmp-bucket-name".some))
        }

        _ <- migrate
        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
        transferJob <- inTransactionT { _ =>
          storageTransferJobs
            .filter(_.migrationId === migration.id)
            .take(1)
            .result
            .headOption
        }
      } yield {
        transferJob.originBucket.value shouldBe "tmp-bucket-name"
        transferJob.destBucket.value shouldBe spec.testData.v1Workspace.bucketName
        migration.tmpBucketTransferJobIssued shouldBe defined
      }
    }


  it should "delete the temporary bucket and record when it was deleted" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >> workspaceMigrations
            .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
            .map(m => (m.tmpBucketTransferred, m.newGoogleProjectId, m.tmpBucket))
            .update((now.some, "google-project-id".some, "tmp-bucket-name".some))
        }

        _ <- migrate

        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield migration.tmpBucketDeleted shouldBe defined
    }


  it should "update the Workspace record after the temporary bucket has been deleted" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        googleProjectId = GoogleProjectId("whatever dude")
        googleProjectNumber = GoogleProjectNumber("abc123")
        _ <- inTransaction { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >> workspaceMigrations
            .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
            .map(m => (m.tmpBucketDeleted, m.newGoogleProjectId, m.newGoogleProjectNumber))
            .update((now.some, googleProjectId.value.some, googleProjectNumber.value.some))
        }

        _ <- migrate

        workspace <- getWorkspace(spec.testData.v1Workspace.workspaceIdAsUUID)
        migration <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        migration.finished shouldBe defined
        migration.outcome shouldBe Success.some
        workspace.googleProjectId shouldBe googleProjectId
        workspace.googleProjectNumber shouldBe googleProjectNumber.some
      }
    }


  "startBucketTransferJob" should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        // just need a unique migration id
        migration <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        workspaceBucketName = GcsBucketName("workspace-bucket-name")
        tmpBucketName = GcsBucketName("tmp-bucket-name")
        job <- startBucketTransferJob(migration, spec.testData.v1Workspace, workspaceBucketName, tmpBucketName)
        transferJob <- inTransactionT { _ =>
          storageTransferJobs
            .filter(_.jobName === job.getName)
            .take(1)
            .result
            .headOption
        }
      } yield {
        transferJob.jobName.value shouldBe job.getName
        transferJob.migrationId shouldBe migration.id
        transferJob.originBucket shouldBe workspaceBucketName
        transferJob.destBucket shouldBe tmpBucketName
      }
    }


  "peekTransferJob" should "return the first active job that was updated last and touch it" in
    runMigrationTest {
      for {
        migration <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- startBucketTransferJob(migration, spec.testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
        job <- peekTransferJob
      } yield job.updated should be > job.created
    }


  it should "ignore finished jobs" in
    runMigrationTest {
      for {
        migration <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        job <- startBucketTransferJob(migration, spec.testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
        finished <- nowTimestamp
        _ <- inTransaction { _ =>
          storageTransferJobs
            .filter(_.jobName === job.getName)
            .map(_.finished)
            .update(finished.some)
        }

        job <- peekTransferJob.mapF { optionT => OptionT(optionT.value.map(_.some)) }
      } yield job should not be defined
    }


  "refreshTransferJobs" should "update the state of storage transfer jobs" in
    runMigrationTest {
      for {
        migration <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- startBucketTransferJob(migration, spec.testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
        transferJob <- refreshTransferJobs
      } yield {
        transferJob.migrationId shouldBe migration.id
        transferJob.finished shouldBe defined
        transferJob.outcome.value shouldBe Outcome.Success
      }
    }


  it should "update the state of jobs in order of last updated" in
    runMigrationTest {
      val storageTransferService = new MockStorageTransferService {
        // want to return no operations to make sure that the job does not complete and is
        // updated continually
        override def listTransferOperations(jobName: JobName, project: GoogleProject): IO[Seq[Operation]] =
          IO.pure(Seq(Operation.newBuilder.build))
      }

      MigrateAction.local(_.copy(storageTransferService = storageTransferService)) {
        for {
          migration1 <- inTransactionT { _ =>
            createAndScheduleWorkspace(spec.testData.v1Workspace) >>
              getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
          }

          migration2 <- inTransactionT { _ =>
            createAndScheduleWorkspace(spec.testData.workspace) >>
              getAttempt(spec.testData.workspace.workspaceIdAsUUID)
          }

          _ <- startBucketTransferJob(migration1, spec.testData.v1Workspace, GcsBucketName("foo"), GcsBucketName("bar"))
          _ <- startBucketTransferJob(migration2, spec.testData.workspace, GcsBucketName("foo"), GcsBucketName("bar"))

          getTransferJobs = inTransaction { _ =>
            storageTransferJobs
              .sortBy(_.id.asc)
              .result
              .map(_.toList)
          }

          transferJobsBefore <- getTransferJobs

          _ <- runStep(refreshTransferJobs *> MigrateAction.unit)
          transferJobsMid <- getTransferJobs

          _ <- runStep(refreshTransferJobs *> MigrateAction.unit)
          transferJobsAfter <- getTransferJobs
        } yield {
          forAll(transferJobsBefore) { job => job.finished should not be defined }

          // the first job created should be updated first
          transferJobsMid(0).updated should be > transferJobsBefore(0).updated
          transferJobsMid(1).updated shouldBe transferJobsBefore(1).updated

          // the second job should be updated next as it was updated the longest time ago
          transferJobsAfter(0).updated shouldBe transferJobsMid(0).updated
          transferJobsAfter(1).updated should be > transferJobsMid(1).updated
        }
      }
    }


  def storageTransferJobForTesting = new PpwStorageTransferJob(
    id = -1,
    jobName = null,
    migrationId = -1,
    created = null,
    updated = null,
    destBucket = null,
    originBucket = null,
    finished = null,
    outcome = null
  )


  "updateMigrationTransferJobStatus" should "update WORKSPACE_BUCKET_TRANSFERRED on job success" in
    runMigrationTest {
      for {
        before <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- updateMigrationTransferJobStatus(
          storageTransferJobForTesting.copy(
            migrationId = before.id,
            destBucket = GcsBucketName("tmp-bucket-name"),
            originBucket = GcsBucketName("workspace-bucket"),
            outcome = Success.some
          )
        )

        after <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        after.workspaceBucketTransferred shouldBe defined
        after.tmpBucketTransferred should not be defined
      }
    }


  it should "update TMP_BUCKET_TRANSFERRED on job success" in
    runMigrationTest {
      for {
        now <- nowTimestamp
        before <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(_.workspaceBucketTransferred)
              .update(now.some) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        _ <- updateMigrationTransferJobStatus(
          storageTransferJobForTesting.copy(
            migrationId = before.id,
            originBucket = GcsBucketName("workspace-bucket"),
            destBucket = GcsBucketName("tmp-bucket-name"),
            outcome = Success.some
          )
        )

        after <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        after.workspaceBucketTransferred shouldBe defined
        after.tmpBucketTransferred shouldBe defined
      }
    }


  it should "fail the migration on job failure" in
    runMigrationTest {
      for {
        before <- inTransactionT { _ =>
          createAndScheduleWorkspace(spec.testData.v1Workspace) >>
            getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }

        failure = Failure("oh noes :(")
        _ <- updateMigrationTransferJobStatus(
          storageTransferJobForTesting.copy(migrationId = before.id, outcome = failure.some)
        )

        after <- inTransactionT { _ =>
          getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
        }
      } yield {
        after.finished shouldBe defined
        after.outcome shouldBe failure.some
      }
    }

  "Outcome" should "have json support for Success" in {
    val jsSuccess = outcomeJsonFormat.write(Success)
    jsSuccess shouldBe JsObject("type" -> JsString("success"))
    outcomeJsonFormat.read(jsSuccess) shouldBe Success
  }

  it should "have json support for Failure" in {
    val message = UUID.randomUUID.toString
    val jsFailure = outcomeJsonFormat.write(Failure(message))
    jsFailure shouldBe JsObject("type" -> JsString("failure"), "message" -> JsString(message))
    outcomeJsonFormat.read(jsFailure) shouldBe Failure(message)
  }

}

