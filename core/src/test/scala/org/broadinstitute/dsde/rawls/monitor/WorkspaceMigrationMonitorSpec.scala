package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.cloud.storage.{Acl, Storage}
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.mock.{MockGoogleStorageService, MockGoogleStorageTransferService}
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationMonitor._
import org.broadinstitute.dsde.rawls.monitor.migration.{WorkspaceMigration, WorkspaceMigrationMonitor}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceServiceSpec
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobName, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, OptionValues}
import slick.jdbc.MySQLProfile.api._

import java.sql.{SQLException, Timestamp}
import java.time.LocalDateTime
import java.util.UUID
import scala.language.postfixOps

class WorkspaceMigrationMonitorSpec
  extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with Eventually
    with OptionValues {

  val testKit: ActorTestKit = ActorTestKit()
  implicit val logger = new ConsoleLogger("unit_test", LogLevel(false, false, true, true))

  // This is a horrible hack to avoid refactoring the tangled mess in the WorkspaceServiceSpec.
  val spec = new WorkspaceServiceSpec()


  def runMigrationTest(test: MigrateAction[Assertion]): Assertion = {
    spec.withTestDataServices { services =>
      test.run {
        WorkspaceMigrationMonitor.MigrationDeps(
          services.slickDataSource,
          spec.testData.billingProject,
          services.workspaceService,
          mockStorageService,
          mockStorageTransferService
        )
      }
        .value
        .unsafeRunSync
        .getOrElse(throw new AssertionError("The test exited prematurely."))
    }
  }


  val mockStorageTransferService = new MockGoogleStorageTransferService[IO] {
    override def createTransferJob(jobName: JobName, jobDescription: String, projectToBill: GoogleProject, originBucket: GcsBucketName, destinationBucket: GcsBucketName, schedule: JobTransferSchedule): IO[TransferJob] =
      IO.pure {
        TransferJob.newBuilder()
          .setName(s"${jobName}")
          .setDescription(jobDescription)
          .setProjectId(s"${projectToBill}")
          .build
      }
  }


  val mockStorageService = new MockGoogleStorageService[IO] {
    override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[Storage.BucketSourceOption], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Boolean] =
      fs2.Stream.emit(true)

    override def insertBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], labels: Map[String, String], traceId: Option[TraceId], bucketPolicyOnlyEnabled: Boolean, logBucket: Option[GcsBucketName], retryConfig: RetryConfig, location: Option[String]): fs2.Stream[IO, Unit] =
      fs2.Stream.emit(())
  }


  def getAttempt(workspaceUuid: UUID): MigrateAction[WorkspaceMigration] =
    WorkspaceMigrationMonitor.getMigrations(workspaceUuid).map(_.last)


  override def afterAll(): Unit = testKit.shutdownTestKit()


  "isMigrating" should "return false when a workspace is not being migrated" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(WorkspaceMigrationMonitor.isMigrating(spec.minimalTestData.v1Workspace)) shouldBe false
    }


  "schedule" should "error when a workspace is scheduled concurrently" in
    spec.withMinimalTestDatabase { _ =>
      spec.runAndWait(WorkspaceMigrationMonitor.schedule(spec.minimalTestData.v1Workspace)) shouldBe()
      assertThrows[SQLException] {
        spec.runAndWait(WorkspaceMigrationMonitor.schedule(spec.minimalTestData.v1Workspace))
      }
    }


  implicit val timestampOrdering = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
  }


  "updated" should "automagically get bumped to the current timestamp when the record is updated" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        }

        before <- WorkspaceMigrationMonitor
          .getMigrations(spec.testData.v1Workspace.workspaceIdAsUUID)
          .map(_.last)

        after <- WorkspaceMigrationMonitor.inTransaction { _ =>
          val migration = WorkspaceMigrationMonitor.workspaceMigrations
              .filter(_.id === before.id)

          migration
            .map(_.newGoogleProjectConfigured)
            .update(now.some) >> migration.result
        }
          .map(_.last)

      } yield before.updated should be < after.updated
    }


  "startMigration" should "update the migration record to be started" in
    runMigrationTest {
      for {
        _ <- inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            schedule(spec.testData.v1Workspace)
        }

        _ <- startMigration

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield migration.started shouldBe defined
    }


  "claimAndConfigureGoogleProject" should "return a valid database operation" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace) >>
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(_.started)
              .update(now.some)
        }

        _ <- claimAndConfigureGoogleProject

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield {
        migration.newGoogleProjectId shouldBe defined
        migration.newGoogleProjectNumber shouldBe defined
        migration.newGoogleProjectConfigured shouldBe defined
      }
    }


  // test is run manually until we figure out how to integration test without dockerising
  "createTempBucket" should "create a new bucket in the same region as the workspace bucket" ignore {
    val sourceProject = "general-dev-billing-account"
    val destProject = "terra-dev-7af423b8"

    val v1Workspace = spec.testData.v1Workspace.copy(
      workspaceId = UUID.randomUUID.toString,
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = "az-leotest"
    )

    val test = for {
      _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
        spec.workspaceQuery.createOrUpdate(v1Workspace) >>
          WorkspaceMigrationMonitor.schedule(v1Workspace) >>
          workspaceMigrations
            .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID)
            .map(m => (m.newGoogleProjectConfigured, m.newGoogleProjectId))
            .update((now.some, destProject.some))
      }

      _ <- createTempBucket

      migration <- getAttempt(v1Workspace.workspaceIdAsUUID)

      storageService <- MigrateAction.asks(_.storageService)
      bucket <- MigrateAction.liftIO {
        storageService.getBucket(GoogleProject(destProject), migration.tmpBucketName.get) <*
          storageService.deleteBucket(GoogleProject(destProject), migration.tmpBucketName.get, isRecursive = true).compile.drain
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
          googleStorageService => test.run(env.copy(storageService = googleStorageService)).value
        }
      }
    })
  }

  "issueWorkspaceBucketTransferJob" should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace) >>
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(m => (m.tmpBucketCreated, m.newGoogleProjectId, m.tmpBucket))
              .update((now.some, "new-google-project".some, "tmp-bucket-name".some))
        }

        job <- issueWorkspaceBucketTransferJob

        transferJob <- WorkspaceMigrationMonitor.inTransaction { _ =>
          WorkspaceMigrationMonitor.storageTransferJobs
            .filter(_.jobName === job.getName)
            .take(1)
            .result
        }
          .map(_.head)

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield {
        transferJob.jobName.value shouldBe job.getName
        transferJob.migrationId shouldBe migration.id
        transferJob.originBucket.value shouldBe spec.testData.v1Workspace.bucketName
        transferJob.destBucket.value shouldBe "tmp-bucket-name"
        migration.workspaceBucketTransferJobIssued shouldBe defined
      }
    }


  "deleteWorkspaceBucket" should "delete the workspace bucket and record when it was deleted" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace) >>
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(_.workspaceBucketTransferred)
              .update(now.some)
        }

        _ <- deleteWorkspaceBucket

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)

      } yield migration.workspaceBucketDeleted shouldBe defined
    }


  // test is run manually until we figure out how to integration test without dockerising
  "createFinalBucket" should "create a new bucket in the same region as the tmp workspace bucket" ignore {
    val destProject = "general-dev-billing-account"
    val dstBucketName = "migration-test-" + UUID.randomUUID.toString.replace("-", "")

    val v1Workspace = spec.testData.v1Workspace.copy(
      namespace = "test-namespace",
      workspaceId = UUID.randomUUID.toString,
      bucketName = dstBucketName
    )

    val test = for {
      _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
        spec.workspaceQuery.createOrUpdate(v1Workspace) >>
          WorkspaceMigrationMonitor.schedule(v1Workspace) >>
          workspaceMigrations
            .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID)
            .map(m => (m.workspaceBucketDeleted, m.newGoogleProjectId, m.tmpBucket))
            .update((now.some, destProject.some, "az-leotest".some))
      }

      _ <- createFinalWorkspaceBucket

      migration <- getAttempt(v1Workspace.workspaceIdAsUUID)

      storageService <- MigrateAction.asks(_.storageService)
      bucket <- MigrateAction.liftIO {
        storageService.getBucket(GoogleProject(destProject), GcsBucketName(dstBucketName)) <*
          storageService.deleteBucket(GoogleProject(destProject), GcsBucketName(dstBucketName), isRecursive = true).compile.drain
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
          googleStorageService => test.run(env.copy(storageService = googleStorageService)).value
        }
      }
    })
  }

  "issueTmpBucketTransferJob" should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace) >>
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(m => (m.finalBucketCreated, m.newGoogleProjectId, m.tmpBucket))
              .update((now.some, "new-google-project".some,"tmp-bucket-name".some))
        }

        job <- issueTmpBucketTransferJob

        transferJob <- WorkspaceMigrationMonitor.inTransaction { _ =>
          WorkspaceMigrationMonitor.storageTransferJobs
            .filter(_.jobName === job.getName)
            .take(1)
            .result
        }
          .map(_.head)

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield {
        transferJob.jobName.value shouldBe job.getName
        transferJob.migrationId shouldBe migration.id
        transferJob.originBucket.value shouldBe "tmp-bucket-name"
        transferJob.destBucket.value shouldBe spec.testData.v1Workspace.bucketName
        migration.tmpBucketTransferJobIssued shouldBe defined
      }
    }

  "deleteTemporaryBucket" should "delete the temporary bucket and record when it was deleted" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace) >>
            workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(m => (m.tmpBucketTransferred, m.newGoogleProjectId, m.tmpBucket))
              .update((now.some, "google-project-id".some, "tmp-bucket-name".some))
        }

        _ <- deleteTemporaryBucket

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield migration.tmpBucketDeleted shouldBe defined
    }

  def now: Timestamp = Timestamp.valueOf(LocalDateTime.now())

}

