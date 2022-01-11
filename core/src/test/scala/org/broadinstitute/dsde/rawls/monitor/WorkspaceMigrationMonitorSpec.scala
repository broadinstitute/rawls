package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.data.{NonEmptyList, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.google.cloud.storage.{Acl, Storage}
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.mock.{MockGoogleStorageService, MockGoogleStorageTransferService}
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationStatus._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationMonitor.MigrationDeps
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


  def runMigrationTest(test: ReaderT[IO, MigrationDeps, Assertion]): Assertion =
    spec.withTestDataServices { services =>
      test.run {
        WorkspaceMigrationMonitor.MigrationDeps(
          services.slickDataSource,
          spec.testData.billingProject,
          services.workspaceService,
          mockStorageService,
          mockStorageTransferService
        )
      }.unsafeRunSync
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


  def getAttempt(workspaceUuid: UUID): ReaderT[IO, MigrationDeps, WorkspaceMigration] =
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


  "claimAndConfigureGoogleProject" should "return a valid database operation" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        }

        _ <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID).flatMap {
          WorkspaceMigrationMonitor.claimAndConfigureNewGoogleProject(_, spec.testData.v1Workspace)
        }

        attempt <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)

      } yield {
        attempt.newGoogleProjectId shouldBe defined
        attempt.newGoogleProjectNumber shouldBe defined
        attempt.newGoogleProjectConfigured shouldBe defined
      }
    }


  // use an existing test project (broad-dsde-dev)
  "createTempBucket" should "create a new bucket in the same region as the workspace bucket" ignore {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"

    val v1Workspace = spec.testData.v1Workspace.copy(
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = sourceBucket
    )

    val test = for {
      _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
        spec.workspaceQuery.createOrUpdate(v1Workspace) >>
          WorkspaceMigrationMonitor.schedule(v1Workspace)
      }

      bucketName <- getAttempt(v1Workspace.workspaceIdAsUUID).flatMap { migration =>
        WorkspaceMigrationMonitor.createTempBucket(migration, v1Workspace, GoogleProjectId(destProject))
      }

      bucket <- ReaderT { env: MigrationDeps =>
        env.storageService.getBucket(GoogleProject(destProject), bucketName) <*
          env.storageService.deleteBucket(GoogleProject(destProject), bucketName, isRecursive = true).compile.drain
      }

      attempt <- getAttempt(v1Workspace.workspaceIdAsUUID)

    } yield {
      bucket shouldBe defined
      attempt.tmpBucketName shouldBe bucketName.some
      attempt.tmpBucketCreated shouldBe defined
    }

    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"

    runMigrationTest(ReaderT { env =>
      GoogleStorageService.resource[IO](pathToCredentialJson, None, serviceProject.some).use {
        googleStorageService => test.run(env.copy(storageService = googleStorageService))
      }
    })
  }


  "deleteWorkspaceBucket" should "delete the workspace bucket and record when it was deleted" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        }

        _ <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID).flatMap {
          WorkspaceMigrationMonitor.deleteWorkspaceBucket(_, spec.testData.v1Workspace)
        }

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield migration.workspaceBucketDeleted shouldBe defined
    }


  "deleteTemporaryBucket" should "delete the temporary bucket and record when it was deleted" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        }

        _ <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID).flatMap { attempt =>
          WorkspaceMigrationMonitor.deleteTemporaryBucket(
            attempt.copy(
              newGoogleProjectId = GoogleProjectId("new-google-project-id").some,
              tmpBucketName = GcsBucketName("tmp-bucket-name").some
            )
          )
        }

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield migration.tmpBucketDeleted shouldBe defined
    }


  "startBucketStorageTransferJob" should "create and start a storage transfer job between the specified buckets" in
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        }

        job <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID).flatMap { attempt =>
          WorkspaceMigrationMonitor.startBucketStorageTransferJob(
            attempt,
            GcsBucketName(spec.testData.v1Workspace.bucketName),
            GcsBucketName("tmp-bucket-name")
          )
        }

        transferJob <- WorkspaceMigrationMonitor.inTransaction { _ =>
          WorkspaceMigrationMonitor.storageTransferJobs
            .filter(_.jobName === job.getName)
            .result
        }
          .map(_.head)

        attempt <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)

      } yield {
        transferJob.jobName.value shouldBe job.getName
        transferJob.migrationId shouldBe attempt.id
        transferJob.originBucket.value shouldBe spec.testData.v1Workspace.bucketName
        transferJob.destBucket shouldBe GcsBucketName("tmp-bucket-name")
      }
    }


  "createFinalBucket" should "create a new bucket in the same region as the tmp workspace bucket" ignore {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val destBucket = "v1-migration-test-" + UUID.randomUUID.toString.replace("-", "")

    val v1Workspace = spec.testData.v1Workspace.copy(
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = destBucket
    )

    val test = for {
      _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
        spec.workspaceQuery.createOrUpdate(v1Workspace) >>
          WorkspaceMigrationMonitor.schedule(v1Workspace)
      }

      bucketName <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID).flatMap { attempt =>
        WorkspaceMigrationMonitor.createFinalBucket(
          attempt.copy(
            newGoogleProjectId = GoogleProjectId(destProject).some,
            tmpBucketName = GcsBucketName(sourceBucket).some
          ),
          v1Workspace
        )
      }

      bucket <- ReaderT { env: MigrationDeps =>
        env.storageService.getBucket(GoogleProject(destProject), bucketName) <*
          env.storageService.deleteBucket(GoogleProject(destProject), bucketName, isRecursive = true).compile.drain
      }

      attempt <- getAttempt(v1Workspace.workspaceIdAsUUID)
    } yield {
      bucket shouldBe defined
      attempt.finalBucketCreated shouldBe defined
    }

    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"

    runMigrationTest(ReaderT { env =>
      GoogleStorageService.resource[IO](pathToCredentialJson, None, serviceProject.some).use {
        googleStorageService => test.run(env.copy(storageService = googleStorageService))
      }
    })
  }


  def runStep(configureBefore: WorkspaceMigration => WorkspaceMigration,
              assertAfter: WorkspaceMigration => Assertion) =
    runMigrationTest {
      for {
        _ <- WorkspaceMigrationMonitor.inTransaction { _ =>
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace) >>
            WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        }

        _ <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
          .map(configureBefore)
          .flatMap(WorkspaceMigrationMonitor.step)

        migration <- getAttempt(spec.testData.v1Workspace.workspaceIdAsUUID)
      } yield assertAfter(migration)
    }


  "step" should "advance the migration from Created -> Started" in
    runStep(identity, _.getStatus.isInstanceOf[Started] shouldBe true)


  def now: Timestamp = Timestamp.valueOf(LocalDateTime.now())

  it should "advance the migration from Started -> GoogleProjectConfigured" in
    runStep(
      _.copy(started = now.some),
      _.getStatus.isInstanceOf[GoogleProjectConfigured] shouldBe true
    )


  // cant mock out creating a Bucket - thanks google.
  it should "advance the migration from GoogleProjectConfigured -> TmpBucketCreated" ignore
    runStep(
      _.copy(
        started = now.some,
        newGoogleProjectConfigured = now.some,
        newGoogleProjectId = GoogleProjectId(UUID.randomUUID.toString).some
      ),
      _.getStatus.isInstanceOf[TmpBucketCreated] shouldBe true
    )


  it should "advance the migration from TmpBucketCreated -> TmpBucketCreated" ignore
    runStep(
      _.copy(
        started = now.some,
        newGoogleProjectConfigured = now.some,
        newGoogleProjectId = GoogleProjectId(UUID.randomUUID.toString).some,
        tmpBucketCreated = now.some,
        tmpBucketName = GcsBucketName("tmp-bucket").some
      ),
      m => m.getStatus.isInstanceOf[TmpBucketCreated] shouldBe true
    )

}

