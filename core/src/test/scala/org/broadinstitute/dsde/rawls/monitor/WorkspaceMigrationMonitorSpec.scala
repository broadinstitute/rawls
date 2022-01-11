package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.google.cloud.storage.Storage
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.mock.{MockGoogleStorageService, MockGoogleStorageTransferService}
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationMonitor
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
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import java.sql.SQLException
import java.util.UUID
import scala.language.postfixOps

class WorkspaceMigrationMonitorSpec
  extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with TestDriverComponent
    with Eventually
    with OptionValues {

  val testKit: ActorTestKit = ActorTestKit()
  implicit val logger = new ConsoleLogger("unit_test", LogLevel(false, false, true, true))

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "isMigrating" should "return false when a workspace is not being migrated" in {
    withMinimalTestDatabase { _ =>
      runAndWait(WorkspaceMigrationMonitor.isMigrating(minimalTestData.v1Workspace)) shouldBe false
    }
  }

  "schedule" should "error when a workspace is scheduled concurrently" in {
    withMinimalTestDatabase { _ =>
      runAndWait(WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)) shouldBe()
      assertThrows[SQLException] {
        runAndWait(WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace))
      }
    }
  }

  "claimAndConfigureGoogleProject" should "return a valid database operation" in {
    val spec = new WorkspaceServiceSpec()
    spec.withTestDataServices { services =>
      spec.runAndWait {
        DBIO.seq(
          spec.workspaceQuery.createOrUpdate(spec.testData.v1Workspace),
          WorkspaceMigrationMonitor.schedule(spec.testData.v1Workspace)
        )
      }

      val (_, _, dbOp) = IO.fromFuture(IO {
        services.slickDataSource.database
          .run {
            WorkspaceMigrationMonitor.workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .result
          }
      })
        .map(_.head)
        .flatMap { attempt =>
          WorkspaceMigrationMonitor.claimAndConfigureNewGoogleProject(
            attempt,
            services.workspaceService,
            spec.testData.v1Workspace,
            spec.testData.billingProject
          )
        }
        .unsafeRunSync

      spec.runAndWait(dbOp) shouldBe()

      val (projectId, projectNumber, projectConfigured) = IO.fromFuture(IO {
        services.slickDataSource.database
          .run {
            WorkspaceMigrationMonitor.workspaceMigrations
              .filter(_.workspaceId === spec.testData.v1Workspace.workspaceIdAsUUID)
              .map(r => (r.newGoogleProjectId, r.newGoogleProjectNumber, r.newGoogleProjectConfigured))
              .result
          }
      })
        .map(_.head)
        .unsafeRunSync

      projectId shouldBe defined
      projectNumber shouldBe defined
      projectConfigured shouldBe defined
    }
  }

  // use an existing test project (broad-dsde-dev)
  "createTempBucket" should "create a new bucket in the same region as the workspace bucket" ignore {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"
    val v1WorkspaceCopy = minimalTestData.v1Workspace.copy(
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = sourceBucket
    )

    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1WorkspaceCopy),
          WorkspaceMigrationMonitor.schedule(v1WorkspaceCopy)
        )
      }

      // Creating the temp bucket requires that the new google project has been created
      val attempt = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === v1WorkspaceCopy.workspaceIdAsUUID).result
      )
        .head
        .copy(newGoogleProjectId = GoogleProjectId(destProject).some)

      val writeAction = GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- WorkspaceMigrationMonitor.createTempBucket(attempt, v1WorkspaceCopy, googleStorageService)
          (bucketName, writeAction) = res
          loadedBucket <- googleStorageService.getBucket(GoogleProject(destProject), bucketName)
          _ <- googleStorageService.deleteBucket(GoogleProject(destProject), bucketName).compile.drain
        } yield {
          loadedBucket shouldBe defined
          writeAction
        }
      }.unsafeRunSync

      runAndWait(writeAction) shouldBe()
    }
  }

  "deleteWorkspaceBucket" should "delete the workspace bucket and record when it was deleted" in {
    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(minimalTestData.v1Workspace),
          WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)
        )
      }

      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === minimalTestData.v1Workspace.workspaceIdAsUUID).result
      )
        .head

      val writeAction = WorkspaceMigrationMonitor.deleteWorkspaceBucket(
        migration,
        minimalTestData.v1Workspace,
        new MockGoogleStorageService[IO] {
          override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[Storage.BucketSourceOption], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Boolean] =
            fs2.Stream.emit(true)
        }
      ).unsafeRunSync

      runAndWait(writeAction) shouldBe()

      val workspaceBucketDeleted = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.workspaceBucketDeleted)
          .result
          .map(_.head)
      )

      workspaceBucketDeleted shouldBe defined
    }
  }

  "deleteTemporaryBucket" should "delete the temporary bucket and record when it was deleted" in
    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(minimalTestData.v1Workspace),
          WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)
        )
      }

      // We need a temp bucket to transfer the workspace bucket contents into
      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === minimalTestData.v1Workspace.workspaceIdAsUUID).result
      )
        .head
        .copy(
          newGoogleProjectId = GoogleProjectId("google-project-").some,
          tmpBucketName = GcsBucketName("temp-bucket-name").some
        )

      val writeAction = WorkspaceMigrationMonitor.deleteTemporaryBucket(
        migration,
        new MockGoogleStorageService[IO] {
          override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[Storage.BucketSourceOption], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Boolean] =
            fs2.Stream.emit(true)
        }
      ).unsafeRunSync

      runAndWait(writeAction) shouldBe()

      val tmpBucketDeleted = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.tmpBucketDeleted)
          .result
          .map(_.head)
      )

      tmpBucketDeleted shouldBe defined
    }

  val mockStsService = new MockGoogleStorageTransferService[IO] {
    override def createTransferJob(jobName: JobName, jobDescription: String, projectToBill: GoogleProject, originBucket: GcsBucketName, destinationBucket: GcsBucketName, schedule: JobTransferSchedule): IO[TransferJob] =
      IO.pure {
        TransferJob.newBuilder()
          .setName(s"${jobName}")
          .setDescription(jobDescription)
          .setProjectId(s"${projectToBill}")
          .build
      }
  }

  "startStorageTransferJobToTmpBucket" should "create and start a storage transfer job from the workspace bucket to the temp bucket" in {
    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(minimalTestData.v1Workspace),
          WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)
        )
      }

      // We need a temp bucket to transfer the workspace bucket contents into
      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === minimalTestData.v1Workspace.workspaceIdAsUUID)
          .result
      )
        .head
        .copy(
          tmpBucketName = GcsBucketName("tmp-bucket-name").some
        )

      val (job, writeAction) = WorkspaceMigrationMonitor.startStorageTransferJobToTmpBucket(
        migration,
        minimalTestData.v1Workspace,
        GoogleProject("to-be-determined"),
        mockStsService
      ).unsafeRunSync

      runAndWait(writeAction) shouldBe()

      val transferJobs = runAndWait(
        WorkspaceMigrationMonitor.storageTransferJobs
          .filter(_.migrationId === migration.id)
          .result
      )

      transferJobs.length shouldBe 1
      transferJobs.head.jobName.value shouldBe job.getName
      transferJobs.head.originBucket.value shouldBe minimalTestData.v1Workspace.bucketName
      transferJobs.head.destBucket shouldBe migration.tmpBucketName.get
    }
  }

  "startStorageTransferJobToFinalBucket" should "create and start a storage transfer job from the tmp bucket to the new workspace bucket" in {
    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(minimalTestData.v1Workspace),
          WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)
        )
      }

      // We need a temp bucket to transfer the workspace bucket contents into
      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === minimalTestData.v1Workspace.workspaceIdAsUUID)
          .result
      )
        .head
        .copy(
          tmpBucketName = GcsBucketName("tmp-bucket-name").some
        )

      val (job, writeAction) = WorkspaceMigrationMonitor.startStorageTransferJobToFinalBucket(
        migration,
        minimalTestData.v1Workspace,
        GoogleProject("to-be-determined"),
        mockStsService
      ).unsafeRunSync

      runAndWait(writeAction) shouldBe()

      val transferJobs = runAndWait(
        WorkspaceMigrationMonitor.storageTransferJobs
          .filter(_.migrationId === migration.id)
          .result
      )

      transferJobs.length shouldBe 1
      transferJobs.head.jobName.value shouldBe job.getName
      transferJobs.head.originBucket.value shouldBe migration.tmpBucketName.get.value
      transferJobs.head.destBucket.value shouldBe minimalTestData.v1Workspace.bucketName
    }
  }

  "createFinalBucket" should "create a new bucket in the same region as the tmp workspace bucket" ignore {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val destBucket = "v1-migration-test-" + UUID.randomUUID.toString.replace("-", "")
    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"

    val v1Workspace = minimalTestData.v1Workspace.copy(
      namespace = sourceProject,
      googleProjectId = GoogleProjectId(sourceProject),
      bucketName = destBucket
    )

    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1Workspace),
          WorkspaceMigrationMonitor.schedule(v1Workspace)
        )
      }

      // Creating the bucket requires that the new google project and tmp bucket have been created
      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID)
          .result
      )
        .head
        .copy(
          newGoogleProjectId = GoogleProjectId(destProject).some,
          tmpBucketName = GcsBucketName(sourceBucket).some
        )

      val writeAction = GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- WorkspaceMigrationMonitor.createFinalBucket(migration, v1Workspace, googleStorageService)
          (bucketName, writeAction) = res
          loadedBucket <- googleStorageService.getBucket(GoogleProject(destProject), bucketName)
          _ <- googleStorageService.deleteBucket(GoogleProject(destProject), bucketName).compile.drain
        } yield {
          loadedBucket shouldBe defined
          loadedBucket.get.getName shouldBe destBucket
          writeAction
        }
      }.unsafeRunSync

      runAndWait(writeAction) shouldBe()

      val finalBucketCreated = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.finalBucketCreated)
          .result
          .map(_.head)
      )

      finalBucketCreated shouldBe defined
    }
  }

}

