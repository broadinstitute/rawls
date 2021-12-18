package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.google.auth.Credentials
import com.google.cloud.{Identity, Policy}
import com.google.cloud.storage.{Acl, Blob, BlobId, Bucket, BucketInfo, Storage}
import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import com.typesafe.config.ConfigFactory
import fs2.Pipe
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationMonitor
import org.broadinstitute.dsde.rawls.workspace.WorkspaceServiceSpec
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobName, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleStorageService, GoogleStorageTransferService, OperationName, RemoveObjectResult, StorageRole}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject, ServiceAccount}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import java.nio.file.Path
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

  class MockGoogleStorageService extends GoogleStorageService[IO] {

    override def listObjectsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, isRecursive: Boolean, maxPageSize: Long, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, GcsObjectName] = ???

    override def listBlobsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, isRecursive: Boolean, maxPageSize: Long, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Blob] = ???

    override def createBlob(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String, metadata: Map[String, String], generation: Option[Long], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Blob] = ???

    override def streamUploadBlob(bucketName: GcsBucketName, objectName: GcsBlobName, metadata: Map[String, String], generation: Option[Long], overwrite: Boolean, traceId: Option[TraceId]): Pipe[IO, Byte, Unit] = ???

    override def setBucketLifecycle(bucketName: GcsBucketName, lifecycleRules: List[BucketInfo.LifecycleRule], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] = ???

    override def unsafeGetBlobBody(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId], retryConfig: RetryConfig): IO[Option[String]] = ???

    override def getBlobBody(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Byte] = ???

    override def getBlob(bucketName: GcsBucketName, blobName: GcsBlobName, credential: Option[Credentials], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Blob] = ???

    override def downloadObject(blobId: BlobId, path: Path, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] = ???

    override def getObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, GetMetadataResponse] = ???

    override def setObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, metadata: Map[String, String], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] = ???

    override def removeObject(bucketName: GcsBucketName, blobName: GcsBlobName, generation: Option[Long], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, RemoveObjectResult] = ???

    override def getBucket(googleProject: GoogleProject, bucketName: GcsBucketName, bucketGetOptions: List[Storage.BucketGetOption], traceId: Option[TraceId]): IO[Option[Bucket]] = ???

    override def insertBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], labels: Map[String, String], traceId: Option[TraceId], bucketPolicyOnlyEnabled: Boolean, logBucket: Option[GcsBucketName], retryConfig: RetryConfig, location: Option[String]): fs2.Stream[IO, Unit] = ???

    override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[Storage.BucketSourceOption], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Boolean] = ???

    override def setBucketPolicyOnly(bucketName: GcsBucketName, bucketPolicyOnlyEnabled: Boolean, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] = ???

    override def setBucketLabels(bucketName: GcsBucketName, labels: Map[String, String], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] = ???

    override def setIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Unit] = ???

    override def overrideIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Policy] = ???

    override def getIamPolicy(bucketName: GcsBucketName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[IO, Policy] = ???
  }

  class MockGoogleStorageTransferService extends GoogleStorageTransferService[IO] {
    override def getStsServiceAccount(project: GoogleProject): IO[ServiceAccount] = ???

    override def createTransferJob(jobName: JobName, jobDescription: String, projectToBill: GoogleProject, originBucket: GcsBucketName, destinationBucket: GcsBucketName, schedule: JobTransferSchedule): IO[TransferJob] = ???

    override def getTransferJob(jobName: JobName, project: GoogleProject): IO[TransferJob] = ???

    override def listTransferOperations(jobName: JobName, project: GoogleProject): IO[Seq[Operation]] = ???

    override def getTransferOperation(operationName: OperationName): IO[Operation] = ???
  }

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
    val config = ConfigFactory.load()
    val gcsConfig = config.getConfig("gcs")
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
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val destBucket = "v1-migration-test-" + UUID.randomUUID.toString.replace("-", "")

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

      // We need a temp bucket to transfer the workspace bucket contents into
      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID).result
      )
        .head
        .copy(
          newGoogleProjectId = GoogleProjectId(destProject).some,
          tmpBucketName = GcsBucketName(sourceBucket).some
        )

      val writeAction = WorkspaceMigrationMonitor.deleteWorkspaceBucket(
        migration,
        v1Workspace,
        new MockGoogleStorageService {
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

  "startBucketStorageTransferJob" should "create and start a storage transfer job between teh source and destination bucket" in {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val destBucket = "v1-migration-test-" + UUID.randomUUID.toString.replace("-", "")

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

      // We need a temp bucket to transfer the workspace bucket contents into
      val migration = runAndWait(
        WorkspaceMigrationMonitor.workspaceMigrations
          .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID).result
      )
        .head
        .copy(
          newGoogleProjectId = GoogleProjectId(destProject).some,
          tmpBucketName = GcsBucketName(sourceBucket).some
        )

      val (job, writeAction) = WorkspaceMigrationMonitor.startBucketStorageTransferJob(
        migration,
        GcsBucketName(v1Workspace.bucketName),
        migration.tmpBucketName.get,
        GoogleProject("to-be-determined"),
        new MockGoogleStorageTransferService {
          override def createTransferJob(jobName: JobName, jobDescription: String, projectToBill: GoogleProject, originBucket: GcsBucketName, destinationBucket: GcsBucketName, schedule: JobTransferSchedule): IO[TransferJob] =
            IO.pure {
              TransferJob.newBuilder()
                .setName(s"${jobName}")
                .setDescription(jobDescription)
                .setProjectId(s"${projectToBill}")
                .build
            }
        }).unsafeRunSync

      runAndWait(writeAction) shouldBe ()

      val transferJobs = runAndWait(
        WorkspaceMigrationMonitor.storageTransferJobs
          .filter(_.migrationId === migration.id)
          .result
      )

      transferJobs.length shouldBe 1
      transferJobs.head.jobName.value shouldBe job.getName
      transferJobs.head.originBucket.value shouldBe v1Workspace.bucketName
      transferJobs.head.destBucket shouldBe migration.tmpBucketName.get
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

