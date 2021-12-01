package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.google.cloud.storage.{Bucket, Storage}
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{RETURNS_SMART_NULLS, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatestplus.mockito.MockitoSugar.mock
import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import java.sql.SQLException
import scala.language.postfixOps

class V1WorkspaceMigrationMonitorSpec
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
      runAndWait(V1WorkspaceMigrationMonitor.isMigrating(minimalTestData.v1Workspace)) shouldBe false
    }
  }

  "schedule" should "error when a workspace is scheduled concurrently" in {
    withMinimalTestDatabase { _ =>
      runAndWait(V1WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace)) shouldBe ()
      assertThrows[SQLException] {
        runAndWait(V1WorkspaceMigrationMonitor.schedule(minimalTestData.v1Workspace))
      }
    }
  }

  // use an existing test project (broad-dsde-dev)
  "temp Integration Test" should "for realsies create a new bucket in the same region" ignore {
    val sourceProject = "general-dev-billing-account"
    val sourceBucket = "az-leotest"
    val destProject = "terra-dev-7af423b8"
    val config = ConfigFactory.load()
    val gcsConfig = config.getConfig("gcs")
    val serviceProject = GoogleProject(sourceProject)
    val pathToCredentialJson = "config/rawls-account.json"
    val v1WorkspaceCopy = minimalTestData.v1Workspace.copy(namespace = sourceProject, googleProjectId = GoogleProjectId(sourceProject), bucketName = sourceBucket)

    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1WorkspaceCopy),
          V1WorkspaceMigrationMonitor.schedule(v1WorkspaceCopy)
        )
      }
      val attempt = runAndWait(migrations.filter(_.workspaceId === v1WorkspaceCopy.workspaceIdAsUUID).result).head
      val writeAction = GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- V1WorkspaceMigrationMonitor.createTempBucket(attempt, v1WorkspaceCopy, GoogleProject(destProject), googleStorageService)
          (bucketName, writeAction) = res
          loadedBucket <- googleStorageService.getBucket(GoogleProject(destProject), bucketName)
          _ <- googleStorageService.deleteBucket(GoogleProject(destProject), bucketName).compile.drain
        } yield {
          loadedBucket shouldBe defined
          writeAction
        }
      }.unsafeRunSync
      runAndWait(writeAction)
    }
  }

  "createTempBucket" should "create a new bucket in the same region" in {
    val sourceProject = "something"
    val sourceBucketName = "something_bucket"
    val destProject = "something_else"
    val v1WorkspaceCopy = minimalTestData.v1Workspace.copy(namespace = sourceProject, googleProjectId = GoogleProjectId(sourceProject), bucketName = sourceBucketName)

    val googleStorageService = mock[GoogleStorageService[IO]](RETURNS_SMART_NULLS)
    when(googleStorageService.getBucket(any[GoogleProject], ArgumentMatchers.eq(GcsBucketName(sourceBucketName)), any[List[Storage.BucketGetOption]]))
      .thenReturn(IO.pure(Option(new Bucket.Builder().setName(sourceBucketName).build())))

    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1WorkspaceCopy),
          V1WorkspaceMigrationMonitor.schedule(v1WorkspaceCopy)
        )
      }
      val attempt = runAndWait(migrations.filter(_.workspaceId === v1WorkspaceCopy.workspaceIdAsUUID).result).head

      val writeAction = (for {
        res <- V1WorkspaceMigrationMonitor.createTempBucket(attempt, v1WorkspaceCopy, GoogleProject(destProject), googleStorageService)
        (bucketName, writeAction) = res
        loadedBucket <- googleStorageService.getBucket(GoogleProject(destProject), bucketName)
        _ <- googleStorageService.deleteBucket(GoogleProject(destProject), bucketName).compile.drain
      } yield {
        loadedBucket shouldBe defined
        writeAction
      }).unsafeRunSync
      runAndWait(writeAction)
      val migrationRow = runAndWait(migrations.filter(_.workspaceId === v1WorkspaceCopy.workspaceIdAsUUID).result).head
      migrationRow.tmpBucketName.value.value should startWith("workspace_migration_")
      migrationRow.tmpBucketCreated shouldBe defined
    }
  }

}
