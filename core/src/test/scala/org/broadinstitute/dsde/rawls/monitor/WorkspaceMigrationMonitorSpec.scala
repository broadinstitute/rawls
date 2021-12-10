package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.rawls.monitor.migration.{WorkspaceMigrationMonitor, WorkspaceMigrationHistory}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceServiceSpec
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
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
            WorkspaceMigrationHistory.workspaceMigrations
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
            WorkspaceMigrationHistory.workspaceMigrations
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
    val v1WorkspaceCopy = minimalTestData.v1Workspace.copy(namespace = sourceProject, googleProjectId = GoogleProjectId(sourceProject), bucketName = sourceBucket)

    withMinimalTestDatabase { _ =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1WorkspaceCopy),
          WorkspaceMigrationMonitor.schedule(v1WorkspaceCopy)
        )
      }

      // Creating the temp bucket requires that the new google project has been created
      val attempt = runAndWait(
        WorkspaceMigrationHistory.workspaceMigrations
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

    withMinimalTestDatabase { dataSource =>
      runAndWait {
        DBIO.seq(
          workspaceQuery.createOrUpdate(v1Workspace),
          WorkspaceMigrationMonitor.schedule(v1Workspace)
        )
      }

      // Creating the bucket requires that the new google project and tmp bucket have been created
      val attempt = runAndWait(
        WorkspaceMigrationHistory.workspaceMigrations
          .filter(_.workspaceId === v1Workspace.workspaceIdAsUUID).result
      )
        .head
        .copy(
          newGoogleProjectId = GoogleProjectId(destProject).some,
          tmpBucketName = GcsBucketName(sourceBucket).some
        )

      val writeAction = GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- WorkspaceMigrationMonitor.createFinalBucket(attempt, v1Workspace, googleStorageService)
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

      val finalBucketCreated = IO.fromFuture(IO {
        dataSource.database
          .run {
            WorkspaceMigrationHistory.workspaceMigrations
              .filter(_.id === attempt.id)
              .map(_.finalBucketCreated)
              .result
          }
      })
        .map(_.head)
        .unsafeRunSync

      finalBucketCreated shouldBe defined
    }
  }

}

