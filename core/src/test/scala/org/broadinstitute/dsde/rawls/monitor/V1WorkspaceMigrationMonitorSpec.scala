package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}

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
  "createTempBucket" should "create a new bucket in the same region" in {
    val sourceAndDestProject = "broad-dsde-dev"
    val config = ConfigFactory.load()
    val gcsConfig = config.getConfig("gcs")
    val serviceProject = GoogleProject(sourceAndDestProject)
    val pathToCredentialJson = "config/rawls-account.json"
    val v1WorkspaceCopy = minimalTestData.v1Workspace.copy(namespace = sourceAndDestProject, googleProjectId = GoogleProjectId(sourceAndDestProject), bucketName = "rawls-test-v1-workspace-migration-monitor-source-bucket")

    withMinimalTestDatabase { _ =>
      runAndWait(workspaceQuery.createOrUpdate(v1WorkspaceCopy))
      GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- V1WorkspaceMigrationMonitor.createTempBucket(v1WorkspaceCopy, GoogleProject(sourceAndDestProject), googleStorageService)
          (bucketName, _) = res
          loadedBucket <- googleStorageService.getBucket(GoogleProject(sourceAndDestProject), bucketName)
          _ <- googleStorageService.deleteBucket(GoogleProject(sourceAndDestProject), bucketName).compile.drain
        } yield loadedBucket shouldBe defined
      }.unsafeRunSync
    }
  }

}
