package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, Workspace, WorkspaceShardStates, WorkspaceVersions}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import java.sql.SQLException
import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.{ConsoleLogger, LogLevel}

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
    val config = ConfigFactory.load()
    val gcsConfig = config.getConfig("gcs")
    val serviceProject = GoogleProject(gcsConfig.getString("serviceProject"))
    val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")

    withMinimalTestDatabase { _ =>
      GoogleStorageService.resource[IO](pathToCredentialJson, None, Option(serviceProject)).use { googleStorageService =>
        for {
          res <- V1WorkspaceMigrationMonitor.createTempBucket(minimalTestData.v1Workspace, GoogleProject("broad-dsde-dev"), googleStorageService)
          (bucketName, _) = res
          loadedBucket <- googleStorageService.getBucket(GoogleProject("broad-dsde-dev"), bucketName)
        } yield loadedBucket.isDefined shouldBe true
      }
    }
  }

}
