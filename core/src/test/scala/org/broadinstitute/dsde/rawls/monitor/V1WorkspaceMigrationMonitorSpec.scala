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
import scala.language.postfixOps

class V1WorkspaceMigrationMonitorSpec
  extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with TestDriverComponent
    with Eventually
    with OptionValues {

  val testKit: ActorTestKit = ActorTestKit()

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
  "createBucketInSameRegion" should "create a new bucket in the same region" in {
    val gcsServiceDao = HttpGoogleServicesDAO

    val googleStorageService = GoogleStorageService
    googleStorageService.insertBucket()
  }

}
