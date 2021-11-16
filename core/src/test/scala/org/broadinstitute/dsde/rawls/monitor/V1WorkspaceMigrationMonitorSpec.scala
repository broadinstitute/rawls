package org.broadinstitute.dsde.rawls.monitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, Workspace, WorkspaceShardStates, WorkspaceVersions}
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

  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("test-ba")
  val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
  val sourceBucketName = "sourceBucket"
  val workspace = Workspace(billingProject.projectName.value, "source", UUID.randomUUID().toString, sourceBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("some-project"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "isMigrating" should "return false when a workspace is not being migrated" in {
    withEmptyTestDatabase {
      runAndWait(V1WorkspaceMigrationMonitor.isMigrating(workspace)) shouldBe false
    }
  }

  "schedule" should "error when a workspace is scheduled concurrently" in {
    withEmptyTestDatabase {
      runAndWait(V1WorkspaceMigrationMonitor.schedule(workspace)) shouldBe()
      assertThrows[SQLException] {
        runAndWait(V1WorkspaceMigrationMonitor.schedule(workspace))
      }
    }
  }
}
