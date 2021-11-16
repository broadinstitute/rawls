package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, GoogleProjectId, GoogleProjectNumber, RawlsBillingAccountName, RawlsBillingProject, RawlsBillingProjectName, Workspace, WorkspaceShardStates, WorkspaceVersions}
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID

class V1WorkspaceMigrationMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with OptionValues {

  "V1WorkspaceMigrationMonitor" should "return false when a workspace is not migrating" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
      val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
      val defaultBillingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("test-ba")
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val sourceBucketName = "sourceBucket"
      val workspace = Workspace(billingProject.projectName.value, "source", UUID.randomUUID().toString, sourceBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("some-project"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      eventually {
        V1WorkspaceMigrationMonitor.isMigrating(workspace)
      }
    }
  }

}
