package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.OptionValues._

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class WorkspaceBillingAccountMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with OptionValues {
  val defaultExecutionContext: ExecutionContext = executionContext
  def this() = this(ActorSystem("WorkspaceBillingAccountMonitorSpec"))

  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("test-ba")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createWorkspaceBillingAccountMonitor(dataSource: SlickDataSource,
                                           mockGcsDAO: GoogleServicesDAO = new MockGoogleServicesDAO("test"))
                                          (implicit executionContext: ExecutionContext) = {
    system.actorOf(WorkspaceBillingAccountMonitor.props(dataSource, mockGcsDAO, 1 second, 1 second))
  }

  "WorkspaceBillingAccountMonitor" should "update the billing account on all v1 and v2 workspaces in a billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(billingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val workspaceWithoutBillingAccount = v2Workspace.copy(
        name = UUID.randomUUID().toString,
        currentBillingAccountOnGoogleProject = None
      )

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(v1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(workspaceQuery.createOrUpdate(workspaceWithoutBillingAccount))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val mockGcsDao = new MockGoogleServicesDAO("test")
      val actor = createWorkspaceBillingAccountMonitor(dataSource, mockGcsDao)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName)).map(_.currentBillingAccountOnGoogleProject).toSet shouldBe Set(Option(newBillingAccount))
      }
      system.stop(actor)
    }
  }

  it should "not endlessly retry when it fails to get billing info for the google project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace = Workspace(billingProject.projectName.value, "whatever", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("any old project"), Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(RawlsBillingAccountName("new-ba"))))

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDAO = spy(new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] =
          Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(exceptionMessage)))
      })
      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage.value should include(exceptionMessage)
      }
      verify(failingGcsDAO, times(1)).getBillingInfoForGoogleProject(workspace.googleProjectId)

      system.stop(actor)
    }
  }

  it should "not endlessly retry when it fails to set billing info for the google project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace = Workspace(billingProject.projectName.value, "whatever", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("any old project"), Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(RawlsBillingAccountName("new-ba"))))

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDAO = spy(new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName): Future[ProjectBillingInfo] =
          Future.failed(new RawlsException(exceptionMessage))
      })
      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage.value should include(exceptionMessage)
      }
      verify(failingGcsDAO, times(1)).getBillingInfoForGoogleProject(workspace.googleProjectId)

      system.stop(actor)
    }
  }

  it should "not try to update the billing account if the new value is the same as the old value" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccountName = defaultBillingAccountName
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(originalBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace = Workspace(billingProject.projectName.value, "whatever", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("any old project"), Option(GoogleProjectNumber("44")), Option(originalBillingAccountName), None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(originalBillingAccountName)))

      val mockGcsDAO = spy(new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] =
          Future.successful(new ProjectBillingInfo().setBillingAccountName(originalBillingAccountName.value).setBillingEnabled(true))
      })

      val actor = createWorkspaceBillingAccountMonitor(dataSource, mockGcsDAO)

      // need to give the actor some time to run
      val afterStaleMillis = 3000
      Thread.sleep(afterStaleMillis)

      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .currentBillingAccountOnGoogleProject.value shouldBe originalBillingAccountName

      verify(mockGcsDAO, times(0)).setBillingAccountName(workspace.googleProjectId, originalBillingAccountName)

      system.stop(actor)
    }
  }

  it should "continue to update other workspace google projects even if one fails to update" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace1 = Workspace(billingProject.projectName.value, "workspace1", UUID.randomUUID().toString, "bucketName1", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val workspace2 = Workspace(billingProject.projectName.value, "workspace2", UUID.randomUUID().toString, "bucketName2", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val badWorkspaceGoogleProjectId = GoogleProjectId("very bad")
      val badWorkspace = Workspace(billingProject.projectName.value, "bad", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, badWorkspaceGoogleProjectId, Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] = {
          if (googleProjectId == badWorkspaceGoogleProjectId) {
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, exceptionMessage)))
          } else {
            super.getBillingInfoForGoogleProject(googleProjectId)
          }
        }
      }

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(workspace1))
      runAndWait(workspaceQuery.createOrUpdate(workspace2))
      runAndWait(workspaceQuery.createOrUpdate(badWorkspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDao)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(workspace1.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .currentBillingAccountOnGoogleProject shouldBe Option(newBillingAccount)
        runAndWait(workspaceQuery.findByName(workspace2.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .currentBillingAccountOnGoogleProject shouldBe Option(newBillingAccount)
        runAndWait(workspaceQuery.findByName(badWorkspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage.value should include(exceptionMessage)
      }

      system.stop(actor)
    }
  }

  // TODO: CA-1235 Remove during cleanup once all workspaces have their own Google project
  it should "propagate error messages to all workspaces in a Google project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(RawlsBillingAccountName("original-ba"))
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("v1-Billing-Project"), CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1GoogleProjectId = GoogleProjectId(billingProject.projectName.value)
      val firstV1Workspace  = Workspace(billingProject.projectName.value, "first-v1-workspace",  UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, v1GoogleProjectId, billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val secondV1Workspace = Workspace(billingProject.projectName.value, "second-v1-workspace", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, v1GoogleProjectId, billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2 workspace", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("v2WorkspaceGoogleProject"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      val exceptionMessage = "oh what a shame!  It went kerplooey!"
      val failingGcsDao = spy(new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName): Future[ProjectBillingInfo] = {
          if (googleProjectId == v1GoogleProjectId) {
            Future.failed(new RawlsException(exceptionMessage))
          } else {
            super.getBillingInfoForGoogleProject(googleProjectId)
          }
        }
      })

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(firstV1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(workspaceQuery.createOrUpdate(secondV1Workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDao)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(secondV1Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage.value should include(exceptionMessage)
        runAndWait(workspaceQuery.findByName(firstV1Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage.value should include(exceptionMessage)
        runAndWait(workspaceQuery.findByName(v2Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe None
      }

      verify(failingGcsDao, times(1)).setBillingAccountName(v1GoogleProjectId, newBillingAccount)

      system.stop(actor)
    }
  }

  it should "mark a billing project's billing account as invalid if Google returns a 403" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val newBillingAccount = RawlsBillingAccountName("new-ba")


      val exceptionMessage = "Naughty naughty!  You ain't got no permissions!"
      val failingGcsDao = new MockGoogleServicesDAO("") {
        override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName): Future[ProjectBillingInfo] = {
          Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, exceptionMessage)))
        }
      }

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName)).getOrElse(fail("project not found"))
        .invalidBillingAccount shouldBe false

      runAndWait(workspaceQuery.createOrUpdate(v1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDao)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName)).getOrElse(fail("project not found"))
          .invalidBillingAccount shouldBe true
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.billingAccountErrorMessage.value should include(exceptionMessage))
      }

      system.stop(actor)
    }
  }
}
