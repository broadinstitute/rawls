package org.broadinstitute.dsde.rawls.monitor

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, OptionValues}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class WorkspaceBillingAccountMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with OptionValues {
  val defaultExecutionContext: ExecutionContext = executionContext
  implicit val cs = IO.contextShift(global)
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

  "WorkspaceBillingAccountMonitor" should "update the billing account on all workspaces in a billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
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

      val actor = createWorkspaceBillingAccountMonitor(dataSource)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName)).map(_.currentBillingAccountOnGoogleProject).toSet shouldBe Set(Option(newBillingAccount))
      }
      system.stop(actor)
    }
  }

  it should "not endlessly retry when it fails to update a billing account" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val badWorkspaceGoogleProjectId = GoogleProjectId("very bad")
      val badWorkspace = Workspace(billingProject.projectName.value, "bad", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, badWorkspaceGoogleProjectId, Option(GoogleProjectNumber("44")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      val newBillingAccount = RawlsBillingAccountName("new-ba")


      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new RawlsException(failureMessage)
      when(failingGcsDAO.updateGoogleProjectBillingAccount(badWorkspace.googleProjectId, Option(newBillingAccount)))
        .thenReturn(Future.failed(exception))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(v1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(workspaceQuery.createOrUpdate(badWorkspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(badWorkspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
      }
      verify(failingGcsDAO, times(1)).updateGoogleProjectBillingAccount(badWorkspace.googleProjectId, Option(newBillingAccount))

      system.stop(actor)
    }
  }

  // TODO: CA-1235 Remove during cleanup once all workspaces have their own Google project
  it should "propagate error messages to all workspaces in a Google project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val secondV1Workspace = Workspace(billingProject.projectName.value, "second", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)

      val newBillingAccount = RawlsBillingAccountName("new-ba")


      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new RawlsException(failureMessage)
      when(failingGcsDAO.updateGoogleProjectBillingAccount(secondV1Workspace.googleProjectId, Option(newBillingAccount)))
        .thenReturn(Future.failed(exception))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(v1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(workspaceQuery.createOrUpdate(secondV1Workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(secondV1Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
        runAndWait(workspaceQuery.findByName(v1Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
        runAndWait(workspaceQuery.findByName(v2Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe None
      }
      verify(failingGcsDAO, times(1)).updateGoogleProjectBillingAccount(secondV1Workspace.googleProjectId, Option(newBillingAccount))

      system.stop(actor)
    }
  }

  it should "mark a billing project's billing account as invalid if Google returns a 403" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val newBillingAccount = RawlsBillingAccountName("new-ba")

      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, failureMessage))
      when(failingGcsDAO.updateGoogleProjectBillingAccount(any[GoogleProjectId], any[Option[RawlsBillingAccountName]]))
        .thenReturn(Future.failed(exception))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName)).getOrElse(fail("project not found"))
        .invalidBillingAccount shouldBe false

      runAndWait(workspaceQuery.createOrUpdate(v1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName)).getOrElse(fail("project not found"))
          .invalidBillingAccount shouldBe true
        runAndWait(workspaceQuery.listWithBillingProject(billingProject.projectName))
          .map(_.billingAccountErrorMessage shouldBe None)
      }

      system.stop(actor)
    }
  }
}
