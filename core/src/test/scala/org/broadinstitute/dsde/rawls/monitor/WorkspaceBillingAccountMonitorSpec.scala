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

  "WorkspaceBillingAccountMonitor" should "update the billing account on all workspaces in a billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
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
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val badWorkspaceGoogleProjectId = GoogleProjectId("very bad")
      val badWorkspace = Workspace(billingProject.projectName.value, "bad", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, badWorkspaceGoogleProjectId, Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new RawlsException(failureMessage)
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        ArgumentMatchers.eq(badWorkspace.googleProjectId),
        ArgumentMatchers.eq(Option(newBillingAccount)),
        any[Option[RawlsBillingAccountName]],
        any[Boolean]))
        .thenReturn(Future.failed(exception))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(badWorkspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(badWorkspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
      }
      verify(failingGcsDAO, times(1)).updateGoogleProjectBillingAccount(badWorkspace.googleProjectId, Option(newBillingAccount), originalBillingAccount)

      system.stop(actor)
    }
  }

  it should "continue even if one workspace google project fails to update" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val workspace1 = Workspace(billingProject.projectName.value, "workspace1", UUID.randomUUID().toString, "bucketName1", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val workspace2 = Workspace(billingProject.projectName.value, "workspace2", UUID.randomUUID().toString, "bucketName2", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val badWorkspaceGoogleProjectId = GoogleProjectId("very bad")
      val badWorkspace = Workspace(billingProject.projectName.value, "bad", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, badWorkspaceGoogleProjectId, Option(GoogleProjectNumber("44")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")

      val newBillingAccount = RawlsBillingAccountName("new-ba")


      // Going to set up some mocking.  In this case, we need to make sure that there is a mock that will catch each of
      // the different param combinations we might pass to it.  Per Mockito docs, the last last match is the one that
      // will be used which is why we have the "generic" case first.
      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)

      // "generic" matcher will catch all calls to this method that don't match the "exception" case below
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        any[GoogleProjectId],
        any[Option[RawlsBillingAccountName]],
        any[Option[RawlsBillingAccountName]],
        any[Boolean]
      )).thenReturn(Future.successful(new ProjectBillingInfo()))

      val failureMessage = "because I feel like it"
      val exception = new RawlsException(failureMessage)
      // the "exception" case.  When method is called with these specific params, we want to Fail the Future.
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        ArgumentMatchers.eq(badWorkspace.googleProjectId),
        ArgumentMatchers.eq(Option(newBillingAccount)),
        any[Option[RawlsBillingAccountName]],
        any[Boolean]))
        .thenReturn(Future.failed(exception))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(workspace1))
      runAndWait(workspaceQuery.createOrUpdate(workspace2))
      runAndWait(workspaceQuery.createOrUpdate(badWorkspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(workspace1.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .currentBillingAccountOnGoogleProject shouldBe Option(newBillingAccount)
        runAndWait(workspaceQuery.findByName(workspace2.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .currentBillingAccountOnGoogleProject shouldBe Option(newBillingAccount)
        runAndWait(workspaceQuery.findByName(badWorkspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
      }

      system.stop(actor)
    }
  }

  // TODO: CA-1235 Remove during cleanup once all workspaces have their own Google project
  it should "propagate error messages to all workspaces in a Google project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val firstV1Workspace  = Workspace(billingProject.projectName.value, "first-v1-workspace",  UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val secondV1Workspace = Workspace(billingProject.projectName.value, "second-v1-workspace", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val v2Workspace = Workspace(billingProject.projectName.value, "v2 workspace", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")

      val newBillingAccount = RawlsBillingAccountName("new-ba")

      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)

      // We are going to mock this method that will get called multiple times with different params.  Need to implement
      // a "catch-all" mock first before implementing a specific param matcher that will change the behavior.  Last
      // matching param list wins per Mockito docs
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        any[GoogleProjectId],
        any[Option[RawlsBillingAccountName]],
        any[Option[RawlsBillingAccountName]],
        any[Boolean]
      )).thenReturn(Future.successful(new ProjectBillingInfo()))

      val failureMessage = "because I feel like it"
      val exception = new RawlsException(failureMessage)
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        secondV1Workspace.googleProjectId,
        Option(newBillingAccount),
        originalBillingAccount,
        false))
        .thenReturn(Future.failed(exception))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(firstV1Workspace))
      runAndWait(workspaceQuery.createOrUpdate(v2Workspace))
      runAndWait(workspaceQuery.createOrUpdate(secondV1Workspace))
      runAndWait(rawlsBillingProjectQuery.updateBillingAccount(billingProject.projectName, Option(newBillingAccount)))

      val actor = createWorkspaceBillingAccountMonitor(dataSource, failingGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findByName(secondV1Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
        runAndWait(workspaceQuery.findByName(firstV1Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe Option(failureMessage)
        runAndWait(workspaceQuery.findByName(v2Workspace.toWorkspaceName)).getOrElse(fail("workspace not found"))
          .billingAccountErrorMessage shouldBe None
      }
      // Note the final boolean on this "verify".  That is important because during initial workspace creation, the
      // "force" boolean will be true and we do not want to count those calls during this assertion.  We only want to
      // count the number of times this was called from the WorkspaceBillingAccountMonitor spec, and in that case, the
      // "force" boolean will be false.
      verify(failingGcsDAO, times(1)).updateGoogleProjectBillingAccount(secondV1Workspace.googleProjectId, Option(newBillingAccount), originalBillingAccount, false)

      system.stop(actor)
    }
  }

  it should "mark a billing project's billing account as invalid if Google returns a 403" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val originalBillingAccount = Option(defaultBillingAccountName)
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, originalBillingAccount, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val v1Workspace = Workspace(billingProject.projectName.value, "v1", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V1, GoogleProjectId(billingProject.projectName.value), billingProject.googleProjectNumber, originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val v2Workspace = Workspace(billingProject.projectName.value, "v2", UUID.randomUUID().toString, "bucketName", None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("differentId"), Option(GoogleProjectNumber("43")), originalBillingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded, "rawls")
      val newBillingAccount = RawlsBillingAccountName("new-ba")

      val failingGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)

      // We are going to mock this method that will get called multiple times with different params.  Need to implement
      // a "catch-all" mock first before implementing a specific param matcher that will change the behavior.  Last
      // matching param list wins per Mockito docs
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        any[GoogleProjectId],
        any[Option[RawlsBillingAccountName]],
        any[Option[RawlsBillingAccountName]],
        any[Boolean]
      )).thenReturn(Future.successful(new ProjectBillingInfo()))

      val failureMessage = "because I feel like it"
      val exception = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, failureMessage))
      when(failingGcsDAO.updateGoogleProjectBillingAccount(
        any[GoogleProjectId],
        ArgumentMatchers.eq(Option(newBillingAccount)),
        ArgumentMatchers.eq(originalBillingAccount),
        ArgumentMatchers.eq(false)))
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
