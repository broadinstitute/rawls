package org.broadinstitute.dsde.rawls.monitor

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import cats.effect.{ContextShift, IO}
import com.google.api.services.storage.model.StorageObject
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import org.mockito.Mockito
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

class CloneWorkspaceFileTransferMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with OptionValues {
  val defaultExecutionContext: ExecutionContext = executionContext
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  def this() = this(ActorSystem("CloneWorkspaceFileTransferMonitorSpec"))

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

  private def createCloneWorkspaceFileTransferMonitor(dataSource: SlickDataSource,
                                                      mockGcsDAO: GoogleServicesDAO)
                                                     (implicit executionContext: ExecutionContext) = {
    system.actorOf(CloneWorkspaceFileTransferMonitor.props(dataSource, mockGcsDAO, 1 second, 1 second))
  }

  "CloneWorkspaceFileTransferMonitor" should "eventually copy files from the source bucket to the destination bucket" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val objectToCopy = new StorageObject().setName("copy-me")
      val sourceWorkspace = Workspace(billingProject.projectName.value, "source", UUID.randomUUID().toString, sourceBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("some-project"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val destWorkspace = Workspace(billingProject.projectName.value, "destination", UUID.randomUUID().toString, destinationBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("different-project"), Option(GoogleProjectNumber("44")), billingProject.billingAccount, None, None, WorkspaceShardStates.Sharded)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID, sourceWorkspace.workspaceIdAsUUID, copyFilesWithPrefix))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId)))
        .thenReturn(Future.successful(List(objectToCopy)))
      when(mockGcsDAO.copyFile(sourceBucketName, objectToCopy.getName, destinationBucketName, objectToCopy.getName, Option(destWorkspace.googleProjectId)))
        .thenReturn(Future.successful(Option(objectToCopy)))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer.isDefined shouldBe true
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()) shouldBe empty
      }
      verify(mockGcsDAO, times(1)).copyFile(sourceBucketName, objectToCopy.getName, destinationBucketName, objectToCopy.getName, Option(destWorkspace.googleProjectId))

      system.stop(actor)
    }
  }

  it should "continue trying to transfer files when receiving 403s from Google while listing objects" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val sourceWorkspace = Workspace(billingProject.projectName.value, "source", UUID.randomUUID().toString, sourceBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("some-project"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val destWorkspace = Workspace(billingProject.projectName.value, "destination", UUID.randomUUID().toString, destinationBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("different-project"), Option(GoogleProjectNumber("44")), billingProject.billingAccount, None, None, WorkspaceShardStates.Sharded)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID, sourceWorkspace.workspaceIdAsUUID, copyFilesWithPrefix))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, failureMessage))
      when(mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId)))
        .thenReturn(Future.failed(exception))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        verify(mockGcsDAO, Mockito.atLeast(5)).listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId))
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer.isDefined shouldBe false
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()).isEmpty shouldBe false
      }

      system.stop(actor)
    }
  }

  it should "continue trying to transfer files when receiving 403s from Google while copying objects" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, Option(defaultBillingAccountName), None, googleProjectNumber = Option(defaultGoogleProjectNumber))
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val objectToCopy = new StorageObject().setName("copy-me")
      val sourceWorkspace = Workspace(billingProject.projectName.value, "source", UUID.randomUUID().toString, sourceBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("some-project"), Option(GoogleProjectNumber("43")), billingProject.billingAccount, None, Option(DateTime.now), WorkspaceShardStates.Sharded)
      val destWorkspace = Workspace(billingProject.projectName.value, "destination", UUID.randomUUID().toString, destinationBucketName, None, DateTime.now, DateTime.now, "creator@example.com", Map.empty, false, WorkspaceVersions.V2, GoogleProjectId("different-project"), Option(GoogleProjectNumber("44")), billingProject.billingAccount, None, None, WorkspaceShardStates.Sharded)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID, sourceWorkspace.workspaceIdAsUUID, copyFilesWithPrefix))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, failureMessage))
      when(mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId)))
        .thenReturn(Future.successful(List(objectToCopy)))
      when(mockGcsDAO.copyFile(sourceBucketName, objectToCopy.getName, destinationBucketName, objectToCopy.getName, Option(destWorkspace.googleProjectId)))
        .thenReturn(Future.failed(exception))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually (timeout = timeout(Span(10, Seconds))) {
        verify(mockGcsDAO, Mockito.atLeast(5)).copyFile(sourceBucketName, objectToCopy.getName, destinationBucketName, objectToCopy.getName, Option(destWorkspace.googleProjectId))
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer.isDefined shouldBe false
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()).isEmpty shouldBe false
      }

      system.stop(actor)
    }
  }
}
