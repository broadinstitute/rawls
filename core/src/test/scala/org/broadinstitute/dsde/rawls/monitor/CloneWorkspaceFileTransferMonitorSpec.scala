package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import com.google.api.services.storage.model.StorageObject
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

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class CloneWorkspaceFileTransferMonitorSpec(_system: ActorSystem)
    extends TestKit(_system)
    with MockitoSugar
    with AnyFlatSpecLike
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll
    with Eventually
    with OptionValues {
  val defaultExecutionContext: ExecutionContext = executionContext
  def this() = this(ActorSystem("CloneWorkspaceFileTransferMonitorSpec"))

  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingAccountName: RawlsBillingAccountName = RawlsBillingAccountName("test-ba")

  override def beforeAll(): Unit =
    super.beforeAll()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private def createCloneWorkspaceFileTransferMonitor(dataSource: SlickDataSource, mockGcsDAO: GoogleServicesDAO)(
    implicit executionContext: ExecutionContext
  ) =
    system.actorOf(CloneWorkspaceFileTransferMonitor.props(dataSource, mockGcsDAO, 1 second, 1 second))

  "CloneWorkspaceFileTransferMonitor" should "eventually copy files from the source bucket to the destination bucket" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(defaultBillingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val objectToCopy = new StorageObject().setName("copy-me")
      val sourceWorkspace = Workspace(
        billingProject.projectName.value,
        "source",
        UUID.randomUUID().toString,
        sourceBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("some-project"),
        Option(GoogleProjectNumber("43")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val destWorkspace = Workspace(
        billingProject.projectName.value,
        "destination",
        UUID.randomUUID().toString,
        destinationBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("different-project"),
        Option(GoogleProjectNumber("44")),
        billingProject.billingAccount,
        None,
        None,
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(
        cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID,
                                             sourceWorkspace.workspaceIdAsUUID,
                                             copyFilesWithPrefix
        )
      )

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(
        mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId))
      )
        .thenReturn(Future.successful(List(objectToCopy)))
      when(
        mockGcsDAO.copyFile(sourceBucketName,
                            objectToCopy.getName,
                            destinationBucketName,
                            objectToCopy.getName,
                            Option(destWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
      )
        .thenReturn(Future.successful(Option(objectToCopy)))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually(timeout = timeout(Span(10, Seconds))) {
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer
          .isDefined shouldBe true
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()) shouldBe empty
      }
      verify(mockGcsDAO, times(1)).copyFile(sourceBucketName,
                                            objectToCopy.getName,
                                            destinationBucketName,
                                            objectToCopy.getName,
                                            Option(destWorkspace.googleProjectId)
      )(system.dispatchers.defaultGlobalDispatcher)

      system.stop(actor)
    }
  }

  it should "continue trying to transfer files when receiving 403s from Google while listing objects" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(defaultBillingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val sourceWorkspace = Workspace(
        billingProject.projectName.value,
        "source",
        UUID.randomUUID().toString,
        sourceBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("some-project"),
        Option(GoogleProjectNumber("43")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val destWorkspace = Workspace(
        billingProject.projectName.value,
        "destination",
        UUID.randomUUID().toString,
        destinationBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("different-project"),
        Option(GoogleProjectNumber("44")),
        billingProject.billingAccount,
        None,
        None,
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(
        cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID,
                                             sourceWorkspace.workspaceIdAsUUID,
                                             copyFilesWithPrefix
        )
      )

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new HttpResponseException.Builder(403, failureMessage, new HttpHeaders()).build
      when(
        mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId))
      )
        .thenReturn(Future.failed(exception))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually(timeout = timeout(Span(10, Seconds))) {
        verify(mockGcsDAO, Mockito.atLeast(5)).listObjectsWithPrefix(sourceBucketName,
                                                                     copyFilesWithPrefix,
                                                                     Option(destWorkspace.googleProjectId)
        )
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer
          .isDefined shouldBe false
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()).isEmpty shouldBe false
      }

      system.stop(actor)
    }
  }

  it should "continue trying to transfer files when receiving 403s from Google while copying objects" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(defaultBillingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val objectToCopy = new StorageObject().setName("copy-me")
      val sourceWorkspace = Workspace(
        billingProject.projectName.value,
        "source",
        UUID.randomUUID().toString,
        sourceBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("some-project"),
        Option(GoogleProjectNumber("43")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val destWorkspace = Workspace(
        billingProject.projectName.value,
        "destination",
        UUID.randomUUID().toString,
        destinationBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("different-project"),
        Option(GoogleProjectNumber("44")),
        billingProject.billingAccount,
        None,
        None,
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(
        cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID,
                                             sourceWorkspace.workspaceIdAsUUID,
                                             copyFilesWithPrefix
        )
      )

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new HttpResponseException.Builder(403, failureMessage, new HttpHeaders()).build
      when(
        mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId))
      )
        .thenReturn(Future.successful(List(objectToCopy)))
      when(
        mockGcsDAO.copyFile(sourceBucketName,
                            objectToCopy.getName,
                            destinationBucketName,
                            objectToCopy.getName,
                            Option(destWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
      )
        .thenReturn(Future.failed(exception))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually(timeout = timeout(Span(10, Seconds))) {
        verify(mockGcsDAO, Mockito.atLeast(5)).copyFile(sourceBucketName,
                                                        objectToCopy.getName,
                                                        destinationBucketName,
                                                        objectToCopy.getName,
                                                        Option(destWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer
          .isDefined shouldBe false
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()).isEmpty shouldBe false
      }

      system.stop(actor)
    }
  }

  it should "continue trying to copy the files until all copies succeed" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(defaultBillingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val sourceBucketName = "sourceBucket"
      val destinationBucketName = "destinationBucket"
      val copyFilesWithPrefix = "prefix"
      val badObjectToCopy = new StorageObject().setName("you-cant-copy-me")
      val goodObjectToCopy = new StorageObject().setName("copy-me")
      val sourceWorkspace = Workspace(
        billingProject.projectName.value,
        "source",
        UUID.randomUUID().toString,
        sourceBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("some-project"),
        Option(GoogleProjectNumber("43")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val destWorkspace = Workspace(
        billingProject.projectName.value,
        "destination",
        UUID.randomUUID().toString,
        destinationBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("different-project"),
        Option(GoogleProjectNumber("44")),
        billingProject.billingAccount,
        None,
        None,
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(destWorkspace))
      runAndWait(
        cloneWorkspaceFileTransferQuery.save(destWorkspace.workspaceIdAsUUID,
                                             sourceWorkspace.workspaceIdAsUUID,
                                             copyFilesWithPrefix
        )
      )

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new HttpResponseException.Builder(403, failureMessage, new HttpHeaders()).build
      when(
        mockGcsDAO.listObjectsWithPrefix(sourceBucketName, copyFilesWithPrefix, Option(destWorkspace.googleProjectId))
      )
        .thenReturn(Future.successful(List(badObjectToCopy, goodObjectToCopy)))
      when(
        mockGcsDAO.copyFile(sourceBucketName,
                            badObjectToCopy.getName,
                            destinationBucketName,
                            badObjectToCopy.getName,
                            Option(destWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
      )
        .thenReturn(Future.failed(exception))
      when(
        mockGcsDAO.copyFile(sourceBucketName,
                            goodObjectToCopy.getName,
                            destinationBucketName,
                            goodObjectToCopy.getName,
                            Option(destWorkspace.googleProjectId)
        )
      )
        .thenReturn(Future.successful(Option(goodObjectToCopy)))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually(timeout = timeout(Span(10, Seconds))) {
        verify(mockGcsDAO, Mockito.atLeast(5)).copyFile(sourceBucketName,
                                                        badObjectToCopy.getName,
                                                        destinationBucketName,
                                                        badObjectToCopy.getName,
                                                        Option(destWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
        verify(mockGcsDAO, Mockito.atLeast(5)).copyFile(sourceBucketName,
                                                        goodObjectToCopy.getName,
                                                        destinationBucketName,
                                                        goodObjectToCopy.getName,
                                                        Option(destWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
        runAndWait(workspaceQuery.findById(destWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${destWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer
          .isDefined shouldBe false
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers()).isEmpty shouldBe false
      }

      system.stop(actor)
    }
  }

  it should "copy files for good workspaces even when problematic workspaces are failing" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName,
                                               CreationStatuses.Ready,
                                               Option(defaultBillingAccountName),
                                               None,
                                               googleProjectNumber = Option(defaultGoogleProjectNumber)
      )
      val sourceBucketName = "sourceBucket"
      val badDestinationBucketName = "badBucket"
      val goodDestinationBucketName = "goodBucket"
      val copyFilesWithPrefix = "prefix"
      val objectToCopy = new StorageObject().setName("copy-me")
      val sourceWorkspace = Workspace(
        billingProject.projectName.value,
        "source",
        UUID.randomUUID().toString,
        sourceBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("some-project"),
        Option(GoogleProjectNumber("43")),
        billingProject.billingAccount,
        None,
        Option(DateTime.now),
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val badDestWorkspace = Workspace(
        billingProject.projectName.value,
        "badDestination",
        UUID.randomUUID().toString,
        badDestinationBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("different-project"),
        Option(GoogleProjectNumber("44")),
        billingProject.billingAccount,
        None,
        None,
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )
      val goodDestWorkspace = Workspace(
        billingProject.projectName.value,
        "goodDestination",
        UUID.randomUUID().toString,
        goodDestinationBucketName,
        None,
        DateTime.now,
        DateTime.now,
        "creator@example.com",
        Map.empty,
        false,
        WorkspaceVersions.V2,
        GoogleProjectId("another-project"),
        Option(GoogleProjectNumber("45")),
        billingProject.billingAccount,
        None,
        None,
        WorkspaceType.RawlsWorkspace,
        WorkspaceState.Ready
      )

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(workspaceQuery.createOrUpdate(sourceWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(badDestWorkspace))
      runAndWait(workspaceQuery.createOrUpdate(goodDestWorkspace))
      runAndWait(
        cloneWorkspaceFileTransferQuery.save(badDestWorkspace.workspaceIdAsUUID,
                                             sourceWorkspace.workspaceIdAsUUID,
                                             copyFilesWithPrefix
        )
      )
      runAndWait(
        cloneWorkspaceFileTransferQuery.save(goodDestWorkspace.workspaceIdAsUUID,
                                             sourceWorkspace.workspaceIdAsUUID,
                                             copyFilesWithPrefix
        )
      )

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val failureMessage = "because I feel like it"
      val exception = new HttpResponseException.Builder(403, failureMessage, new HttpHeaders()).build
      when(
        mockGcsDAO.listObjectsWithPrefix(sourceBucketName,
                                         copyFilesWithPrefix,
                                         Option(badDestWorkspace.googleProjectId)
        )
      )
        .thenReturn(Future.successful(List(objectToCopy)))
      when(
        mockGcsDAO.listObjectsWithPrefix(sourceBucketName,
                                         copyFilesWithPrefix,
                                         Option(goodDestWorkspace.googleProjectId)
        )
      )
        .thenReturn(Future.successful(List(objectToCopy)))
      when(
        mockGcsDAO.copyFile(sourceBucketName,
                            objectToCopy.getName,
                            badDestinationBucketName,
                            objectToCopy.getName,
                            Option(badDestWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
      )
        .thenReturn(Future.failed(exception))
      when(
        mockGcsDAO.copyFile(sourceBucketName,
                            objectToCopy.getName,
                            goodDestinationBucketName,
                            objectToCopy.getName,
                            Option(goodDestWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
      )
        .thenReturn(Future.successful(Option(objectToCopy)))

      val actor = createCloneWorkspaceFileTransferMonitor(dataSource, mockGcsDAO)

      eventually(timeout = timeout(Span(10, Seconds))) {
        verify(mockGcsDAO, Mockito.atLeast(5)).copyFile(sourceBucketName,
                                                        objectToCopy.getName,
                                                        badDestinationBucketName,
                                                        objectToCopy.getName,
                                                        Option(badDestWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
        verify(mockGcsDAO, times(1)).copyFile(sourceBucketName,
                                              objectToCopy.getName,
                                              goodDestinationBucketName,
                                              objectToCopy.getName,
                                              Option(goodDestWorkspace.googleProjectId)
        )(system.dispatchers.defaultGlobalDispatcher)
        runAndWait(workspaceQuery.findById(badDestWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${badDestWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer
          .isDefined shouldBe false
        runAndWait(workspaceQuery.findById(goodDestWorkspace.workspaceIdAsUUID.toString))
          .getOrElse(fail(s"${goodDestWorkspace.name} not found"))
          .completedCloneWorkspaceFileTransfer
          .isDefined shouldBe true
        runAndWait(cloneWorkspaceFileTransferQuery.listPendingTransfers())
          .map(_.destWorkspaceBucketName) should contain theSameElementsAs Seq(badDestinationBucketName)
      }

      system.stop(actor)
    }
  }
}
