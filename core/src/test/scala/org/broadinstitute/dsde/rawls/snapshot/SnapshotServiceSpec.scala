package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model._
import cats.effect.IO
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.deltalayer.{DeltaLayer, MockDeltaLayerWriter}
import org.mockito.Mockito.{RETURNS_SMART_NULLS, spy, times, verify, when}
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, GoogleProjectId, NamedDataRepoSnapshot, SamPolicy, SamPolicyWithNameAndEmail, SamResourceAction, SamResourceTypeName, SamResourceTypeNames, SamWorkspacePolicyNames, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.Duration

class SnapshotServiceSpec extends AnyWordSpecLike with Matchers with MockitoSugar with TestDriverComponent {

  implicit val cs = IO.contextShift(global)

  //test constants
  val fakeRawlsClientEmail = WorkbenchEmail("fake-rawls-service-account@serviceaccounts.google.com")
  val fakeDeltaLayerStreamerEmail = WorkbenchEmail("fake-rawls-service-account@serviceaccounts.google.com")


  "SnapshotService" should {
    "create a new snapshot reference to a TDR snapshot" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))
      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))
      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.successful("fake-token"))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
      when(mockWorkspaceManagerDAO.createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken]))
        .thenReturn(new DataRepoSnapshotResource().metadata(new ResourceMetadata().resourceId(UUID.randomUUID()).workspaceId(UUID.randomUUID()).name("foo").description("").cloningInstructions(CloningInstructionsEnum.NOTHING)).attributes(new DataRepoSnapshotAttributes()))

      val workspace = minimalTestData.workspace
      val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        deltaLayer,
        "fake-terra-data-repo-dev",
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), UUID.randomUUID())), Duration.Inf)

      verify(mockSamDAO, times(1)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(1)).getServiceForProject(workspace.googleProject)
      verify(mockWorkspaceManagerDAO, times(1)).createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(1)).createBigQueryDatasetReference(any[UUID], any[ReferenceResourceCommonFields], any[GcpBigQueryDatasetAttributes], any[OAuth2BearerToken])
    }

    "not leave an orphaned bq dataset if creating a snapshot reference fails" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))

      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))

      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.successful("fake-token"))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      when(mockWorkspaceManagerDAO.createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken])).thenThrow(new RuntimeException("oh no!"))

      val workspace = minimalTestData.workspace
      val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        deltaLayer,
        "fake-terra-data-repo-dev",
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      val createException = intercept[RuntimeException] {
        Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), UUID.randomUUID())), Duration.Inf)
      }
      createException.getMessage shouldBe "oh no!"

      verify(mockSamDAO, times(0)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(0)).getServiceForProject(workspace.googleProject)
      verify(mockWorkspaceManagerDAO, times(1)).createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(0)).createBigQueryDatasetReference(any[UUID], any[ReferenceResourceCommonFields], any[GcpBigQueryDatasetAttributes], any[OAuth2BearerToken])
    }

    "not leave an orphaned snapshot data reference if creating a BQ dataset in Google fails" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))

      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))

      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.successful("fake-token"))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenThrow(new RuntimeException)

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      when(mockWorkspaceManagerDAO.createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken]))
        .thenReturn(new DataRepoSnapshotResource().metadata(new ResourceMetadata().resourceId(UUID.randomUUID()).workspaceId(UUID.randomUUID()).name("foo").description("").cloningInstructions(CloningInstructionsEnum.NOTHING)).attributes(new DataRepoSnapshotAttributes()))

      val workspace = minimalTestData.workspace
      val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        deltaLayer,
        "fake-terra-data-repo-dev",
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      val createException = intercept[RawlsExceptionWithErrorReport] {
        Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), UUID.randomUUID())), Duration.Inf)
      }
      createException.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
      assert(createException.errorReport.message.contains("Unable to create snapshot reference in workspace"))

      verify(mockSamDAO, times(1)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(1)).getServiceForProject(workspace.googleProject)
      verify(mockWorkspaceManagerDAO, times(1)).createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(1)).deleteDataRepoSnapshotReference(any[UUID], any[UUID], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(0)).createBigQueryDatasetReference(any[UUID], any[ReferenceResourceCommonFields], any[GcpBigQueryDatasetAttributes], any[OAuth2BearerToken])
    }

    "not leave an orphaned snapshot data reference nor an orphaned BQ dataset if creating a BQ data reference in WSM fails" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))

      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))

      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.successful("fake-token"))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      when(mockWorkspaceManagerDAO.createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken]))
        .thenReturn(new DataRepoSnapshotResource().metadata(new ResourceMetadata().resourceId(UUID.randomUUID()).workspaceId(UUID.randomUUID()).name("foo").description("").cloningInstructions(CloningInstructionsEnum.NOTHING)).attributes(new DataRepoSnapshotAttributes()))

      when(mockWorkspaceManagerDAO.createBigQueryDatasetReference(any[UUID], any[ReferenceResourceCommonFields], any[GcpBigQueryDatasetAttributes], any[OAuth2BearerToken])).thenThrow(new RuntimeException)

      val workspace = minimalTestData.workspace
      val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        deltaLayer,
        "fake-terra-data-repo-dev",
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      val createException = intercept[RawlsExceptionWithErrorReport] {
        Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), UUID.randomUUID())), Duration.Inf)
      }
      createException.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
      assert(createException.errorReport.message.contains("Unable to create snapshot reference in workspace"))

      verify(mockSamDAO, times(1)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(2)).getServiceForProject(workspace.googleProject)
      verify(mockWorkspaceManagerDAO, times(1)).createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(1)).createBigQueryDatasetReference(any[UUID], any[ReferenceResourceCommonFields], any[GcpBigQueryDatasetAttributes], any[OAuth2BearerToken])

      // see SnapshotService line 65, "fire and forget these undos, we've made our best effort to fix things at this point"
      // because the call to workspaceManagerDAO.deleteDataRepoSnapshotReference is fire-and-forget, we can't count on it happening
      // by this point of this test. We need an eventually here:
      eventually {
        verify(mockWorkspaceManagerDAO, times(1)).deleteDataRepoSnapshotReference(any[UUID], any[UUID], any[OAuth2BearerToken])
      }

    }

    "not leave an orphaned snapshot data reference nor an orphaned BQ dataset if getting a pet token from Sam fails" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))

      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))

      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.failed(new RuntimeException))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      when(mockWorkspaceManagerDAO.createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken]))
        .thenReturn(new DataRepoSnapshotResource().metadata(new ResourceMetadata().resourceId(UUID.randomUUID()).workspaceId(UUID.randomUUID()).name("foo").description("").cloningInstructions(CloningInstructionsEnum.NOTHING)).attributes(new DataRepoSnapshotAttributes()))

      val workspace = minimalTestData.workspace
      val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        deltaLayer,
        "fake-terra-data-repo-dev",
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      val createException = intercept[RawlsExceptionWithErrorReport] {
        Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), UUID.randomUUID())), Duration.Inf)
      }
      createException.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
      assert(createException.errorReport.message.contains("Unable to create snapshot reference in workspace"))

      verify(mockSamDAO, times(1)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(2)).getServiceForProject(workspace.googleProject)
      verify(mockWorkspaceManagerDAO, times(1)).createDataRepoSnapshotReference(any[UUID], any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[String], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(1)).deleteDataRepoSnapshotReference(any[UUID], any[UUID], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(0)).createBigQueryDatasetReference(any[UUID], any[ReferenceResourceCommonFields], any[GcpBigQueryDatasetAttributes], any[OAuth2BearerToken])
    }

    "remove all resources when a snapshot reference is deleted" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))
      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      val snapshotDataReferenceId = UUID.randomUUID()
      when(mockWorkspaceManagerDAO.getDataRepoSnapshotReference(any[UUID], any[UUID], any[OAuth2BearerToken]))
        .thenReturn(new DataRepoSnapshotResource().metadata(new ResourceMetadata().resourceId(snapshotDataReferenceId).workspaceId(UUID.randomUUID()).name("foo").description("").cloningInstructions(CloningInstructionsEnum.NOTHING)).attributes(new DataRepoSnapshotAttributes()))

      when(mockWorkspaceManagerDAO.getBigQueryDatasetReferenceByName(any[UUID], any[String], any[OAuth2BearerToken]))
        .thenReturn(new GcpBigQueryDatasetResource().metadata(new ResourceMetadata().resourceId(UUID.randomUUID())).attributes(new GcpBigQueryDatasetAttributes()))

      val workspace = minimalTestData.workspace
      val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        deltaLayer,
        "fake-terra-data-repo-dev",
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      val deltaLayerDatasetName = DeltaLayer.generateDatasetNameForWorkspace(workspace)

      val snapshotUUID = UUID.randomUUID()

      Await.result(snapshotService.deleteSnapshot(workspace.toWorkspaceName, snapshotUUID.toString), Duration.Inf)

      verify(mockWorkspaceManagerDAO, times(1)).getDataRepoSnapshotReference(ArgumentMatchers.eq(workspace.workspaceIdAsUUID), ArgumentMatchers.eq(snapshotUUID),any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(1)).deleteDataRepoSnapshotReference(ArgumentMatchers.eq(workspace.workspaceIdAsUUID), ArgumentMatchers.eq(snapshotUUID), any[OAuth2BearerToken])
      // assert we do NOT attempt to delete the companion dataset or its WSM reference
      verify(mockWorkspaceManagerDAO, times(0)).getBigQueryDatasetReferenceByName(ArgumentMatchers.eq(workspace.workspaceIdAsUUID), ArgumentMatchers.eq(deltaLayerDatasetName), any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(0)).deleteBigQueryDatasetReference(ArgumentMatchers.eq(workspace.workspaceIdAsUUID), any[UUID], any[OAuth2BearerToken])
      verify(mockBigQueryServiceFactory, times(0)).getServiceForProject(any[GoogleProjectId])

    }

    "find one matching snapshot reference by referenced snapshotId if one page of references" in withMinimalTestDatabase { _ =>
      // generate a single page of ResourceDescriptions
      val resources = generateTestReferences(20)

      val snapshotService = mockSnapshotServiceForReferences(resources)

      // search for one of the snapshotIds that should be in the list
      val criteria = "00000000-0000-0000-0000-000000000012"
      val found = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria)),
        Duration.Inf).gcpDataRepoSnapshots

      found should have size 1
      found.head.getAttributes.getSnapshot shouldBe criteria
    }

    "find multiple matching snapshot references by referenced snapshotId if one page of references" in withMinimalTestDatabase { _ =>
      // generate a single page of ResourceDescriptions, but duplicate the first 10 snapshotIds
      val resources = generateTestReferences(20) ++ generateTestReferences(10)

      val snapshotService = mockSnapshotServiceForReferences(resources)

      // search for one of the snapshotIds that should be duplicated
      val criteria1 = "00000000-0000-0000-0000-000000000005"
      val found1 = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria1)),
        Duration.Inf).gcpDataRepoSnapshots

      found1 should have size 2
      found1.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria1
      }

      // now search for one of the snapshotIds that should NOT be duplicated, to be sure
      val criteria2 = "00000000-0000-0000-0000-000000000015"
      val found2 = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria2)),
        Duration.Inf).gcpDataRepoSnapshots

      found2 should have size 1
      found2.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria2
      }

    }

    "return an empty list of snapshot references by referenced snapshotId if one page of references" in withMinimalTestDatabase { _ =>
      // generate a single page of ResourceDescriptions
      val resources = generateTestReferences(20)

      val snapshotService = mockSnapshotServiceForReferences(resources)

      // search for one of the snapshotIds that should NOT be in the list
      val criteria = "00000000-0000-0000-0000-000000000099"
      val found = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria)),
        Duration.Inf).gcpDataRepoSnapshots

      found should have size 0
    }

    "find one matching snapshot reference by referenced snapshotId if multiple pages of references" in withMinimalTestDatabase { _ =>
      // 10 resources and batchsize of 3 means we should make 4 queries to WSM
      val resources = generateTestReferences(10)
      val batchSize = 3

      // yes we're spying on a mock
      val snapshotService = spy(mockSnapshotServiceForReferences(resources))

      // search for one of the snapshotIds that should be in the list
      val criteria = "00000000-0000-0000-0000-000000000006"
      val found = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria), batchSize),
        Duration.Inf).gcpDataRepoSnapshots

      found should have size 1
      found.head.getAttributes.getSnapshot shouldBe criteria

      // ensure we paged through the resources properly
      verify(snapshotService, times(4))
        .retrieveSnapshotReferences(ArgumentMatchers.eq(minimalTestData.workspace.workspaceIdAsUUID), any[Int], ArgumentMatchers.eq(batchSize))
    }

    "find multiple matching snapshot references by referenced snapshotId if multiple pages of references" in withMinimalTestDatabase { _ =>
      // 21 resources total and batchsize of 5 means we should make 5 queries to WSM
      val resources = generateTestReferences(10) ++ // contains snapshotId "...0006"
        generateTestReferences(4) ++                // does not contain snapshotId "...0006"
        generateTestReferences(7)                   // contains snapshotId "...0006"
      val batchSize = 5

      // yes we're spying on a mock
      val snapshotService = spy(mockSnapshotServiceForReferences(resources))

      // search for one of the snapshotIds that should be in the list
      val criteria = "00000000-0000-0000-0000-000000000006"
      val found = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria), batchSize),
        Duration.Inf).gcpDataRepoSnapshots

      found should have size 2
      found.foreach { x =>
        x.getAttributes.getSnapshot shouldBe criteria
      }

      // ensure we paged through the resources properly
      verify(snapshotService, times(5))
        .retrieveSnapshotReferences(ArgumentMatchers.eq(minimalTestData.workspace.workspaceIdAsUUID), any[Int], ArgumentMatchers.eq(batchSize))
    }

    "return an empty list of snapshot references by referenced snapshotId if multiple pages of references" in withMinimalTestDatabase { _ =>
      // 10 resources and batchsize of 3 means we should make 4 queries to WSM
      val resources = generateTestReferences(10)
      val batchSize = 3

      // yes we're spying on a mock
      val snapshotService = spy(mockSnapshotServiceForReferences(resources))

      // search for one of the snapshotIds that should be in the list
      val criteria = "00000000-0000-0000-0000-000000000099"
      val found = Await.result(
        snapshotService.findBySnapshotId(minimalTestData.workspace.toWorkspaceName, UUID.fromString(criteria), batchSize),
        Duration.Inf).gcpDataRepoSnapshots

      found should have size 0

      // ensure we paged through the resources properly
      verify(snapshotService, times(4))
        .retrieveSnapshotReferences(ArgumentMatchers.eq(minimalTestData.workspace.workspaceIdAsUUID), any[Int], ArgumentMatchers.eq(batchSize))
    }

  }

  def generateTestReferences(numReferences: Int): List[ResourceDescription] = {
    (1 to numReferences).toList.map { idx =>
      val paddedIdx = "%08d".format(idx)

      val metadata = new ResourceMetadata()
      metadata.setResourceType(ResourceType.DATA_REPO_SNAPSHOT)
      metadata.setName(s"snapshot_reference_$idx")
      metadata.setResourceId(UUID.randomUUID())
      metadata.setWorkspaceId(minimalTestData.workspace.workspaceIdAsUUID)

      val snaprefAttrs = new DataRepoSnapshotAttributes()
      snaprefAttrs.setSnapshot(s"00000000-0000-0000-0000-0000$paddedIdx")
      snaprefAttrs.setInstanceName("terra")

      val attrsUnion = new ResourceAttributesUnion()
      attrsUnion.setGcpDataRepoSnapshot(snaprefAttrs)

      val rd = new ResourceDescription()
      rd.setMetadata(metadata)
      rd.setResourceAttributes(attrsUnion)

      rd
    }
  }

  def mockSnapshotServiceForReferences(resources: List[ResourceDescription]): SnapshotService = {
    // mock sam that always says we have permission
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))
    // mock GoogleBigQueryServiceFactory
    val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
    when(mockBigQueryServiceFactory.getServiceForProject(any[GoogleProjectId])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))
    // mock WorkspaceManagerDAO, don't set up any method responses yet
    val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

    when(mockWorkspaceManagerDAO.enumerateDataRepoSnapshotReferences(any[UUID], any[Int], any[Int], any[OAuth2BearerToken]))
      .thenAnswer{ answer =>
        val offset = answer.getArgument[Int](1)
        val limit = answer.getArgument[Int](2)
        val resList = new ResourceList()
        resList.setResources(resources.slice(offset, offset+limit).asJava)
        resList
      }

    val deltaLayer = new DeltaLayer(mockBigQueryServiceFactory, new MockDeltaLayerWriter, mockSamDAO, fakeRawlsClientEmail, fakeDeltaLayerStreamerEmail)

    SnapshotService.constructor(
      slickDataSource,
      mockSamDAO,
      mockWorkspaceManagerDAO,
      deltaLayer,
      "fake-terra-data-repo-dev",
      fakeRawlsClientEmail,
      fakeDeltaLayerStreamerEmail
    )(userInfo)

  }


}
