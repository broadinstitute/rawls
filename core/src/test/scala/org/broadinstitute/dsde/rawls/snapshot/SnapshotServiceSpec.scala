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
import org.mockito.Mockito.{RETURNS_SMART_NULLS, times, verify, when}
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

  }

}
