package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{CloningInstructionsEnum, DataReferenceDescription, DataReferenceRequestMetadata, DataRepoSnapshot, GoogleBigQueryDatasetUid, ReferenceTypeEnum}
import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.mockito.Mockito.{RETURNS_SMART_NULLS, doThrow, times, verify, when}
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, ErrorReport, GoogleProjectId, NamedDataRepoSnapshot, SamPolicy, SamPolicyWithNameAndEmail, SamResourceAction, SamResourceTypeName, SamResourceTypeNames, SamWorkspacePolicyNames, UserInfo}
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
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
  val fakeCredentialPath = "/fake/Credential/Path/credentials.json"
  val fakeRawlsClientEmail = WorkbenchEmail("fake-rawls-service-account@serviceaccounts.google.com")
  val fakeDeltaLayerStreamerEmail = WorkbenchEmail("fake-rawls-service-account@serviceaccounts.google.com")


  "SnapshotService" should {
    "create a new snapshot reference to a TDR snapshot" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))
      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))
      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.successful("fake-token"))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceFromCredentialPath(any[String], any[GoogleProject])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
      when(mockWorkspaceManagerDAO.createDataReference(any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[ReferenceTypeEnum], any[DataRepoSnapshot], any[CloningInstructionsEnum], any[OAuth2BearerToken])).thenReturn(new DataReferenceDescription().referenceId(UUID.randomUUID()).name("foo").description("").workspaceId(UUID.randomUUID()).cloningInstructions(CloningInstructionsEnum.NOTHING))

      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        mockBigQueryServiceFactory,
        "fake-terra-data-repo-dev",
        fakeCredentialPath,
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), "bar")), Duration.Inf)

      verify(mockSamDAO, times(1)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(1)).getServiceFromCredentialPath(fakeCredentialPath, GoogleProject(workspace.namespace))
      verify(mockWorkspaceManagerDAO, times(1)).createDataReference(any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[ReferenceTypeEnum], any[DataRepoSnapshot], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(1)).createBigQueryDataset(any[UUID], any[DataReferenceRequestMetadata], any[GoogleBigQueryDatasetUid], any[OAuth2BearerToken])
    }

  }

  "SnapshotService" should {
    "not leave an orphaned bq dataset if creating a snapshot reference fails" in withMinimalTestDatabase { dataSource =>
      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)

      when(mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))

      when(mockSamDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[String], any[UserInfo])).thenReturn(Future.successful(Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail(userInfo.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")))))

      when(mockSamDAO.getPetServiceAccountToken(any[GoogleProjectId], any[Set[String]], any[UserInfo])).thenReturn(Future.successful("fake-token"))

      val mockBigQueryServiceFactory = mock[GoogleBigQueryServiceFactory](RETURNS_SMART_NULLS)
      when(mockBigQueryServiceFactory.getServiceFromCredentialPath(any[String], any[GoogleProject])).thenReturn(MockBigQueryServiceFactory.ioFactory().getServiceForPet("foo", GoogleProject("foo")))

      val mockWorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)

      when(mockWorkspaceManagerDAO.createDataReference(any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[ReferenceTypeEnum], any[DataRepoSnapshot], any[CloningInstructionsEnum], any[OAuth2BearerToken])).thenThrow(new RuntimeException)

      val workspace = minimalTestData.workspace

      val snapshotService = SnapshotService.constructor(
        slickDataSource,
        mockSamDAO,
        mockWorkspaceManagerDAO,
        mockBigQueryServiceFactory,
        "fake-terra-data-repo-dev",
        fakeCredentialPath,
        fakeRawlsClientEmail,
        fakeDeltaLayerStreamerEmail
      )(userInfo)

      try {
        Await.result(snapshotService.createSnapshot(workspace.toWorkspaceName, NamedDataRepoSnapshot(DataReferenceName("foo"), Option(DataReferenceDescriptionField("foo")), "bar")), Duration.Inf)
      }
      catch {
        case e: RuntimeException => "WSM snapshot info not returned"
      }

      verify(mockSamDAO, times(0)).listPoliciesForResource(any[SamResourceTypeName], any[String], any[UserInfo])
      verify(mockBigQueryServiceFactory, times(0)).getServiceFromCredentialPath(fakeCredentialPath, GoogleProject(workspace.namespace))
      verify(mockWorkspaceManagerDAO, times(1)).createDataReference(any[UUID], any[DataReferenceName], any[Option[DataReferenceDescriptionField]], any[ReferenceTypeEnum], any[DataRepoSnapshot], any[CloningInstructionsEnum], any[OAuth2BearerToken])
      verify(mockWorkspaceManagerDAO, times(0)).createBigQueryDataset(any[UUID], any[DataReferenceRequestMetadata], any[GoogleBigQueryDatasetUid], any[OAuth2BearerToken])
    }

  }

}
