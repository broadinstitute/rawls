package org.broadinstitute.dsde.rawls.snapshot

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.datarepo.client
import bio.terra.datarepo.model.{
  CloudPlatform => SnapshotCloudPlatform,
  DatasetSummaryModel,
  SnapshotModel,
  SnapshotSourceModel
}
import bio.terra.policy.model.{TpsPaoGetResult, TpsPolicyInput, TpsPolicyInputs, TpsPolicyPair}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
import org.broadinstitute.dsde.rawls.model.{
  RawlsRequestContext,
  SamResourceAction,
  SamResourceTypeName,
  SamResourceTypeNames,
  SamUserStatusResponse,
  Workspace
}
import org.broadinstitute.dsde.rawls.policy.PolicyService
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceService}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class SnapshotServiceSpec extends AnyWordSpecLike with Matchers with MockitoSugar with TestDriverComponent {

  // create a mockito-powered SamDAO that always returns true for permission checks and returns
  // a test-fixture user info object
  private def defaultMockSamDao() = {
    val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(mockSamDAO.getResourceAuthDomain(any[SamResourceTypeName], any[String], any[RawlsRequestContext]))
      .thenReturn(Future.successful(Seq.empty))
    when(
      mockSamDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                               any[String],
                               any[SamResourceAction],
                               any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))
    when(
      mockSamDAO.getUserStatus(any[RawlsRequestContext])
    ).thenReturn(
      Future.successful(
        Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
      )
    )

    mockSamDAO
  }

  // create a mockito-powered DataRepoDAO that always returns a stubbed snapshot
  private def defaultDataRepoDao(): DataRepoDAO = {
    val mockDataRepoDAO = mock[DataRepoDAO](RETURNS_SMART_NULLS)
    when(
      mockDataRepoDAO.getSnapshot(
        any[UUID],
        any[OAuth2BearerToken]
      )
    )
      .thenReturn(
        new SnapshotModel()
          .id(java.util.UUID.randomUUID())
          .name("snapshot")
          .description("snapshot description")
          .source(
            java.util.List
              .of(new SnapshotSourceModel().dataset(new DatasetSummaryModel().cloudPlatform(SnapshotCloudPlatform.GCP)))
          )
      )
    mockDataRepoDAO
  }

  private def defaultMockWorkspaceRepository(workspace: Workspace): WorkspaceRepository = {
    val mockWorkspaceRepository = mock[WorkspaceRepository]
    when(mockWorkspaceRepository.getWorkspace(mockitoEq(workspace.workspaceIdAsUUID), any()))
      .thenReturn(Future.successful(Option(workspace)))
    when(mockWorkspaceRepository.getWorkspace(mockitoEq(workspace.toWorkspaceName), any()))
      .thenReturn(Future.successful(Option(workspace)))
    mockWorkspaceRepository
  }

  private def defaultWorkspaceServiceConstructor(ctx: RawlsRequestContext = testContext): WorkspaceService =
    mock[WorkspaceService](RETURNS_SMART_NULLS)

  private def defaultPolicyService: PolicyService = {
    val emptyPao =
      new TpsPaoGetResult().effectiveAttributes(new TpsPolicyInputs()).sourcesObjectIds(List[UUID]().asJava)

    val policyService = mock[PolicyService]
    when(policyService.getPao(any(), any())).thenReturn(Future.successful(Option(emptyPao)))
    when(policyService.getOrCreateSnapshotPao(any(), any())).thenReturn(Future.successful(emptyPao))
    when(policyService.linkSnapshotPaoToWorkspacePao(any(), any(), any(), any())).thenReturn(Future.unit)
    policyService
  }

  "SnapshotService" should {
    def emptySnapshot(id: UUID) = new SnapshotModel().id(id)

    "create multiple snapshot references" in {
      val workspace = minimalTestData.workspace
      val snapshotIds = Set(UUID.randomUUID(), UUID.randomUUID())

      val policyService = defaultPolicyService

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))

      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        defaultWorkspaceServiceConstructor,
        policyService
      )(testContext)

      Await.result(snapshotService.createSnapshotsByWorkspaceNameV3(workspace.toWorkspaceName, snapshotIds),
                   Duration.Inf
      )
      snapshotIds.map(id =>
        verify(policyService).linkSnapshotPaoToWorkspacePao(mockitoEq(id),
                                                            mockitoEq(workspace.workspaceIdAsUUID),
                                                            mockitoEq(false),
                                                            any()
        )
      )
    }

    "create multiple snapshot references when called with a workspace id" in {
      val workspace = minimalTestData.workspace
      val snapshotIds = Set(UUID.randomUUID(), UUID.randomUUID())

      val policyService = defaultPolicyService

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))

      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        defaultWorkspaceServiceConstructor,
        policyService
      )(testContext)

      Await.result(snapshotService.createSnapshotsByWorkspaceIdV3(workspace.workspaceIdAsUUID.toString, snapshotIds),
                   Duration.Inf
      )
      snapshotIds.map(id =>
        verify(policyService).linkSnapshotPaoToWorkspacePao(mockitoEq(id),
                                                            mockitoEq(workspace.workspaceIdAsUUID),
                                                            mockitoEq(false),
                                                            any()
        )
      )
    }

    "fail to create multiple snapshot references if the workspace does not have a PAO" in {
      val workspace = minimalTestData.workspace
      val snapshotIds = Set(UUID.randomUUID(), UUID.randomUUID())

      val policyService = defaultPolicyService
      when(policyService.getPao(mockitoEq(workspace.workspaceIdAsUUID), any())).thenReturn(Future.successful(None))

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))

      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        defaultWorkspaceServiceConstructor,
        policyService
      )(testContext)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(snapshotService.createSnapshotsByWorkspaceNameV3(workspace.toWorkspaceName, snapshotIds),
                     Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
      snapshotIds.map(id =>
        verify(policyService, never).linkSnapshotPaoToWorkspacePao(mockitoEq(id),
                                                                   mockitoEq(workspace.workspaceIdAsUUID),
                                                                   mockitoEq(false),
                                                                   any()
        )
      )
    }

    "fail to create multiple snapshot references if the user does not have access to one of the snapshots" in {
      val workspace = minimalTestData.workspace
      val snapshotIds = Set(UUID.randomUUID(), UUID.randomUUID())

      val policyService = defaultPolicyService

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))
      when(dataRepo.getSnapshot(mockitoEq(snapshotIds.last), any())).thenAnswer(_ =>
        throw new client.ApiException(StatusCodes.NotFound.intValue, "not found")
      )
      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        defaultWorkspaceServiceConstructor,
        policyService
      )(testContext)

      val exception = intercept[RawlsExceptionWithErrorReport] {
        Await.result(snapshotService.createSnapshotsByWorkspaceNameV3(workspace.toWorkspaceName, snapshotIds),
                     Duration.Inf
        )
      }
      exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
      snapshotIds.map(id =>
        verify(policyService, never).linkSnapshotPaoToWorkspacePao(mockitoEq(id),
                                                                   mockitoEq(workspace.workspaceIdAsUUID),
                                                                   mockitoEq(false),
                                                                   any()
        )
      )
    }

    "fail to create a reference if any snapshots have protected data policies and the workspace doesn't" in {
      val workspace = minimalTestData.workspace
      val snapshotIds = Set(UUID.randomUUID(), UUID.randomUUID())

      val protectedDataPao = new TpsPaoGetResult().effectiveAttributes(
        new TpsPolicyInputs().inputs(
          List(new TpsPolicyInput().namespace(TERRA_POLICY_NAMESPACE).name(TpsPolicies.ProtectedData.name)).asJava
        )
      )

      val policyService = defaultPolicyService
      when(policyService.getOrCreateSnapshotPao(any(), any())).thenReturn(Future.successful(protectedDataPao))

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))
      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        defaultWorkspaceServiceConstructor,
        policyService
      )(testContext)

      intercept[ProtectedDataException] {
        Await.result(snapshotService.createSnapshotsByWorkspaceNameV3(workspace.toWorkspaceName, snapshotIds),
                     Duration.Inf
        )
      }
      snapshotIds.map(id =>
        verify(policyService, never).linkSnapshotPaoToWorkspacePao(mockitoEq(id),
                                                                   mockitoEq(workspace.workspaceIdAsUUID),
                                                                   mockitoEq(false),
                                                                   any()
        )
      )
    }

    "add new groups to the workspace's auth domain if any snapshots have an auth domain" in {
      val workspace = minimalTestData.workspace
      val snapshotIds = Set(UUID.randomUUID(), UUID.randomUUID())

      val snapshotPao1 = new TpsPaoGetResult().effectiveAttributes(
        new TpsPolicyInputs().inputs(
          List(
            new TpsPolicyInput()
              .namespace(TERRA_POLICY_NAMESPACE)
              .name(TpsPolicies.GroupConstraint.name)
              .additionalData(
                List(new TpsPolicyPair().key(TpsPolicies.GroupConstraint.additionalDataKey).value("group1")).asJava
              )
          ).asJava
        )
      )
      val snapshotPao2 = new TpsPaoGetResult().effectiveAttributes(
        new TpsPolicyInputs().inputs(
          List(
            new TpsPolicyInput()
              .namespace(TERRA_POLICY_NAMESPACE)
              .name(TpsPolicies.GroupConstraint.name)
              .additionalData(
                List(new TpsPolicyPair().key(TpsPolicies.GroupConstraint.additionalDataKey).value("group2")).asJava
              )
          ).asJava
        )
      )

      val policyService = defaultPolicyService
      when(policyService.getOrCreateSnapshotPao(any(), any()))
        .thenReturn(Future.successful(snapshotPao1))
        .thenReturn(Future.successful(snapshotPao2))

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))

      val workspaceService = mock[WorkspaceService]
      when(
        workspaceService.addAuthDomainGroups(mockitoEq(workspace.toWorkspaceName),
                                             mockitoEq(Set("group1", "group2")),
                                             any()
        )
      ).thenReturn(Future.unit)

      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        _ => workspaceService,
        policyService
      )(testContext)

      Await.result(snapshotService.createSnapshotsByWorkspaceNameV3(workspace.toWorkspaceName, snapshotIds),
                   Duration.Inf
      )
      verify(workspaceService).addAuthDomainGroups(mockitoEq(workspace.toWorkspaceName),
                                                   mockitoEq(Set("group1", "group2")),
                                                   any()
      )
      snapshotIds.map(id =>
        verify(policyService).linkSnapshotPaoToWorkspacePao(mockitoEq(id),
                                                            mockitoEq(workspace.workspaceIdAsUUID),
                                                            mockitoEq(false),
                                                            any()
        )
      )
    }

    "not try to link snapshots that are already linked to the workspace" in {
      val workspace = minimalTestData.workspace
      val newSnapshotId = UUID.randomUUID
      val existingSnapshotId = UUID.randomUUID
      val snapshotIds = Set(newSnapshotId, existingSnapshotId)

      val workspacePao = new TpsPaoGetResult()
        .effectiveAttributes(new TpsPolicyInputs())
        .sourcesObjectIds(List(existingSnapshotId).asJava)

      val policyService = defaultPolicyService
      when(policyService.getPao(any(), any())).thenReturn(Future.successful(Option(workspacePao)))

      val dataRepo = defaultDataRepoDao()
      snapshotIds.map(id => when(dataRepo.getSnapshot(mockitoEq(id), any())).thenReturn(emptySnapshot(id)))

      val snapshotService = SnapshotService.constructor(
        defaultMockWorkspaceRepository(workspace),
        defaultMockSamDao(),
        "fake-terra-data-repo-dev",
        dataRepo,
        defaultWorkspaceServiceConstructor,
        policyService
      )(testContext)

      Await.result(snapshotService.createSnapshotsByWorkspaceIdV3(workspace.workspaceIdAsUUID.toString, snapshotIds),
                   Duration.Inf
      )
      verify(policyService).linkSnapshotPaoToWorkspacePao(mockitoEq(newSnapshotId),
                                                          mockitoEq(workspace.workspaceIdAsUUID),
                                                          mockitoEq(false),
                                                          any()
      )
      verify(policyService, never).linkSnapshotPaoToWorkspacePao(mockitoEq(existingSnapshotId),
                                                                 mockitoEq(workspace.workspaceIdAsUUID),
                                                                 mockitoEq(false),
                                                                 any()
      )
    }
  }

}
