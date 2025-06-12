package org.broadinstitute.dsde.rawls.policy

import bio.terra.policy.client.ApiException
import bio.terra.policy.model.{
  TpsComponent,
  TpsObjectType,
  TpsPaoConflict,
  TpsPaoCreateRequest,
  TpsPaoGetResult,
  TpsPaoSourceRequest,
  TpsPaoUpdateResult,
  TpsPolicyInput,
  TpsPolicyInputs,
  TpsPolicyPair,
  TpsUpdateMode
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.dataaccess.tps.TpsDAO
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
import org.broadinstitute.dsde.rawls.model.{ManagedGroupRef, RawlsGroupName, RawlsRequestContext, WorkspaceRequest}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{never, verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.matchers.should.Matchers._

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class PolicyServiceSpec extends AnyFlatSpec {
  val baseWorkspaceRequest = WorkspaceRequest("workspace-namespace", "workspace-name", Map.empty)
  implicit val ec: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "createWorkspacePao"

  it should "set group-constraint and protected-data policies if auth domain is present" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest =
      baseWorkspaceRequest.copy(authorizationDomain = Option(Set(ManagedGroupRef(RawlsGroupName("test-group")))))

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.createPao(any(), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    val expectedPaoRequest = new TpsPaoCreateRequest()
      .objectType(TpsObjectType.WORKSPACE)
      .objectId(workspaceId)
      .component(TpsComponent.RAWLS)
      .attributes(
        new TpsPolicyInputs().inputs(
          List(
            new TpsPolicyInput().namespace(TERRA_POLICY_NAMESPACE).name(TpsPolicies.ProtectedData.name),
            new TpsPolicyInput()
              .namespace(TERRA_POLICY_NAMESPACE)
              .name(TpsPolicies.GroupConstraint.name)
              .additionalData(
                List(new TpsPolicyPair().key(TpsPolicies.GroupConstraint.additionalDataKey).value("test-group")).asJava
              )
          ).asJava
        )
      )

    Await.result(policyService.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsDAO).createPao(mockitoEq(expectedPaoRequest), any())
  }

  it should "set protected-data policy if auth domain is not present and enhanced bucket logging is enabled" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest = baseWorkspaceRequest.copy(authorizationDomain = None, enhancedBucketLogging = Some(true))

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.createPao(any(), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    val expectedPaoRequest = new TpsPaoCreateRequest()
      .objectType(TpsObjectType.WORKSPACE)
      .objectId(workspaceId)
      .component(TpsComponent.RAWLS)
      .attributes(
        new TpsPolicyInputs().inputs(
          List(
            new TpsPolicyInput().namespace(TERRA_POLICY_NAMESPACE).name(TpsPolicies.ProtectedData.name)
          ).asJava
        )
      )

    Await.result(policyService.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsDAO).createPao(mockitoEq(expectedPaoRequest), any())
  }

  it should "set protected-data policy if auth domain is present but empty and enhanced bucket logging is enabled" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest =
      baseWorkspaceRequest.copy(authorizationDomain = Option(Set.empty), enhancedBucketLogging = Some(true))

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.createPao(any(), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    val expectedPaoRequest = new TpsPaoCreateRequest()
      .objectType(TpsObjectType.WORKSPACE)
      .objectId(workspaceId)
      .component(TpsComponent.RAWLS)
      .attributes(
        new TpsPolicyInputs().inputs(
          List(
            new TpsPolicyInput().namespace(TERRA_POLICY_NAMESPACE).name(TpsPolicies.ProtectedData.name)
          ).asJava
        )
      )

    Await.result(policyService.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsDAO).createPao(mockitoEq(expectedPaoRequest), any())
  }

  it should "not set any policies if auth domain is not present and enhanced bucket logging is not enabled" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest = baseWorkspaceRequest.copy(authorizationDomain = None, enhancedBucketLogging = Some(false))

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.createPao(any(), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    val expectedPaoRequest = new TpsPaoCreateRequest()
      .objectType(TpsObjectType.WORKSPACE)
      .objectId(workspaceId)
      .component(TpsComponent.RAWLS)

    Await.result(policyService.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsDAO).createPao(mockitoEq(expectedPaoRequest), any())
  }

  behavior of "mergeWorkspacePao"

  it should "call mergePao with the correct parameters" in {
    val sourceWorkspaceId = UUID.randomUUID()
    val destWorkspaceId = UUID.randomUUID()

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.mergePao(any(), any(), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    val expectedPaoRequest = new TpsPaoSourceRequest()
      .sourceObjectId(destWorkspaceId)
      .updateMode(TpsUpdateMode.FAIL_ON_CONFLICT)

    Await.result(policyService.mergeWorkspacePao(sourceWorkspaceId, destWorkspaceId, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsDAO).mergePao(mockitoEq(expectedPaoRequest), mockitoEq(sourceWorkspaceId), any())
  }

  behavior of "deleteWorkspacePao"

  it should "call deletePao with the correct parameters" in {
    val workspaceId = UUID.randomUUID()

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.deletePao(any(), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    Await.result(policyService.deleteWorkspacePao(workspaceId, mock[RawlsRequestContext]), Duration.Inf)

    verify(tpsDAO).deletePao(mockitoEq(workspaceId), any())
  }

  it should "not throw an exception if TpsDAO.deletePao throws an exception" in {
    val workspaceId = UUID.randomUUID()

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.deletePao(any(), any())).thenReturn(Future.failed(new ApiException("Test exception")))
    val policyService = new PolicyService(tpsDAO)

    noException should be thrownBy
      Await.result(policyService.deleteWorkspacePao(workspaceId, mock[RawlsRequestContext]), Duration.Inf)
  }

  behavior of "getPao"

  it should "wrap the PAO TPS returns in an Option" in {
    val workspaceId = UUID.randomUUID
    val emptyPao = new TpsPaoGetResult()

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.getPao(mockitoEq(workspaceId), any())).thenReturn(Future.successful(emptyPao))
    val policyService = new PolicyService(tpsDAO)

    val res = Await.result(policyService.getPao(workspaceId, mock[RawlsRequestContext]), Duration.Inf)
    res shouldBe Option(emptyPao)
  }

  it should "return None if TPS returns a 404" in {
    val workspaceId = UUID.randomUUID

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.getPao(mockitoEq(workspaceId), any())).thenReturn(Future.failed(new ApiException(404, "pao not found")))
    val policyService = new PolicyService(tpsDAO)

    val res = Await.result(policyService.getPao(workspaceId, mock[RawlsRequestContext]), Duration.Inf)
    res shouldBe None
  }

  it should "throw for other TPS exceptions" in {
    val workspaceId = UUID.randomUUID

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.getPao(mockitoEq(workspaceId), any())).thenReturn(Future.failed(new ApiException(500, "disaster")))
    val policyService = new PolicyService(tpsDAO)

    val exception = intercept[ApiException] {
      Await.result(policyService.getPao(workspaceId, mock[RawlsRequestContext]), Duration.Inf)
    }
    exception.getCode shouldBe 500
  }

  behavior of "getOrCreateSnapshotPao"

  it should "return existing PAOs" in {
    val snapshotId = UUID.randomUUID
    val emptyPao = new TpsPaoGetResult()

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.getPao(mockitoEq(snapshotId), any())).thenReturn(Future.successful(emptyPao))
    val policyService = new PolicyService(tpsDAO)

    val res = Await.result(policyService.getOrCreateSnapshotPao(snapshotId, mock[RawlsRequestContext]), Duration.Inf)
    res shouldBe emptyPao
    verify(tpsDAO, never).createPao(any(), any())
  }

  it should "create an empty PAO if the snapshot doesn't have a PAO already" in {
    val snapshotId = UUID.randomUUID
    val emptyPao = new TpsPaoGetResult()

    val expectedCreateRequest =
      new TpsPaoCreateRequest().objectType(TpsObjectType.SNAPSHOT).component(TpsComponent.TDR).objectId(snapshotId)

    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.getPao(mockitoEq(snapshotId), any()))
      .thenReturn(Future.failed(new ApiException(404, "pao not found")))
      .thenReturn(Future.successful(emptyPao))
    when(tpsDAO.createPao(mockitoEq(expectedCreateRequest), any())).thenReturn(Future.unit)
    val policyService = new PolicyService(tpsDAO)

    val res = Await.result(policyService.getOrCreateSnapshotPao(snapshotId, mock[RawlsRequestContext]), Duration.Inf)
    res shouldBe emptyPao
    verify(tpsDAO).createPao(mockitoEq(expectedCreateRequest), any())
  }

  behavior of "linkSnapshotPaoToWorkspacePao"

  it should "support dry runs" in {
    val snapshotId = UUID.randomUUID
    val workspaceId = UUID.randomUUID

    val expectedUpdateRequest = new TpsPaoSourceRequest().sourceObjectId(snapshotId).updateMode(TpsUpdateMode.DRY_RUN)
    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())).thenReturn(
      Future.successful(
        new TpsPaoUpdateResult()
          .updateApplied(false)
          .resultingPao(new TpsPaoGetResult())
          .conflicts(List.empty[TpsPaoConflict].asJava)
      )
    )
    val policyService = new PolicyService(tpsDAO)

    Await.result(policyService.linkSnapshotPaoToWorkspacePao(snapshotId, workspaceId, true, mock[RawlsRequestContext]),
                 Duration.Inf
    )
    verify(tpsDAO).linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())
  }

  it should "link PAOs" in {
    val snapshotId = UUID.randomUUID
    val workspaceId = UUID.randomUUID

    val expectedUpdateRequest =
      new TpsPaoSourceRequest().sourceObjectId(snapshotId).updateMode(TpsUpdateMode.FAIL_ON_CONFLICT)
    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())).thenReturn(
      Future.successful(
        new TpsPaoUpdateResult()
          .updateApplied(true)
          .resultingPao(new TpsPaoGetResult())
          .conflicts(List.empty[TpsPaoConflict].asJava)
      )
    )
    val policyService = new PolicyService(tpsDAO)

    Await.result(policyService.linkSnapshotPaoToWorkspacePao(snapshotId, workspaceId, false, mock[RawlsRequestContext]),
                 Duration.Inf
    )
    verify(tpsDAO).linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())
  }

  it should "throw if there are any conflicts during a dry run" in {
    val snapshotId = UUID.randomUUID
    val workspaceId = UUID.randomUUID

    val expectedUpdateRequest = new TpsPaoSourceRequest().sourceObjectId(snapshotId).updateMode(TpsUpdateMode.DRY_RUN)
    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())).thenReturn(
      Future.successful(
        new TpsPaoUpdateResult()
          .updateApplied(false)
          .resultingPao(new TpsPaoGetResult())
          .conflicts(List(new TpsPaoConflict()).asJava)
      )
    )
    val policyService = new PolicyService(tpsDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        policyService.linkSnapshotPaoToWorkspacePao(snapshotId, workspaceId, true, mock[RawlsRequestContext]),
        Duration.Inf
      )
    }
    verify(tpsDAO).linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())
  }

  it should "throw if there are any conflicts when linking" in {
    val snapshotId = UUID.randomUUID
    val workspaceId = UUID.randomUUID

    val expectedUpdateRequest =
      new TpsPaoSourceRequest().sourceObjectId(snapshotId).updateMode(TpsUpdateMode.FAIL_ON_CONFLICT)
    val tpsDAO = mock[TpsDAO](RETURNS_SMART_NULLS)
    when(tpsDAO.linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())).thenReturn(
      Future.successful(
        new TpsPaoUpdateResult()
          .updateApplied(true)
          .resultingPao(new TpsPaoGetResult())
          .conflicts(List(new TpsPaoConflict()).asJava)
      )
    )
    val policyService = new PolicyService(tpsDAO)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        policyService.linkSnapshotPaoToWorkspacePao(snapshotId, workspaceId, false, mock[RawlsRequestContext]),
        Duration.Inf
      )
    }
    verify(tpsDAO).linkPao(mockitoEq(expectedUpdateRequest), mockitoEq(workspaceId), any())
  }
}
