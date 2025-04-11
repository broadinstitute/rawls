package org.broadinstitute.dsde.rawls.policy

import bio.terra.policy.model.{
  TpsComponent,
  TpsObjectType,
  TpsPaoCreateRequest,
  TpsPolicyInput,
  TpsPolicyInputs,
  TpsPolicyPair
}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.tps.TpsDAO
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
import org.broadinstitute.dsde.rawls.model.{ManagedGroupRef, RawlsGroupName, RawlsRequestContext, WorkspaceRequest}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

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
}
