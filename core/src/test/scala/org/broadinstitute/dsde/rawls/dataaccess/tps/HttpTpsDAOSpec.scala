package org.broadinstitute.dsde.rawls.dataaccess.tps

import bio.terra.policy.api.TpsApi
import bio.terra.policy.model._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.TpsModel.{TERRA_POLICY_NAMESPACE, TpsPolicies}
import org.broadinstitute.dsde.rawls.model.{ManagedGroupRef, RawlsGroupName, RawlsRequestContext, WorkspaceRequest}
import org.mockito.Mockito.{verify, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._

class HttpTpsDAOSpec extends AnyFlatSpec {
  val baseWorkspaceRequest = WorkspaceRequest("workspace-namespace", "workspace-name", Map.empty)
  implicit val ec: ExecutionContext = TestExecutionContext.testExecutionContext

  def getTpsDAO(mockTpsApi: TpsApi): HttpTpsDAO =
    new HttpTpsDAO("http://example.com") {
      override protected def getTpsApi(ctx: RawlsRequestContext): TpsApi = mockTpsApi
    }

  behavior of "createWorkspacePao"

  it should "set group-constraint and protected-data policies if auth domain is present" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest =
      baseWorkspaceRequest.copy(authorizationDomain = Option(Set(ManagedGroupRef(RawlsGroupName("test-group")))))

    val tpsApi = mock[TpsApi](RETURNS_SMART_NULLS)
    val tpsDAO = getTpsDAO(tpsApi)

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

    Await.result(tpsDAO.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsApi).createPao(expectedPaoRequest)
  }

  it should "set protected-data policy if auth domain is not present and enhanced bucket logging is enabled" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest = baseWorkspaceRequest.copy(authorizationDomain = None, enhancedBucketLogging = Some(true))

    val tpsApi = mock[TpsApi](RETURNS_SMART_NULLS)
    val tpsDAO = getTpsDAO(tpsApi)

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

    Await.result(tpsDAO.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsApi).createPao(expectedPaoRequest)
  }

  it should "not set any policies if auth domain is not present and enhanced bucket logging is not enabled" in {
    val workspaceId = UUID.randomUUID()
    val workspaceRequest = baseWorkspaceRequest.copy(authorizationDomain = None, enhancedBucketLogging = Some(false))

    val tpsApi = mock[TpsApi](RETURNS_SMART_NULLS)
    val tpsDAO = getTpsDAO(tpsApi)

    val expectedPaoRequest = new TpsPaoCreateRequest()
      .objectType(TpsObjectType.WORKSPACE)
      .objectId(workspaceId)
      .component(TpsComponent.RAWLS)

    Await.result(tpsDAO.createWorkspacePao(workspaceId, workspaceRequest, mock[RawlsRequestContext]),
                 Duration.Inf
    )

    verify(tpsApi).createPao(expectedPaoRequest)
  }
}
