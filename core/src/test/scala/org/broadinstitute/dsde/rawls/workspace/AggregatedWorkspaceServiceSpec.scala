package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspacePolicy
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class AggregatedWorkspaceServiceSpec extends AnyFlatSpec with MockitoTestUtils {

  private val rawlsWorkspace = Workspace(
    namespace = "test-namespace",
    name = "test-name",
    workspaceId = UUID.randomUUID.toString,
    bucketName = "aBucket",
    workflowCollectionName = Some("workflow-collection"),
    createdDate = new DateTime(),
    lastModified = new DateTime(),
    createdBy = "test",
    attributes = Map.empty
  )

  private val mcWorkspace = Workspace.buildReadyMcWorkspace(
    namespace = "fake",
    name = "fakews",
    workspaceId = UUID.randomUUID.toString,
    createdDate = DateTime.now(),
    lastModified = DateTime.now(),
    createdBy = "fake",
    attributes = Map.empty
  )

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  behavior of "fetchAggregatedWorkspace"

  it should "combine WSM data with Rawls data for Azure MC workspaces" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    val azContext = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString)
    val policies = Seq(
      new WsmPolicyInput()
        .name("fakepolicy")
        .namespace("fakens")
        .addAdditionalDataItem(new WsmPolicyPair().key("dataKey").value("dataValue"))
    )
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription()
        .azureContext(
          new AzureContext()
            .tenantId(azContext.tenantId.toString)
            .subscriptionId(azContext.subscriptionId.toString)
            .resourceGroupId(azContext.managedResourceGroupId)
        )
        .policies(policies.asJava)
    )
    val svc = new AggregatedWorkspaceService(wsmDao)

    val aggregatedWorkspace = svc.fetchAggregatedWorkspace(mcWorkspace, defaultRequestContext)

    aggregatedWorkspace.baseWorkspace shouldBe mcWorkspace
    aggregatedWorkspace.azureCloudContext shouldBe Some(azContext)
    aggregatedWorkspace.getCloudPlatform shouldBe WorkspaceCloudPlatform.Azure
    aggregatedWorkspace.policies shouldBe policies.map(input =>
      WorkspacePolicy(
        input.getName,
        input.getNamespace,
        Option(
          input.getAdditionalData.asScala.toList
            .map(data => Map.apply(data.getKey -> data.getValue))
        )
          .getOrElse(List.empty)
      )
    )
  }

  it should "combine WSM data with Rawls data for GCP MC workspaces" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext]))
      .thenReturn(new WorkspaceDescription().gcpContext(new GcpContext().projectId("project-id")))
    val svc = new AggregatedWorkspaceService(wsmDao)

    val aggregatedWorkspace = svc.fetchAggregatedWorkspace(mcWorkspace, defaultRequestContext)

    aggregatedWorkspace.baseWorkspace shouldBe mcWorkspace
    aggregatedWorkspace.googleProjectId shouldBe Some(GoogleProjectId("project-id"))
    aggregatedWorkspace.getCloudPlatform shouldBe WorkspaceCloudPlatform.Gcp
  }

  it should "raise if the workspace is not found by WSM" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext]))
      .thenAnswer(_ => throw new ApiException(StatusCodes.NotFound.intValue, "not found"))
    val svc = new AggregatedWorkspaceService(wsmDao)

    intercept[AggregateWorkspaceNotFoundException] {
      svc.optimizedFetchAggregatedWorkspace(mcWorkspace, defaultRequestContext)
    }

    verify(wsmDao).getWorkspace(any[UUID], any[RawlsRequestContext])
  }

  it should "raise if the call to WSM fails" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext]))
      .thenAnswer(_ => throw new ApiException(StatusCodes.InternalServerError.intValue, "not found"))
    val svc = new AggregatedWorkspaceService(wsmDao)

    intercept[WorkspaceAggregationException] {
      svc.optimizedFetchAggregatedWorkspace(mcWorkspace, defaultRequestContext)
    }

    verify(wsmDao).getWorkspace(any[UUID], any[RawlsRequestContext])
  }

  it should "raise if the WSM workspace is missing any form of cloud context" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription()
    )
    val svc = new AggregatedWorkspaceService(wsmDao)

    val thrown = intercept[InvalidCloudContextException] {
      svc.optimizedFetchAggregatedWorkspace(mcWorkspace, defaultRequestContext)
    }

    thrown.getMessage should include("expected exactly one set of cloud metadata")
  }

  it should "reach out to WSM for legacy GCP rawls workspaces" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription().gcpContext(new GcpContext().projectId("project-id"))
    )
    val svc = new AggregatedWorkspaceService(wsmDao)

    val aggregatedWorkspace = svc.fetchAggregatedWorkspace(rawlsWorkspace, defaultRequestContext)

    aggregatedWorkspace.baseWorkspace shouldBe rawlsWorkspace
    aggregatedWorkspace.azureCloudContext shouldBe None
    aggregatedWorkspace.getCloudPlatform shouldBe WorkspaceCloudPlatform.Gcp
    verify(wsmDao).getWorkspace(ArgumentMatchers.eq(rawlsWorkspace.workspaceIdAsUUID), any[RawlsRequestContext])
  }

  behavior of "optimizedFetchAggregatedWorkspace"

  it should "not reach out to WSM for legacy GCP Rawls workspaces" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription().gcpContext(new GcpContext().projectId("project-id"))
    )
    val svc = new AggregatedWorkspaceService(wsmDao)

    val aggregatedWorkspace = svc.optimizedFetchAggregatedWorkspace(rawlsWorkspace, defaultRequestContext)

    aggregatedWorkspace.baseWorkspace shouldBe rawlsWorkspace
    aggregatedWorkspace.azureCloudContext shouldBe None
    aggregatedWorkspace.getCloudPlatform shouldBe WorkspaceCloudPlatform.Gcp
    verify(wsmDao, never()).getWorkspace(any[UUID], any[RawlsRequestContext])
  }
}
