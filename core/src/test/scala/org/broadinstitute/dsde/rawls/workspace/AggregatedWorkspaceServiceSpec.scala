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
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class AggregatedWorkspaceServiceSpec extends AnyFlatSpec with MockitoTestUtils {

  private val rawlsWorkspace = Workspace(
    "test-namespace",
    "test-name",
    "aWorkspaceId",
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  private val mcWorkspace = Workspace.buildReadyMcWorkspace(
    "fake",
    "fakews",
    UUID.randomUUID.toString,
    DateTime.now(),
    DateTime.now(),
    "fake",
    Map.empty
  )

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  behavior of "getAggregatedWorkspace"

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

    val aggregatedWorkspace = svc.getAggregatedWorkspace(mcWorkspace, defaultRequestContext)

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
    val googleProjectId = GoogleProjectId("project-id")
    val policies = Seq(
      new WsmPolicyInput()
        .name("fakepolicy")
        .namespace("fakens")
        .addAdditionalDataItem(new WsmPolicyPair().key("dataKey").value("dataValue"))
    )
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription()
        .gcpContext(
          new GcpContext().projectId(googleProjectId.toString())
        )
        .policies(policies.asJava)
    )
    val svc = new AggregatedWorkspaceService(wsmDao)

    val aggregatedWorkspace = svc.getAggregatedWorkspace(mcWorkspace, defaultRequestContext)

    aggregatedWorkspace.baseWorkspace shouldBe mcWorkspace
    aggregatedWorkspace.googleProjectId shouldBe Some(googleProjectId)
    aggregatedWorkspace.getCloudPlatform shouldBe WorkspaceCloudPlatform.Gcp
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

  it should "not reach out to WSM for legacy GCP Rawls workspaces" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    val svc = new AggregatedWorkspaceService(wsmDao)

    val aggregatedWorkspace = svc.getAggregatedWorkspace(rawlsWorkspace, defaultRequestContext)

    aggregatedWorkspace.baseWorkspace shouldBe rawlsWorkspace
    aggregatedWorkspace.azureCloudContext shouldBe None
    aggregatedWorkspace.getCloudPlatform shouldBe WorkspaceCloudPlatform.Gcp
    verify(wsmDao, times(0)).getWorkspace(any[UUID], any[RawlsRequestContext])
  }

  it should "raise if the workspace is not found by WSM" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext]))
      .thenAnswer(_ => throw new ApiException(StatusCodes.NotFound.intValue, "not found"))
    val svc = new AggregatedWorkspaceService(wsmDao)

    intercept[AggregateWorkspaceNotFoundException] {
      svc.getAggregatedWorkspace(mcWorkspace, defaultRequestContext)
    }

    verify(wsmDao).getWorkspace(any[UUID], any[RawlsRequestContext])
  }

  it should "raise if the call to WSM fails" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext]))
      .thenAnswer(_ => throw new ApiException(StatusCodes.InternalServerError.intValue, "not found"))
    val svc = new AggregatedWorkspaceService(wsmDao)

    intercept[WorkspaceAggregationException] {
      svc.getAggregatedWorkspace(mcWorkspace, defaultRequestContext)
    }

    verify(wsmDao).getWorkspace(any[UUID], any[RawlsRequestContext])
  }

  it should "raise if the WSM workspace does not have an Azure cloud context" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any[UUID], any[RawlsRequestContext])).thenReturn(
      new WorkspaceDescription()
    )
    val svc = new AggregatedWorkspaceService(wsmDao)

    intercept[InvalidCloudContextException] {
      svc.getAggregatedWorkspace(mcWorkspace, defaultRequestContext)
    }
  }
}
