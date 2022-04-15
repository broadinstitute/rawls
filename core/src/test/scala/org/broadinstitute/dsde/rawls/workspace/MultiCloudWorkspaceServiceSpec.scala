package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.JobReport.StatusEnum
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.mock.MockWorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{MultiCloudWorkspaceRequest, RawlsUserEmail, RawlsUserSubjectId, UserInfo, Workspace, WorkspaceCloudPlatform, WorkspaceType}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec extends AnyFlatSpec with Matchers with TestDriverComponent {

  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")


  def activeMcWorkspaceConfig = MultiCloudWorkspaceConfig(
    multiCloudWorkspacesEnabled = true,
    Some(MultiCloudWorkspaceManagerConfig("fake_app_id", 60 seconds)),
    Some(AzureConfig("fake_profile_id", "fake_tenant_id", "fake_sub_id", "fake_mrg_id", "fake_bp_id", "fake_group")),
  )

  it should "throw an exception if creating a multi-cloud workspace is not enabled" in {
    val userInfo = UserInfo(RawlsUserEmail("example@example.com"), OAuth2BearerToken("fake_token"), 1234, RawlsUserSubjectId("ABCDEF"))
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(multiCloudWorkspacesEnabled = false, None, None)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource, workspaceManagerDAO, config
    )(userInfo)
    val request = MultiCloudWorkspaceRequest("fake", "fake_name", Map.empty, cloudPlatform = WorkspaceCloudPlatform.Azure, "fake_region")

    val actual = intercept[RawlsExceptionWithErrorReport] {
      mcWorkspaceService.createMultiCloudWorkspace(request)
    }

    actual.errorReport.statusCode shouldBe Some(StatusCodes.NotImplemented)
  }

  it should "throw an exception if a workspace with the same name already exists" in {
    val namespace = "testing_ns" + UUID.randomUUID().toString
    val name = "fake_name"
    val userInfo = UserInfo(RawlsUserEmail("example@example.com"), OAuth2BearerToken("fake_token"), 1234, RawlsUserSubjectId("ABCDEF"))
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource, workspaceManagerDAO, activeMcWorkspaceConfig
    )(userInfo)
    val request = MultiCloudWorkspaceRequest(
      namespace, name, Map.empty, cloudPlatform = WorkspaceCloudPlatform.Azure, "fake_region")

    Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  it should "create a workspace" in {
    val userInfo = UserInfo(RawlsUserEmail("example@example.com"), OAuth2BearerToken("fake_token"), 1234, RawlsUserSubjectId("ABCDEF"))
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource, workspaceManagerDAO, activeMcWorkspaceConfig
    )(userInfo)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = new MultiCloudWorkspaceRequest(
      namespace, "fake_name", Map.empty, cloudPlatform = WorkspaceCloudPlatform.Azure, "fake_region")

    val result: Workspace = Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito.verify(workspaceManagerDAO).enableApplication(
      ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
      ArgumentMatchers.eq("fake_app_id"),
      ArgumentMatchers.eq( userInfo.accessToken)
    )
    Mockito.verify(workspaceManagerDAO).createAzureWorkspaceCloudContext(
      ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
      ArgumentMatchers.eq("fake_tenant_id"),
      ArgumentMatchers.eq("fake_mrg_id"),
      ArgumentMatchers.eq("fake_sub_id"),
      ArgumentMatchers.eq( userInfo.accessToken)
    )
    Mockito.verify(workspaceManagerDAO).createAzureRelay(
      ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
      ArgumentMatchers.eq("fake_region"),
      ArgumentMatchers.eq( userInfo.accessToken)
    )
  }

  it should "fail on cloud context creation failure" in {
    testAsyncCreationFailure(StatusEnum.FAILED, StatusEnum.SUCCEEDED)
  }

  it should "fail on azure relay creation failure" in {
    testAsyncCreationFailure(StatusEnum.SUCCEEDED, StatusEnum.FAILED)
  }

  def testAsyncCreationFailure(createCloudContestStatus: StatusEnum, createAzureRelayStatus: StatusEnum): Unit = {
    val userInfo = UserInfo(RawlsUserEmail("example@example.com"), OAuth2BearerToken("fake_token"), 1234, RawlsUserSubjectId("ABCDEF"))
    val workspaceManagerDAO = MockWorkspaceManagerDAO.buildWithAsyncResults(createCloudContestStatus, createAzureRelayStatus)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource, workspaceManagerDAO, activeMcWorkspaceConfig
    )(userInfo)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = new MultiCloudWorkspaceRequest(
      namespace, "fake_name", Map.empty, cloudPlatform = WorkspaceCloudPlatform.Azure, "fake_region")

    intercept[WorkspaceManagerCreationFailureException] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    }
  }
}
