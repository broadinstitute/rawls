package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{AzureStorageResource, CreatedControlledAzureStorage}
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  MultiCloudWorkspaceRequest,
  RawlsBillingProject,
  SamBillingProjectActions,
  SamResourceTypeNames,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceRequest,
  WorkspaceType
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec extends AnyFlatSpec with Matchers with TestDriverComponent {

  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")
  implicit val workbenchMetricBaseName: ShardId = "test"

  def activeMcWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    multiCloudWorkspacesEnabled = true,
    Some(MultiCloudWorkspaceManagerConfig("fake_app_id", 60 seconds)),
    Some(
      AzureConfig(
        "fake_profile_id",
        UUID.randomUUID().toString,
        UUID.randomUUID().toString,
        "fake_mrg_id",
        "fake_bp_id",
        "fake_group",
        "eastus",
        "fake-landing-zone-definition",
        "fake-landing-zone-version"
      )
    )
  )

  it should "delegate legacy creation requests to WorkspaceService" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      config,
      workbenchMetricBaseName
    )(testContext)
    val workspaceRequest = WorkspaceRequest(
      "fake_billing_project",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )
    val billingProject = mock[RawlsBillingProject]
    when(billingProject.billingProfileId).thenReturn(None)

    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
    when(workspaceService.withBillingProjectContext(any(), any())(any())).thenAnswer { invocation =>
      (invocation.getArgument(2): RawlsBillingProject => Future[Workspace])(billingProject)
    }
    when(workspaceService.createWorkspace(workspaceRequest, testContext)).thenReturn(
      Future.successful(
        Workspace("fake", "fake", "fake", "fake", None, currentTime(), currentTime(), "fake", Map.empty)
      )
    )

    val result = Await.result(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(
                                workspaceRequest,
                                workspaceService
                              ),
                              Duration.Inf
    )

    result.workspaceType shouldBe WorkspaceType.RawlsWorkspace
    verify(workspaceService, Mockito.times(1)).createWorkspace(workspaceRequest, testContext)
  }

  it should "not delegate when called with an azure billing project" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      config,
      workbenchMetricBaseName
    )(testContext)
    val workspaceRequest = WorkspaceRequest(
      "fake_mc_billing_project_name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    val result = Await.result(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(
                                workspaceRequest,
                                workspaceService
                              ),
                              Duration.Inf
    )

    result.workspaceType shouldBe WorkspaceType.McWorkspace
  }

  it should "return forbidden if creating a workspace against a billing project that the user does not have the createWorkspace action for" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = Mockito.spy(new MockSamDAO(slickDataSource))
    when(
      samDAO.userHasAction(SamResourceTypeNames.billingProject,
                           "fake_mc_billing_project_name",
                           SamBillingProjectActions.createWorkspace,
                           testContext
      )
    ).thenReturn(Future.successful(false))
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      config,
      workbenchMetricBaseName
    )(testContext)
    val workspaceRequest = WorkspaceRequest(
      "fake_mc_billing_project_name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(
                     workspaceRequest,
                     workspaceService
                   ),
                   Duration.Inf
      )
    }

    actual.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if creating a multi-cloud workspace is not enabled" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(multiCloudWorkspacesEnabled = false, None, None)
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      config,
      workbenchMetricBaseName
    )(testContext)
    val request = MultiCloudWorkspaceRequest(
      "fake",
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      "fake_region",
      mock[AzureManagedAppCoordinates],
      "fake_billingProjectId"
    )

    val actual = intercept[RawlsExceptionWithErrorReport] {
      mcWorkspaceService.createMultiCloudWorkspace(request)
    }

    actual.errorReport.statusCode shouldBe Some(StatusCodes.NotImplemented)
  }

  it should "throw an exception if a workspace with the same name already exists" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      workbenchMetricBaseName
    )(testContext)
    val request = MultiCloudWorkspaceRequest(
      "fake",
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      "fake_region",
      AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake"),
      "fake_billingProjectId"
    )

    Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
  }

  it should "create a workspace" in {
    val subscriptionId = UUID.randomUUID()
    val tenantId = UUID.randomUUID()
    //  Needed because the storage container takes the storage account ID as input.
    val storageAccountId = UUID.randomUUID()
    val customWsmDao = new MockWorkspaceManagerDAO() {
      override def mockCreateAzureStorageAccountResult(): CreatedControlledAzureStorage =
        new CreatedControlledAzureStorage().resourceId(storageAccountId).azureStorage(new AzureStorageResource())
    }
    val workspaceManagerDAO = Mockito.spy(customWsmDao)

    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = MultiCloudWorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      "fake_region",
      AzureManagedAppCoordinates(tenantId, subscriptionId, "fake_mrg_id"),
      "fake_billingProjectId"
    )
//case class AzureManagedAppCoordinates(tenantId: UUID, subscriptionId: UUID, managedResourceGroupId: String)
    val result: Workspace = Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .enableApplication(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_app_id"),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureWorkspaceCloudContext(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(tenantId.toString),
        ArgumentMatchers.eq("fake_mrg_id"),
        ArgumentMatchers.eq(subscriptionId.toString),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureRelay(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_region"),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageAccount(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_region"),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(storageAccountId),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "fail on cloud context creation failure" in {
    testAsyncCreationFailure(StatusEnum.FAILED, StatusEnum.SUCCEEDED)
  }

  it should "fail on azure relay creation failure" in {
    testAsyncCreationFailure(StatusEnum.SUCCEEDED, StatusEnum.FAILED)
  }

  def testAsyncCreationFailure(createCloudContestStatus: StatusEnum, createAzureRelayStatus: StatusEnum): Unit = {
    val workspaceManagerDAO =
      MockWorkspaceManagerDAO.buildWithAsyncResults(createCloudContestStatus, createAzureRelayStatus)
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = MultiCloudWorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      WorkspaceCloudPlatform.Azure,
      "fake_region",
      AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "managed_resource_group_id"),
      "fake_billingProjectId"
    )

    intercept[WorkspaceManagerCreationFailureException] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request), Duration.Inf)
    }
  }
}
