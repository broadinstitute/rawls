package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, MockLeonardoDAO, SamDAO}
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.broadinstitute.dsde.rawls.model.WorkspaceType.McWorkspace
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeString,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamResourceTypeNames,
  SamWorkspaceActions,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceDeletionResult,
  WorkspaceName,
  WorkspacePolicy,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.workspace.MultiCloudWorkspaceService.getStorageContainerName
import org.broadinstitute.dsde.workbench.client.leonardo
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString, eq => equalTo}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec extends AnyFlatSpec with Matchers with OptionValues with TestDriverComponent {

  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")
  implicit val workbenchMetricBaseName: ShardId = "test"

  def activeMcWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    MultiCloudWorkspaceManagerConfig("fake_app_id", 60 seconds, 120 seconds),
    AzureConfig(
      "fake-landing-zone-definition",
      "fake-protected-landing-zone-definition",
      "fake-landing-zone-version",
      Map("fake_parameter" -> "fake_value"),
      Map("fake_parameter" -> "fake_value"),
      landingZoneAllowAttach = false
    )
  )

  behavior of "createMultiCloudOrRawlsWorkspace"

  it should "delegate legacy creation requests to WorkspaceService" in {
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        mock[BillingProfileManagerDAO],
        samDAO,
        config,
        leonardoDAO,
        workbenchMetricBaseName
      )(testContext)
    )
    val workspaceRequest = WorkspaceRequest(
      "fake_billing_project",
      UUID.randomUUID().toString,
      Map.empty
    )
    val billingProject = mock[RawlsBillingProject]
    when(billingProject.projectName).thenReturn(RawlsBillingProjectName("fake_billing_project"))
    when(billingProject.billingProfileId).thenReturn(None)

    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    doReturn(Future.successful(billingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    when(workspaceService.createWorkspace(any(), any())).thenReturn(
      Future.successful(
        Workspace("fake", "fake", "fake", "fake", None, currentTime(), currentTime(), "fake", Map.empty)
      )
    )

    val result = Await.result(
      mcWorkspaceService.createMultiCloudOrRawlsWorkspace(workspaceRequest, workspaceService),
      Duration.Inf
    )

    result.workspaceType shouldBe Some(WorkspaceType.RawlsWorkspace)
    result.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    verify(workspaceService, Mockito.times(1))
      .createWorkspace(equalTo(workspaceRequest), any())
  }

  it should "not delegate when called with an azure billing project" in {
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val bpDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS);
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpDAO,
        samDAO,
        config,
        leonardoDAO,
        workbenchMetricBaseName
      )(testContext)
    )
    val workspaceRequest = WorkspaceRequest(
      "test-bp-name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpDAO)
      .getBillingProfile(any[UUID], any[RawlsRequestContext])

    val result = Await.result(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(
                                workspaceRequest,
                                workspaceService
                              ),
                              Duration.Inf
    )

    result.workspaceType shouldBe Some(WorkspaceType.McWorkspace)
    result.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Azure)
  }

  it should "return forbidden if creating a workspace against a billing project that the user does not have the createWorkspace action for" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = Mockito.spy(new MockSamDAO(slickDataSource))
    val bpDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS);
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

    when(
      samDAO.userHasAction(SamResourceTypeNames.billingProject,
                           "fake_mc_billing_project_name",
                           SamBillingProjectActions.createWorkspace,
                           testContext
      )
    ).thenReturn(Future.successful(false))
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpDAO,
        samDAO,
        config,
        leonardoDAO,
        workbenchMetricBaseName
      )(testContext)
    )
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

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpDAO)
      .getBillingProfile(any[UUID], any[RawlsRequestContext])

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

  it should "throw an exception if the billing profile is not found" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val samDAO = new MockSamDAO(slickDataSource)
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

    when(bpmDAO.getBillingProfile(any[UUID], any[RawlsRequestContext])).thenReturn(None)
    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpmDAO,
        samDAO,
        config,
        leonardoDAO,
        workbenchMetricBaseName
      )(testContext)
    )
    val request = WorkspaceRequest(
      "fake_mc_billing_project_name",
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(any(), any())

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        mcWorkspaceService.createMultiCloudOrRawlsWorkspace(request, workspaceService),
        Duration.Inf
      )
    }

    assert(actual.errorReport.message.contains("Unable to find billing profile"))
  }

  behavior of "createMultiCloudWorkspace"

  it should "throw an exception if a workspace with the same name already exists and not delete the original workspace" in {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val samDAO = new MockSamDAO(slickDataSource)
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()
    val namespace = "fake"
    val workspaceName = s"fake-name-${UUID.randomUUID().toString}"
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest(
      namespace,
      workspaceName,
      Map.empty
    )
    val billingProfileId = UUID.randomUUID()

    Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(billingProfileId)),
                 Duration.Inf
    )
    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(billingProfileId)),
                   Duration.Inf
      )
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)

    // Make sure that the pre-existing workspace was not deleted.
    verify(workspaceManagerDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
    Await
      .result(slickDataSource.inTransaction(_.workspaceQuery.findByName(WorkspaceName(namespace, workspaceName))),
              Duration.Inf
      )
      .get
      .name shouldBe workspaceName
  }

  it should "throw an exception if the billing profile was created before 9/12/2023" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val samDAO = new MockSamDAO(slickDataSource)
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest(
      "fake",
      s"fake-name-${UUID.randomUUID().toString}",
      Map.empty
    )
    val billingProfileId = UUID.randomUUID()

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(
                     request,
                     new ProfileModel().id(billingProfileId).createdDate("2023-09-11T22:20:48.949Z")
                   ),
                   Duration.Inf
      )
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "not throw an exception for billing profiles created 9/12/2023 or later" in {
    val workspaceManagerDAO = new MockWorkspaceManagerDAO()
    val samDAO = new MockSamDAO(slickDataSource)
    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest(
      "fake",
      s"fake-name-${UUID.randomUUID().toString}",
      Map.empty
    )
    val billingProfileId = UUID.randomUUID()
    Await.result(mcWorkspaceService.createMultiCloudWorkspace(
                   request,
                   new ProfileModel().id(billingProfileId).createdDate("2023-09-12T22:20:48.949Z")
                 ),
                 Duration.Inf
    )
  }

  it should "deploy a WDS instance during workspace creation" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    val leonardoDAO: LeonardoDAO = Mockito.spy(
      new MockLeonardoDAO()
    )
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val name = "fake_name"
    val request = WorkspaceRequest(
      namespace,
      name,
      Map.empty
    )
    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(name),
        any(), // spend profile id
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        any[Option[WsmPolicyInputs]],
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(leonardoDAO)
      .createWDSInstance(
        ArgumentMatchers.eq("token"),
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(None)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId))),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "not deploy a WDS instance during workspace creation if test attribute is set as a boolean" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    val leonardoDAO: LeonardoDAO = Mockito.spy(
      new MockLeonardoDAO()
    )
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeBoolean(true))
    )
    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(leonardoDAO, never())
      .createWDSInstance(
        ArgumentMatchers.eq("token"),
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(None)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId))),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "create the workspace even if WDS instance creation fails" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    val leonardoDAO: LeonardoDAO = mock[MockLeonardoDAO]

    Mockito
      .when(leonardoDAO.createWDSInstance(anyString(), any[UUID](), any()))
      .thenAnswer(_ => throw new leonardo.ApiException(500, "intentional Leo exception for unit test"))

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty
    )
    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(leonardoDAO)
      .createWDSInstance(
        ArgumentMatchers.eq("token"),
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(None)
      )
    Mockito
      .verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId))),
        ArgumentMatchers.eq(testContext)
      )
  }

  it should "fail on WSM workspace creation failure and try to rollback workspace creation" in {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO() {
      override def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                                   displayName: String,
                                                   spendProfileId: String,
                                                   billingProjectNamespace: String,
                                                   applicationIds: Seq[String],
                                                   cloudPlatform: CloudPlatform,
                                                   policyInputs: Option[WsmPolicyInputs],
                                                   ctx: RawlsRequestContext
      ): CreateWorkspaceV2Result = throw new ApiException(500, "whoops")

      override def deleteWorkspaceV2(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): JobResult =
        throw new ApiException(404, "i've never seen that workspace in my life")
    })

    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest("fake_ns", s"fake_name-${UUID.randomUUID()}", Map.empty)

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )
    }

    verifyWorkspaceCreationRollback(workspaceManagerDAO, request.toWorkspaceName)
  }

  it should "fail on workspace creation failure and try to rollback workspace creation" in {
    val workspaceManagerDAO =
      Mockito.spy(MockWorkspaceManagerDAO.buildWithAsyncCreateWorkspaceResult(StatusEnum.FAILED))

    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest(
      s"fake_ns_${UUID.randomUUID()}",
      s"fake_name_${UUID.randomUUID()}",
      Map.empty
    )

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )
    }

    verifyWorkspaceCreationRollback(workspaceManagerDAO, request.toWorkspaceName)
  }

  it should "fail on container creation failure and try to rollback workspace creation" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      override def createAzureStorageContainer(workspaceId: UUID,
                                               storageContainerName: String,
                                               ctx: RawlsRequestContext
      ): CreatedControlledAzureStorageContainer = throw new ApiException(500, "what's a container?")
    })

    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest("fake_ns", "fake_name", Map.empty)
    intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )
    }

    verifyWorkspaceCreationRollback(workspaceManagerDAO, request.toWorkspaceName)
  }

  it should "still delete from the database when cleaning up the workspace in WSM fails" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      override def createAzureStorageContainer(workspaceId: UUID,
                                               storageContainerName: String,
                                               ctx: RawlsRequestContext
      ): CreatedControlledAzureStorageContainer =
        throw new ApiException(500, "error")

      override def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit =
        throw new ApiException(500, "no take backsies")
    })

    val leonardoDAO: LeonardoDAO = new MockLeonardoDAO()

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      leonardoDAO,
      workbenchMetricBaseName
    )(testContext)
    val request = WorkspaceRequest("fake_ns", s"fake_name-${UUID.randomUUID()}", Map.empty)
    intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )
    }

    verifyWorkspaceCreationRollback(workspaceManagerDAO, request.toWorkspaceName)
  }

  it should "fail with an improperly structured additional fields element on a policy and rollback workspace creation" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[MockLeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val policies = List(
      WorkspacePolicy("protected-data", "terra", List.empty),
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup", "otherInvalid" -> "other")))
    )
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      protectedData = None,
      policies = Some(policies)
    )

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )
    }

    exception.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    verifyWorkspaceCreationRollback(workspaceManagerDAO, request.toWorkspaceName)
  }

  it should "create a workspace with the requested policies" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    val samDAO = new MockSamDAO(slickDataSource)
    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[MockLeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val policies = List(
      WorkspacePolicy("protected-data", "terra", List.empty),
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      protectedData = None,
      policies = Some(policies)
    )

    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_name"),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        ArgumentMatchers.eq(
          Some(
            new WsmPolicyInputs()
              .inputs(
                Seq(
                  new WsmPolicyInput()
                    .name("protected-data")
                    .namespace("terra"),
                  new WsmPolicyInput()
                    .name("group-constraint")
                    .namespace("terra")
                    .additionalData(List(new WsmPolicyPair().key("group").value("myFakeGroup")).asJava),
                  new WsmPolicyInput()
                    .name("region-constraint")
                    .namespace("other-namespace")
                    .additionalData(
                      List(new WsmPolicyPair().key("key1").value("value1"),
                           new WsmPolicyPair().key("key2").value("value2")
                      ).asJava
                    )
                ).asJava
              )
          )
        ),
        ArgumentMatchers.eq(testContext)
      )
    Mockito
      .verify(workspaceManagerDAO, Mockito.times(0))
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.any[UUID](),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any(),
        any(),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.any()
      )
  }

  it should "create a protected data workspace" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)

    val mcWorkspaceService = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[MockLeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    val namespace = "fake_ns" + UUID.randomUUID().toString
    val request = WorkspaceRequest(
      namespace,
      "fake_name",
      Map.empty,
      protectedData = Some(true)
    )
    val result: Workspace =
      Await.result(mcWorkspaceService.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())),
                   Duration.Inf
      )

    result.name shouldBe "fake_name"
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace

    Mockito
      .verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq("fake_name"),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        ArgumentMatchers.eq(
          Some(
            new WsmPolicyInputs()
              .inputs(
                Seq(
                  new WsmPolicyInput()
                    .name("protected-data")
                    .namespace("terra")
                    .additionalData(List().asJava)
                ).asJava
              )
          )
        ),
        ArgumentMatchers.eq(testContext)
      )

    Mockito
      .verify(workspaceManagerDAO, Mockito.times(0))
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.any[UUID](),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq(namespace),
        any(),
        any(),
        ArgumentMatchers.eq(None),
        ArgumentMatchers.any()
      )
  }

  private def verifyWorkspaceCreationRollback(workspaceManagerDAO: MockWorkspaceManagerDAO,
                                              workspaceName: WorkspaceName
  ): Unit = {
    verify(workspaceManagerDAO).deleteWorkspaceV2(any(), anyString(), any[RawlsRequestContext])
    Await.result(slickDataSource.inTransaction(_.workspaceQuery.findByName(workspaceName)), Duration.Inf) shouldBe None
  }

  behavior of "cloneMultiCloudWorkspace"

  def withMockedMultiCloudWorkspaceService(runTest: MultiCloudWorkspaceService => Assertion,
                                           samDAO: SamDAO = new MockSamDAO(slickDataSource)
  ): Assertion = {
    val workspaceManagerDAO = spy(new MockWorkspaceManagerDAO())
    val config = MultiCloudWorkspaceConfig(ConfigFactory.load())
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    val leonardoDAO: LeonardoDAO = spy(new MockLeonardoDAO())

    val mcWorkspaceService = spy(
      MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        bpmDAO,
        new MockSamDAO(slickDataSource),
        config,
        leonardoDAO,
        workbenchMetricBaseName
      )(testContext)
    )

    doReturn(Future.successful(testData.azureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.azureBillingProject.projectName), any())

    doReturn(Future.successful(testData.billingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.billingProject.projectName), any())

    doReturn(Some(testData.azureBillingProfile))
      .when(bpmDAO)
      .getBillingProfile(equalTo(testData.azureBillingProfile.getId), any())

    // For testing of old Azure billing projects, can be removed if that code is removed.
    doReturn(Future.successful(testData.oldAzureBillingProject))
      .when(mcWorkspaceService)
      .getBillingProjectContext(equalTo(testData.oldAzureBillingProject.projectName), any())
    doReturn(Some(testData.oldAzureBillingProfile))
      .when(bpmDAO)
      .getBillingProfile(equalTo(testData.oldAzureBillingProfile.getId), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getV2WorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getV2WorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.workspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.workspace.toWorkspaceName), any())

    doReturn(Future.successful(testData.azureWorkspace))
      .when(mcWorkspaceService)
      .getWorkspaceContext(equalTo(testData.azureWorkspace.toWorkspaceName), any())

    runTest(mcWorkspaceService)
  }

  it should "fail if the destination workspace already exists" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.azureWorkspace)
              _ <- mcWorkspaceService.cloneMultiCloudWorkspace(
                mock[WorkspaceService](RETURNS_SMART_NULLS),
                testData.azureWorkspace.toWorkspaceName,
                WorkspaceRequest(
                  testData.azureWorkspace.namespace,
                  testData.azureWorkspace.name,
                  Map.empty
                )
              )
            } yield fail(
              "cloneMultiCloudWorkspace does not fail when a workspace " +
                "with the same name already exists."
            ),
            Duration.Inf
          )
        }

        result.errorReport.statusCode.value shouldBe StatusCodes.Conflict
      }
    }

  it should "not create a workspace record if the request to Workspace Manager fails" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.cloneWorkspace(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                any(),
                                                                equalTo("kifflom"),
                                                                any(),
                                                                equalTo(testData.azureWorkspace.namespace),
                                                                any(),
                                                                any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.ImATeapot, "short and stout"))
        )

        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        // preserve the error workspace manager returned
        result.errorReport.statusCode.value shouldBe StatusCodes.ImATeapot

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "throw an exception if the billing profile was created before 9/12/2023" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.oldAzureBillingProject.projectName.value, "unsupported")
        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            for {
              _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.azureWorkspace)
              _ <- mcWorkspaceService.cloneMultiCloudWorkspace(
                mock[WorkspaceService](RETURNS_SMART_NULLS),
                testData.azureWorkspace.toWorkspaceName,
                WorkspaceRequest(
                  cloneName.namespace,
                  cloneName.name,
                  Map.empty
                )
              )
            } yield fail(
              "cloneMultiCloudWorkspace does not fail when a workspace " +
                "with the same name already exists."
            ),
            Duration.Inf
          )
        }
        result.errorReport.statusCode.value shouldBe StatusCodes.Forbidden
      }
    }

  it should "not create a workspace record if the Workspace Manager clone operation fails" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.getCloneWorkspaceResult(
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) => MockWorkspaceManagerDAO.getCloneWorkspaceResult(StatusEnum.FAILED))

        intercept[WorkspaceManagerOperationFailureException] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if the workspace has no storage containers" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        // workspaceManagerDAO returns an empty ResourceList by default for enumerateStorageContainers

        val actual = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        assert(actual.errorReport.message.contains("does not have the expected storage container"))

        verify(mcWorkspaceService.workspaceManagerDAO, never())
          .cloneAzureStorageContainer(any(), any(), any(), any(), any(), any(), any())

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not create a workspace record if there is no storage container with the correct name" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")

        when(
          mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          new ResourceList().addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name("not the correct name")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
        )

        val actual = intercept[RawlsExceptionWithErrorReport] {
          Await.result(
            mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map.empty
              )
            ),
            Duration.Inf
          )
        }

        assert(actual.errorReport.message.contains("does not have the expected storage container"))

        verify(mcWorkspaceService.workspaceManagerDAO, never())
          .cloneAzureStorageContainer(any(), any(), any(), any, any(), any(), any())

        // fail if the workspace exists
        val clone = Await.result(
          slickDataSource.inTransaction(_.workspaceQuery.findByName(cloneName)),
          Duration.Inf
        )

        clone shouldBe empty
      }
    }

  it should "not allow users to clone azure workspaces into gcp billing projects" in
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              testData.billingProject.projectName.value,
              UUID.randomUUID().toString,
              Map.empty
            )
          ),
          Duration.Inf
        )
      }

      result.errorReport.statusCode.value shouldBe StatusCodes.BadRequest
    }

  it should "not allow users to clone gcp workspaces into azure billing projects" in
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.workspace.toWorkspaceName,
            WorkspaceRequest(
              testData.azureBillingProject.projectName.value,
              UUID.randomUUID().toString,
              Map.empty
            )
          ),
          Duration.Inf
        )
      }

      result.errorReport.statusCode.value shouldBe StatusCodes.BadRequest
    }

  it should "deploy a WDS instance during workspace clone" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
      when(
        mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
          equalTo(testData.azureWorkspace.workspaceIdAsUUID),
          any(),
          any(),
          any()
        )
      ).thenAnswer((_: InvocationOnMock) =>
        new ResourceList().addResourcesItem(
          new ResourceDescription().metadata(
            new ResourceMetadata()
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      Await.result(
        for {
          _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
          clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              cloneName.namespace,
              cloneName.name,
              Map.empty
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning deploys WDS and then a very simple
          // validation of the clone success.
          verify(mcWorkspaceService.leonardoDAO, times(1))
            .createWDSInstance(anyString(),
                               equalTo(clone.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe McWorkspace
        },
        Duration.Inf
      )
    }
  }

  it should "not deploy a WDS instance during workspace clone if test attribute is set as a string" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
      when(
        mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
          equalTo(testData.azureWorkspace.workspaceIdAsUUID),
          any(),
          any(),
          any()
        )
      ).thenAnswer((_: InvocationOnMock) =>
        new ResourceList().addResourcesItem(
          new ResourceDescription().metadata(
            new ResourceMetadata()
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      Await.result(
        for {
          _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
          clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              cloneName.namespace,
              cloneName.name,
              Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeString("true"))
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning does not deploy WDS.
          verify(mcWorkspaceService.leonardoDAO, never())
            .createWDSInstance(anyString(),
                               equalTo(clone.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe McWorkspace
        },
        Duration.Inf
      )
    }
  }

  it should "clone the workspace even if WDS instance creation fails" in withEmptyTestDatabase {
    withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
      val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
      val sourceContainerUUID = UUID.randomUUID()
      when(
        mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
          equalTo(testData.azureWorkspace.workspaceIdAsUUID),
          any(),
          any(),
          any()
        )
      ).thenAnswer((_: InvocationOnMock) =>
        new ResourceList().addResourcesItem(
          new ResourceDescription().metadata(
            new ResourceMetadata()
              .resourceId(sourceContainerUUID)
              .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
              .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
          )
        )
      )
      // for this test, throw an error on WDS deployment
      when(mcWorkspaceService.leonardoDAO.createWDSInstance(anyString(), any[UUID](), any()))
        .thenAnswer(_ => throw new leonardo.ApiException(500, "intentional Leo exception for unit test"))

      Await.result(
        for {
          _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
          clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
            mock[WorkspaceService](RETURNS_SMART_NULLS),
            testData.azureWorkspace.toWorkspaceName,
            WorkspaceRequest(
              cloneName.namespace,
              cloneName.name,
              Map.empty
            )
          )
        } yield {
          // other tests assert that a workspace clone does all the proper things, like cloning storage and starting
          // a resource manager job. This test only checks that cloning deploys WDS and then a very simple
          // validation of the clone success.
          verify(mcWorkspaceService.leonardoDAO, times(1))
            .createWDSInstance(anyString(),
                               equalTo(clone.workspaceIdAsUUID),
                               equalTo(Some(testData.azureWorkspace.workspaceIdAsUUID))
            )
          clone.toWorkspaceName shouldBe cloneName
          clone.workspaceType shouldBe McWorkspace
        },
        Duration.Inf
      )
    }
  }

  it should
    "clone an azure workspace" +
    " & create a new workspace record with merged attributes" +
    " & create a new job for the workspace manager resource monitor" in
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val cloneName = WorkspaceName(testData.azureWorkspace.namespace, "kifflom")
        val sourceContainerUUID = UUID.randomUUID()
        when(
          mcWorkspaceService.workspaceManagerDAO.enumerateStorageContainers(
            equalTo(testData.azureWorkspace.workspaceIdAsUUID),
            any(),
            any(),
            any()
          )
        ).thenAnswer((_: InvocationOnMock) =>
          new ResourceList().addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(sourceContainerUUID)
                .name(MultiCloudWorkspaceService.getStorageContainerName(testData.azureWorkspace.workspaceIdAsUUID))
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
        )
        Await.result(
          for {
            _ <- slickDataSource.inTransaction(_.rawlsBillingProjectQuery.create(testData.azureBillingProject))
            clone <- mcWorkspaceService.cloneMultiCloudWorkspace(
              mock[WorkspaceService](RETURNS_SMART_NULLS),
              testData.azureWorkspace.toWorkspaceName,
              WorkspaceRequest(
                cloneName.namespace,
                cloneName.name,
                Map(
                  AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
                ),
                None,
                Some("analyses/"),
                policies = Some(
                  List(
                    WorkspacePolicy("dummy-policy-name", "terra", List.empty)
                  )
                )
              )
            )
            // Ensure test data has the attribute we expect to merge in.
            _ = testData.azureWorkspace.attributes shouldBe Map(
              AttributeName.withDefaultNS("description") -> AttributeString("source description")
            )
            _ = clone.toWorkspaceName shouldBe cloneName
            _ = clone.workspaceType shouldBe McWorkspace
            _ = clone.attributes shouldBe Map(
              AttributeName.withDefaultNS("description") -> AttributeString("source description"),
              AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
            )

            jobs <- slickDataSource.inTransaction { access =>
              for {
                // the newly cloned workspace should be persisted
                clone_ <- access.workspaceQuery.findByName(cloneName)
                _ = clone_.value.workspaceId shouldBe clone.workspaceId

                // a new resource monitor job should be created
                jobs <- access.WorkspaceManagerResourceMonitorRecordQuery
                  .selectByWorkspaceId(clone.workspaceIdAsUUID)
              } yield jobs
            }
          } yield {
            verify(mcWorkspaceService.workspaceManagerDAO, times(1))
              .cloneWorkspace(
                equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                equalTo(clone.workspaceIdAsUUID),
                equalTo(cloneName.name),
                any(),
                equalTo(cloneName.namespace),
                any(),
                equalTo(
                  Some(
                    new WsmPolicyInputs()
                      .inputs(
                        Seq(
                          new WsmPolicyInput()
                            .name("dummy-policy-name")
                            .namespace("terra")
                        ).asJava
                      )
                  )
                )
              )
            verify(mcWorkspaceService.workspaceManagerDAO, times(1))
              .cloneAzureStorageContainer(
                equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                equalTo(clone.workspaceIdAsUUID),
                equalTo(sourceContainerUUID),
                equalTo(getStorageContainerName(clone.workspaceIdAsUUID)),
                equalTo(CloningInstructionsEnum.RESOURCE),
                equalTo(Some("analyses/")),
                any()
              )
            clone.completedCloneWorkspaceFileTransfer shouldBe None
            jobs.size shouldBe 1
            jobs.head.jobType shouldBe JobType.CloneWorkspaceContainerResult
            jobs.head.workspaceId.value.toString shouldBe clone.workspaceId
          },
          Duration.Inf
        )
      }
    }

  behavior of "deleteMultiCloudOrRawlsWorkspaceV2"

  it should "delete a rawls workspace synchronously" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.workspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
        when(workspaceService.deleteWorkspace(ArgumentMatchers.eq(wsName)))
          .thenReturn(
            Future.successful(WorkspaceDeletionResult.fromGcpBucketName(testData.workspace.bucketName))
          )

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.workspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspaceV2(
              wsName,
              workspaceService
            )
          } yield {
            verify(workspaceService).deleteWorkspace(equalTo(wsName))
            result shouldBe WorkspaceDeletionResult.fromGcpBucketName(testData.workspace.bucketName)
          },
          Duration.Inf
        )
      }
    }
  }

  it should "create a new job for the workspace manager resource monitor when deleting an azure workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)

            deletionResult <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspaceV2(
              testData.azureWorkspace.toWorkspaceName,
              mock[WorkspaceService](RETURNS_SMART_NULLS)
            )

            jobs <- slickDataSource.inTransaction { access =>
              for {
                // a new resource monitor job should be created
                jobs <- access.WorkspaceManagerResourceMonitorRecordQuery
                  .selectByWorkspaceId(testData.azureWorkspace.workspaceIdAsUUID)
              } yield jobs
            }
          } yield {
            deletionResult.jobId shouldBe defined
            jobs.size shouldBe 1
            jobs.head.jobType shouldBe JobType.WorkspaceDeleteInit
            jobs.head.workspaceId.value.toString shouldBe testData.azureWorkspace.workspaceId
            assertWorkspaceExists(testData.azureWorkspace)
            assertWorkspaceState(testData.azureWorkspace, WorkspaceState.Deleting)
          },
          Duration.Inf
        )
      }
    }
  }

  it should "fail with a conflict if the workspace is already deleting" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val alreadyDeletingWs = Workspace.buildMcWorkspace(
          namespace = testData.azureBillingProject.projectName.value,
          name = "fake",
          workspaceId = UUID.randomUUID().toString,
          createdDate = DateTime.now,
          lastModified = DateTime.now,
          createdBy = "fake",
          attributes = Map.empty,
          WorkspaceState.Deleting
        )
        Await.result(insertWorkspaceWithBillingProject(testData.azureBillingProject, alreadyDeletingWs), Duration.Inf)

        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(mcWorkspaceService.deleteMultiCloudOrRawlsWorkspaceV2(alreadyDeletingWs.toWorkspaceName,
                                                                             mock[WorkspaceService](RETURNS_SMART_NULLS)
                       ),
                       Duration.Inf
          )
        }

        result.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
      }
    }
  }

  it should "fail with an error if the workspace is in an undeleteable state" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val alreadyDeletingWs = Workspace.buildMcWorkspace(
          namespace = testData.azureBillingProject.projectName.value,
          name = "fake",
          workspaceId = UUID.randomUUID().toString,
          createdDate = DateTime.now,
          lastModified = DateTime.now,
          createdBy = "fake",
          attributes = Map.empty,
          WorkspaceState.Creating
        )
        Await.result(insertWorkspaceWithBillingProject(testData.azureBillingProject, alreadyDeletingWs), Duration.Inf)

        val result = intercept[RawlsExceptionWithErrorReport] {
          Await.result(mcWorkspaceService.deleteMultiCloudOrRawlsWorkspaceV2(alreadyDeletingWs.toWorkspaceName,
                                                                             mock[WorkspaceService](RETURNS_SMART_NULLS)
                       ),
                       Duration.Inf
          )
        }

        result.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
      }
    }
  }

  behavior of "deleteMultiCloudOrRawlsWorkspace"

  it should "fail if user does not have delete permissions on the workspace" in {
    withEmptyTestDatabase {
      val samDAO = Mockito.spy(new MockSamDAO(slickDataSource))
      when(
        samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.azureWorkspace.workspaceId),
          ArgumentMatchers.eq(SamWorkspaceActions.delete),
          any()
        )
      ).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "forbidden"))))
      val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

      val svc = MultiCloudWorkspaceService.constructor(
        slickDataSource,
        mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
        mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS),
        samDAO,
        activeMcWorkspaceConfig,
        mock[LeonardoDAO](RETURNS_SMART_NULLS),
        workbenchMetricBaseName
      )(testContext)
      val result = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)
            _ <- svc.deleteMultiCloudOrRawlsWorkspace(testData.azureWorkspace.toWorkspaceName, workspaceService)
          } yield {},
          Duration.Inf
        )
      }

      result.errorReport.statusCode shouldEqual Some(StatusCodes.Forbidden)
      assertWorkspaceExists(testData.azureWorkspace)
    }
  }

  it should "delete an MC workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.azureWorkspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield result shouldBe None,
          Duration.Inf
        )

        verify(mcWorkspaceService).deleteWorkspaceInWSM(equalTo(testData.azureWorkspace.workspaceIdAsUUID))
        assertWorkspaceGone(testData.azureWorkspace)
      }
    }
  }

  it should "delete a rawls workspace" in {
    withEmptyTestDatabase {
      withMockedMultiCloudWorkspaceService { mcWorkspaceService =>
        val wsName = testData.workspace.toWorkspaceName
        val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
        when(workspaceService.deleteWorkspace(ArgumentMatchers.eq(wsName)))
          .thenReturn(
            Future.successful(WorkspaceDeletionResult.fromGcpBucketName(testData.workspace.bucketName))
          )

        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.billingProject, testData.workspace)
            result <- mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield {
            verify(workspaceService).deleteWorkspace(equalTo(wsName))
            result shouldBe Some(testData.workspace.bucketName)
          },
          Duration.Inf
        )
      }
    }
  }

  it should "leave the rawls state in place if WSM returns an error during workspace deletion" in {
    withEmptyTestDatabase {
      val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
      when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
        throw new ApiException(500, "error")
      )

      val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
      val wsName = testData.azureWorkspace.toWorkspaceName
      val svc = MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        mock[BillingProfileManagerDAO],
        new MockSamDAO(slickDataSource),
        activeMcWorkspaceConfig,
        mock[LeonardoDAO],
        workbenchMetricBaseName
      )(testContext)

      intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          for {
            _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)
            _ <- svc.deleteMultiCloudOrRawlsWorkspace(
              wsName,
              workspaceService
            )
          } yield verify(workspaceService, times(0)).deleteWorkspace(any()),
          Duration.Inf
        )
      }

      verify(workspaceManagerDAO).getWorkspace(equalTo(testData.azureWorkspace.workspaceIdAsUUID), any())
      verify(workspaceManagerDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
      assertWorkspaceExists(testData.azureWorkspace)
    }
  }

  it should "delete the rawls record if the WSM record is not found" in {
    withEmptyTestDatabase {
      val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
      when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
        throw new ApiException(404, "not found")
      )

      val workspaceService = mock[WorkspaceService](RETURNS_SMART_NULLS)
      val wsName = testData.azureWorkspace.toWorkspaceName
      val svc = MultiCloudWorkspaceService.constructor(
        slickDataSource,
        workspaceManagerDAO,
        mock[BillingProfileManagerDAO],
        new MockSamDAO(slickDataSource),
        activeMcWorkspaceConfig,
        mock[LeonardoDAO],
        workbenchMetricBaseName
      )(testContext)
      Await.result(
        for {
          _ <- insertWorkspaceWithBillingProject(testData.azureBillingProject, testData.azureWorkspace)
          _ <- svc.deleteMultiCloudOrRawlsWorkspace(
            wsName,
            workspaceService
          )
        } yield verify(workspaceService, times(0)).deleteWorkspace(any()),
        Duration.Inf
      )

      // rawls workspace should be deleted if WSM returns not found
      verify(workspaceManagerDAO, times(0)).deleteWorkspaceV2(any(), anyString(), any())
      assertWorkspaceGone(testData.azureWorkspace)
    }
  }

  behavior of "deleteWorkspaceInWSM"

  it should "not attempt to delete a workspace and raise an exception if WSM returns a failure when getting the workspace" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(403, "forbidden")
    )

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      new MockSamDAO(slickDataSource),
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.errorReport.statusCode shouldEqual Some(StatusCodes.InternalServerError)
    verify(svc.workspaceManagerDAO, times(0)).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                anyString(),
                                                                any[RawlsRequestContext]
    )
  }

  it should "not attempt to delete a workspace in WSM that does not exist in WSM" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())

    val samDAO = new MockSamDAO(slickDataSource)
    when(workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext])).thenAnswer(_ =>
      throw new ApiException(404, "workspace not found")
    )

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO, times(0)).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                                anyString(),
                                                                any[RawlsRequestContext]
    )
  }

  it should "fail for for non 403 errors" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(
      _ => throw new ApiException(StatusCodes.BadRequest.intValue, "failure")
    )
    val samDAO = new MockSamDAO(slickDataSource)
    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[ApiException] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(1)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "fail for for non-ApiException errors" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO())
    when(workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], anyString(), any[RawlsRequestContext])).thenAnswer(
      _ => throw new Exception("failure")
    )
    val samDAO = new MockSamDAO(slickDataSource)
    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)

    val actual = intercept[Exception] {
      Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    }

    actual.getMessage shouldBe "failure"
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(1)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "delete the rawls workspace when workspace manager returns 403 during polling" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0

      override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                              jobControlId: String,
                                              ctx: RawlsRequestContext
      ): JobResult = {
        times = times + 1
        if (times > 1) {
          throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden")
        } else {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.RUNNING))
        }
      }
    })

    val samDAO = new MockSamDAO(slickDataSource)

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(2)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  it should "start deletion and poll for success when the workspace exists in workspace manager" in {
    val workspaceManagerDAO = Mockito.spy(new MockWorkspaceManagerDAO() {
      var times = 0
      override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                              jobControlId: String,
                                              ctx: RawlsRequestContext
      ): JobResult = {
        times = times + 1
        if (times > 1) {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.SUCCEEDED))
        } else {
          new JobResult().jobReport(new JobReport().id(jobControlId).status(StatusEnum.RUNNING))
        }
      }
    })

    val samDAO = new MockSamDAO(slickDataSource)

    val svc = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      activeMcWorkspaceConfig,
      mock[LeonardoDAO],
      workbenchMetricBaseName
    )(testContext)
    Await.result(svc.deleteWorkspaceInWSM(testData.azureWorkspace.workspaceIdAsUUID), Duration.Inf)
    verify(svc.workspaceManagerDAO).deleteWorkspaceV2(equalTo(testData.azureWorkspace.workspaceIdAsUUID),
                                                      anyString(),
                                                      any[RawlsRequestContext]
    )
    verify(svc.workspaceManagerDAO, times(2)).getDeleteWorkspaceV2Result(
      equalTo(testData.azureWorkspace.workspaceIdAsUUID),
      anyString,
      any[RawlsRequestContext]
    )
  }

  private def assertWorkspaceState(workspace: Workspace, expectedState: WorkspaceState) = {
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findByName(workspace.toWorkspaceName)),
      Duration.Inf
    )

    existing.get.state shouldBe expectedState
  }

  private def assertWorkspaceGone(workspace: Workspace) = {
    // fail if the workspace exists
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findByName(workspace.toWorkspaceName)),
      Duration.Inf
    )

    existing shouldBe empty
  }

  private def assertWorkspaceExists(workspace: Workspace) = {
    val existing = Await.result(
      slickDataSource.inTransaction(_.workspaceQuery.findById(workspace.workspaceId)),
      Duration.Inf
    )

    existing.get.workspaceId shouldBe workspace.workspaceId
  }

  private def insertWorkspaceWithBillingProject(billingProject: RawlsBillingProject, workspace: Workspace) =
    slickDataSource.inTransaction { access =>
      access.rawlsBillingProjectQuery.create(billingProject) >> access.workspaceQuery.createOrUpdate(workspace)
    }
}
