package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{
  CloudPlatform,
  CreateWorkspaceV2Result,
  CreatedControlledAzureStorageContainer,
  CreatedControlledGcpGcsBucket,
  GcpContext,
  JobReport,
  JobResult,
  WorkspaceDescription,
  WsmPolicyInput,
  WsmPolicyInputs,
  WsmPolicyPair
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  CreationStatuses,
  ErrorReport,
  GoogleProjectId,
  ManagedGroupRef,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsGroupName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceTypeNames,
  UserInfo,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceName,
  WorkspacePolicy,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.joda.time.DateTime
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, never, verify, when}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class MultiCloudWorkspaceServiceCreateSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with OptionValues {

  implicit val executionContext: TestExecutionContext = new TestExecutionContext()
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")

  val userInfo: UserInfo = UserInfo(RawlsUserEmail("owner-access"),
                                    OAuth2BearerToken("token"),
                                    123,
                                    RawlsUserSubjectId("123456789876543212345")
  )
  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)
  val namespace: String = "fake-namespace"
  val name: String = "fake-name"
  val workspaceName: WorkspaceName = WorkspaceName(namespace, name)
  val workspaceId: UUID = UUID.randomUUID()
  val defaultWorkspace: Workspace = Workspace.buildMcWorkspace(
    namespace = namespace,
    name = name,
    workspaceId = workspaceId.toString,
    DateTime.now(),
    DateTime.now(),
    createdBy = testContext.userInfo.userEmail.value,
    attributes = Map.empty,
    state = WorkspaceState.Ready
  )

  behavior of "createMultiCloudOrRawlsWorkspace"

  it should "return forbidden if the user does not have the createWorkspace action for the billing project" in {
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("azure-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )

    val samDAO = mock[SamDAO]
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(false))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      billingRepository
    )
    val workspaceRequest = WorkspaceRequest(
      billingProject.projectName.value,
      UUID.randomUUID().toString,
      Map.empty,
      None,
      None,
      None,
      None
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudOrRawlsWorkspace(workspaceRequest, mock[WorkspaceService]), Duration.Inf)
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if the billing profile is not found in BPM" in {
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("azure-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(bpmDAO.getBillingProfile(any[UUID], any[RawlsRequestContext])).thenReturn(None)
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      bpmDAO,
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      billingRepository
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

    val actual = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudOrRawlsWorkspace(request, mock[WorkspaceService]),
        Duration.Inf
      )
    }

    assert(actual.errorReport.message.contains("Unable to find billing profile"))
  }

  behavior of "createMultiCloudWorkspace"

  it should "create a 'legacy' GCP workspace if no billing profile is present" in {
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("gcp-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val mcConfig =
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig])
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val workspaceRepository = mock[WorkspaceRepository]
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      mcConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val request = WorkspaceRequest(namespace, name, Map.empty, bucketLocation = Some("us-central1"))
    val legacyService = mock[WorkspaceService]
    when(legacyService.createWorkspace(any, any)).thenReturn(Future(defaultWorkspace))

    val result =
      Await.result(service.createMultiCloudOrRawlsWorkspace(request, legacyService, testContext), Duration.Inf)

    result.state shouldBe WorkspaceState.Ready
    result.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    verify(legacyService).createWorkspace(any, any)
    verify(workspaceManagerDAO, never).createWorkspaceWithSpendProfile(
      any,
      any,
      any,
      any,
      any,
      any,
      any,
      any
    )
    verify(workspaceManagerDAO, never).createGcpStorageBucket(any, any, any, any)
  }

  it should "create a 'legacy' GCP workspace if the billing profile is GCP and no flag is supplied" in {
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("gcp-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val mcConfig =
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig])
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val workspaceRepository = mock[WorkspaceRepository]
    val bpmDAO = mock[BillingProfileManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.GCP)
    when(bpmDAO.getBillingProfile(any(), any())).thenReturn(Some(billingProfile))
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      bpmDAO,
      samDAO,
      mcConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val request = WorkspaceRequest(namespace, name, Map.empty, bucketLocation = Some("us-central1"))
    val legacyService = mock[WorkspaceService]
    when(legacyService.createWorkspace(any, any)).thenReturn(Future(defaultWorkspace))

    val result =
      Await.result(service.createMultiCloudOrRawlsWorkspace(request, legacyService, testContext), Duration.Inf)

    result.state shouldBe WorkspaceState.Ready
    result.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    verify(legacyService).createWorkspace(any, any)
    verify(workspaceManagerDAO, never).createWorkspaceWithSpendProfile(
      any,
      any,
      ArgumentMatchers.eq(billingProfile.getId.toString),
      any,
      any,
      ArgumentMatchers.eq(CloudPlatform.GCP),
      any,
      any
    )
    verify(workspaceManagerDAO, never).createGcpStorageBucket(any, any, any, any)
  }

  it should "create a GCP workspace if the billing profile is GCP and the proper flag is supplied" in {
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName("gcp-billing-project"),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(any())).thenReturn(Future(Some(billingProject)))
    val mcConfig =
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig])
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val namespace = "fake"
    val name = "fake-name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.createMCWorkspace(any, ArgumentMatchers.eq(workspaceName), any, any, any)(any))
      .thenReturn(Future(defaultWorkspace))
    when(workspaceRepository.getWorkspace(any[UUID])).thenReturn(Future.successful(Some(defaultWorkspace)))
    when(workspaceRepository.updateBucketName(any[UUID], any[String])).thenReturn(Future.successful(1))
    when(workspaceRepository.updateGoogleProjectId(any[UUID], any[GoogleProjectId])).thenReturn(Future.successful(1))
    val bpmDAO = mock[BillingProfileManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.GCP)
    when(bpmDAO.getBillingProfile(any(), any())).thenReturn(Some(billingProfile))
    val workspaceJobId = UUID.randomUUID()

    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(workspaceId)
    when(
      workspaceManagerDAO
        .createWorkspaceWithSpendProfile(
          any(),
          any(),
          any(),
          any(),
          any(),
          ArgumentMatchers.eq(CloudPlatform.GCP),
          any(),
          any()
        )
    ).thenReturn(jobReport)
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    when(
      workspaceManagerDAO.createGcpStorageBucket(
        any(),
        any(),
        any(),
        any()
      )
    ).thenReturn(new CreatedControlledGcpGcsBucket().resourceId(UUID.randomUUID()))
    when(workspaceManagerDAO.getWorkspace(any[UUID], any()))
      .thenReturn(new WorkspaceDescription().gcpContext(new GcpContext().projectId("fake-project-id")))

    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      bpmDAO,
      samDAO,
      mcConfig,
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val request = WorkspaceRequest(namespace,
                                   name,
                                   Map(AttributeName("system", "mc_gcp") -> AttributeBoolean(true)),
                                   bucketLocation = Some("us-central1")
    )
    val legacyService = mock[WorkspaceService]
    val result =
      Await.result(service.createMultiCloudOrRawlsWorkspace(request, legacyService, testContext), Duration.Inf)

    result.state shouldBe WorkspaceState.Ready
    result.cloudPlatform shouldBe Some(WorkspaceCloudPlatform.Gcp)
    verify(legacyService, never).createWorkspace(any, any)
    verify(workspaceManagerDAO).createWorkspaceWithSpendProfile(any,
                                                                any,
                                                                ArgumentMatchers.eq(billingProfile.getId.toString),
                                                                any,
                                                                any,
                                                                ArgumentMatchers.eq(CloudPlatform.GCP),
                                                                any,
                                                                any
    )
    verify(workspaceManagerDAO).createGcpStorageBucket(any, any, any, any)
  }

  it should "not delete the original workspace if a workspace with the same name already exists" in {
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val namespace = "fake"
    val name = "fake-name"
    val workspaceName = WorkspaceName(namespace, name)
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.createMCWorkspace(any, ArgumentMatchers.eq(workspaceName), any, any, any)(any))
      .thenAnswer { _ =>
        throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"Workspace '$name' already exists"))
      }
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )
    val request = WorkspaceRequest(namespace, name, Map.empty)

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudWorkspace(request, new ProfileModel().id(UUID.randomUUID())), Duration.Inf)
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Conflict)
    // Make sure that the pre-existing workspace was not deleted.
    verify(workspaceManagerDAO, never).deleteWorkspaceV2(any, any, any)
    verify(workspaceRepository, never).deleteWorkspace(workspaceName)
  }

  it should "throw an exception if the billing profile was created before 9/12/2023" in {
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )
    val request = WorkspaceRequest(
      "fake",
      s"fake-name-${UUID.randomUUID().toString}",
      Map.empty
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudWorkspace(
                     request,
                     new ProfileModel().id(UUID.randomUUID()).createdDate("2023-09-11T22:20:48.949Z")
                   ),
                   Duration.Inf
      )
    }

    thrown.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "not throw an exception for billing profiles created 9/12/2023 or later" in {
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    // calls assertBillingProfileCreationDate directly instead of createMultiCloudWorkspace
    // so we don't need to mock all the successes
    // we already know createMultiCloudWorkspace is calling assertBillingProfileCreationDate, because of the test above
    service.assertBillingProfileCreationDate(
      new ProfileModel().id(UUID.randomUUID()).createdDate("2023-09-12T22:20:48.949Z")
    ) shouldBe ()
  }

  it should "throw an exception for an Azure workspace if the workspaceRequest contains a nonempty authorizationDomain" in {
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )
    val request = WorkspaceRequest(
      "fake",
      s"fake-name-${UUID.randomUUID().toString}",
      Map.empty,
      authorizationDomain = Option(Set(ManagedGroupRef(RawlsGroupName("authDomain"))))
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.createMultiCloudWorkspace(
                     request,
                     new ProfileModel().id(UUID.randomUUID()).createdDate("2023-09-14T22:20:48.949Z")
                   ),
                   Duration.Inf
      )
    }
    thrown.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "deploy a WDS instance during workspace creation for Azure workspaces" in {
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE)
    val workspaceRepository = mock[WorkspaceRepository]
    val workspaceIdCaptor = ArgumentCaptor.forClass(classOf[UUID])
    when(
      workspaceRepository.createMCWorkspace(
        workspaceIdCaptor.capture(),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(defaultWorkspace))

    val workspaceJobId = UUID.randomUUID()
    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(workspaceId)
    when(
      workspaceManagerDAO
        .createWorkspaceWithSpendProfile(
          any(),
          ArgumentMatchers.eq(name),
          ArgumentMatchers.eq(billingProfile.getId.toString),
          ArgumentMatchers.eq(namespace),
          any[Seq[String]],
          ArgumentMatchers.eq(CloudPlatform.AZURE),
          any[Option[WsmPolicyInputs]],
          ArgumentMatchers.eq(testContext)
        )
    ).thenReturn(jobReport)
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    val azureStorageContainerId = UUID.randomUUID()
    val containerResult = new CreatedControlledAzureStorageContainer().resourceId(azureStorageContainerId)
    when(workspaceManagerDAO.createAzureStorageContainer(any, any, any)).thenReturn(containerResult)
    val leonardoDAO: LeonardoDAO = mock[LeonardoDAO]
    doNothing().when(leonardoDAO).createWDSInstance(any(), any(), any())
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val request = WorkspaceRequest(namespace, name, Map.empty)
    val result: Workspace =
      Await.result(service.createMultiCloudWorkspaceInt(request, workspaceId, billingProfile, testContext),
                   Duration.Inf
      )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
        ArgumentMatchers.eq(name),
        ArgumentMatchers.eq(billingProfile.getId.toString),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        any[Option[WsmPolicyInputs]],
        ArgumentMatchers.eq(testContext)
      )

    verify(leonardoDAO).createWDSInstance(testContext.userInfo.accessToken.token, workspaceIdCaptor.getValue, None)

    verify(workspaceManagerDAO).createAzureStorageContainer(
      workspaceIdCaptor.getValue,
      MultiCloudWorkspaceService.getStorageContainerName(workspaceIdCaptor.getValue),
      testContext
    )
  }

  it should "not deploy a WDS instance during workspace creation if test attribute is set as a boolean" in {
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE)
    val workspaceRepository = mock[WorkspaceRepository]
    val attributes = Map(AttributeName.withDefaultNS("disableAutomaticAppCreation") -> AttributeBoolean(true))
    val workspace = defaultWorkspace
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.eq(attributes),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))

    val workspaceJobId = UUID.randomUUID()
    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(UUID.fromString(workspace.workspaceId))

    when(
      workspaceManagerDAO
        .createWorkspaceWithSpendProfile(
          any(),
          ArgumentMatchers.eq(name),
          ArgumentMatchers.eq(billingProfile.getId.toString),
          ArgumentMatchers.eq(namespace),
          any[Seq[String]],
          ArgumentMatchers.eq(CloudPlatform.AZURE),
          any[Option[WsmPolicyInputs]],
          ArgumentMatchers.eq(testContext)
        )
    ).thenReturn(
      jobReport
    )
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    val azureStorageContainerId = UUID.randomUUID()
    val containerResult = new CreatedControlledAzureStorageContainer().resourceId(azureStorageContainerId)
    when(
      workspaceManagerDAO.createAzureStorageContainer(
        workspaceId,
        MultiCloudWorkspaceService.getStorageContainerName(workspaceId),
        testContext
      )
    )
      .thenReturn(containerResult)

    val leonardoDAO: LeonardoDAO = mock[LeonardoDAO]
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val request = WorkspaceRequest(
      namespace,
      name,
      attributes
    )
    val result: Workspace =
      Await.result(service.createMultiCloudWorkspaceInt(request, workspaceId, billingProfile, testContext),
                   Duration.Inf
      )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace

    verify(workspaceManagerDAO).createWorkspaceWithSpendProfile(
      ArgumentMatchers.eq(UUID.fromString(result.workspaceId)),
      ArgumentMatchers.eq(name),
      any(), // spend profile id
      ArgumentMatchers.eq(namespace),
      any[Seq[String]],
      ArgumentMatchers.eq(CloudPlatform.AZURE),
      any[Option[WsmPolicyInputs]],
      ArgumentMatchers.eq(testContext)
    )
    verify(workspaceManagerDAO)
      .createAzureStorageContainer(
        UUID.fromString(result.workspaceId),
        MultiCloudWorkspaceService.getStorageContainerName(UUID.fromString(result.workspaceId)),
        testContext
      )
    verify(leonardoDAO, never()).createWDSInstance(any, any, any)

  }

  it should "fail and rollback workspace creation on initial WSM workspace creation failure" in {
    val workspace = defaultWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        any,
        ArgumentMatchers.eq(testContext),
        any
      )(any)
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    // The primary failure
    when(wsmDAO.createWorkspaceWithSpendProfile(any, any, any, any, any, any, any, any)).thenAnswer { _ =>
      throw new ApiException(404, "i've never seen that workspace in my life")
    }
    val deleteJobId = UUID.randomUUID()
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any()))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString).status(StatusEnum.SUCCEEDED)))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE),
          testContext
        ),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any())
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext)
  }

  it should "fail and rollback workspace creation on async workspace creation job failure in WSM" in {
    val workspace = defaultWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val createJobId = UUID.randomUUID()
    when(wsmDAO.createWorkspaceWithSpendProfile(any(), any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().id(createJobId.toString)))
    // This is the primary failure we're testing
    when(wsmDAO.getCreateWorkspaceResult(createJobId.toString, testContext))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().status(StatusEnum.FAILED)))
    val deleteJobId = UUID.randomUUID()
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any()))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString).status(StatusEnum.SUCCEEDED)))

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE),
          testContext
        ),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any())
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext)
  }

  it should "fail and rollback workspace creation on container creation failure in WSM " in {
    val workspace = defaultWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        any,
        ArgumentMatchers.eq(testContext),
        any
      )(any)
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val createJobId = UUID.randomUUID()
    when(wsmDAO.createWorkspaceWithSpendProfile(any, any, any, any, any, any, any, any))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().id(createJobId.toString)))
    when(wsmDAO.getCreateWorkspaceResult(createJobId.toString, testContext))
      .thenReturn(new CreateWorkspaceV2Result().jobReport(new JobReport().status(StatusEnum.SUCCEEDED)))
    val deleteJobId = UUID.randomUUID()
    when(wsmDAO.deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any, any))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString)))
    when(wsmDAO.getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext))
      .thenReturn(new JobResult().jobReport(new JobReport().id(deleteJobId.toString).status(StatusEnum.SUCCEEDED)))
    // The primary failure
    val storageContainerName = MultiCloudWorkspaceService.getStorageContainerName(workspaceId)
    when(wsmDAO.createAzureStorageContainer(workspaceId, storageContainerName, testContext)).thenAnswer { _ =>
      throw new ApiException(500, "what's a container?")
    }
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE),
          testContext
        ),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
    verify(wsmDAO).deleteWorkspaceV2(ArgumentMatchers.eq(workspaceId), any(), any())
    verify(wsmDAO).getDeleteWorkspaceV2Result(workspaceId, deleteJobId.toString, testContext)
  }

  it should "still delete from the database when cleaning up the workspace in WSM fails" in {
    val workspace = defaultWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(workspace))
    when(workspaceRepository.deleteWorkspace(workspace.toWorkspaceName)).thenReturn(Future(true))
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any, any, any, any)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.createWorkspaceWithSpendProfile(any, any, any, any, any, any, any, any)).thenAnswer { _ =>
      throw new ApiException(404, "i've never seen that workspace in my life")
    }
    when(wsmDAO.deleteWorkspaceV2(any, any, any)).thenAnswer { _ =>
      throw new ApiException(404, "Workspace not found because it wasn't created successfully")
    }
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.createMultiCloudWorkspaceInt(
          WorkspaceRequest(namespace, name, Map.empty),
          workspaceId,
          new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE),
          testContext
        ),
        Duration.Inf
      )
    }
    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(wsmDAO).deleteWorkspaceV2(any(), any(), any())
    verify(workspaceRepository).deleteWorkspace(workspace.toWorkspaceName)
  }

  it should "pass the transformed policies to WSM when creating a workspace" in {
    val samDAO = mock[SamDAO]
    when(samDAO.userHasAction(any(), any(), any(), any())).thenReturn(Future(true))
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(model.CloudPlatform.AZURE)
    val workspaceRepository = mock[WorkspaceRepository]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(WorkspaceName(namespace, name)),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(defaultWorkspace))

    val workspaceJobId = UUID.randomUUID()
    val jobReport = new CreateWorkspaceV2Result()
      .jobReport(new JobReport().id(workspaceJobId.toString).status(StatusEnum.SUCCEEDED))
      .workspaceId(workspaceId)
    val workspaceRequest = WorkspaceRequest(namespace, name, Map.empty, protectedData = Some(true))
    when(
      workspaceManagerDAO
        .createWorkspaceWithSpendProfile(
          ArgumentMatchers.eq(workspaceId),
          ArgumentMatchers.eq(name),
          ArgumentMatchers.eq(billingProfile.getId.toString),
          ArgumentMatchers.eq(namespace),
          any[Seq[String]],
          ArgumentMatchers.eq(CloudPlatform.AZURE),
          ArgumentMatchers.eq(MultiCloudWorkspaceService.buildPolicyInputs(workspaceRequest)),
          ArgumentMatchers.eq(testContext)
        )
    ).thenReturn(jobReport)
    when(workspaceManagerDAO.getCreateWorkspaceResult(workspaceJobId.toString, testContext)).thenReturn(jobReport)
    val azureStorageContainerId = UUID.randomUUID()
    val containerResult = new CreatedControlledAzureStorageContainer().resourceId(azureStorageContainerId)
    when(workspaceManagerDAO.createAzureStorageContainer(any, any, any)).thenReturn(containerResult)
    val leonardoDAO: LeonardoDAO = mock[LeonardoDAO]
    doNothing().when(leonardoDAO).createWDSInstance(any(), any(), any())
    val service = new MultiCloudWorkspaceService(
      testContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      samDAO,
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 60 seconds, 120 seconds), mock[AzureConfig]),
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result: Workspace = Await.result(
      service.createMultiCloudWorkspaceInt(workspaceRequest, workspaceId, billingProfile, testContext),
      Duration.Inf
    )

    result.name shouldBe name
    result.workspaceType shouldBe WorkspaceType.McWorkspace
    result.namespace shouldEqual namespace
    verify(workspaceManagerDAO)
      .createWorkspaceWithSpendProfile(
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(name),
        ArgumentMatchers.eq(billingProfile.getId.toString),
        ArgumentMatchers.eq(namespace),
        any[Seq[String]],
        ArgumentMatchers.eq(CloudPlatform.AZURE),
        ArgumentMatchers.eq(MultiCloudWorkspaceService.buildPolicyInputs(workspaceRequest)),
        ArgumentMatchers.eq(testContext)
      )
  }

  behavior of "buildPolicyInputs"

  it should "transform the policy inputs from the request" in {
    val requestPolicies = List(
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(namespace, name, Map.empty, policies = Some(requestPolicies))

    val policies = MultiCloudWorkspaceService.buildPolicyInputs(request)

    policies shouldBe Some(
      new WsmPolicyInputs()
        .inputs(
          Seq(
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
  }

  it should "create a policy for the protected data flag" in {
    val request = WorkspaceRequest(namespace, name, Map.empty, protectedData = Some(true))

    val policies = MultiCloudWorkspaceService.buildPolicyInputs(request)

    policies shouldBe Some(
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
  }

  it should "merge the protected data policy with other inputs in the request" in {
    val requestPolicies = List(
      WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup"))),
      WorkspacePolicy("region-constraint", "other-namespace", List(Map("key1" -> "value1"), Map("key2" -> "value2")))
    )
    val request = WorkspaceRequest(
      namespace,
      name,
      Map.empty,
      policies = Some(requestPolicies),
      protectedData = Some(true)
    )
    val policies = MultiCloudWorkspaceService.buildPolicyInputs(request)
    policies shouldBe Some(
      new WsmPolicyInputs()
        .inputs(
          Seq(
            new WsmPolicyInput()
              .name("protected-data")
              .namespace("terra")
              .additionalData(List().asJava),
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
  }
}
