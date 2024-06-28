package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import bio.terra.profile.model.{CloudPlatform, ProfileModel}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{CloneWorkspaceResult, JobReport, WsmPolicyInputs}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeString,
  CreationStatuses,
  ManagedGroupRef,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsGroupName,
  RawlsRequestContext,
  RawlsUserEmail,
  SamBillingProjectActions,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  UserInfo,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceDetails,
  WorkspacePolicy,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.SeqHasAsJava

// kept separate from MultiCloudWorkspaceServiceSpec to separate true unit tests from tests with awkward actors
class MultiCloudWorkspaceServiceUnitTestsSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with Matchers
    with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")

  val requestContext = mock[RawlsRequestContext]
  when(requestContext.otelContext).thenReturn(None)

  behavior of "cloneMultiCloudWorkspaceAsync"

  it should "pass a request to clone an azure workspace to cloneAzureWorkspaceAsync" in {
    // Set up static data
    val sourceWorkspaceName = "source-name"
    val sourceWorkspaceNamespace = "source-namespace"
    val sourceWorkspace = Workspace.buildMcWorkspace(
      sourceWorkspaceNamespace,
      sourceWorkspaceName,
      UUID.randomUUID().toString,
      DateTime.now(),
      DateTime.now(),
      "creator",
      Map(),
      WorkspaceState.Ready
    )
    val destWorkspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map())
    val billingProfileId = UUID.randomUUID()
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceRequest.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(billingProfileId.toString)
    )
    // Set up mocks
    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCloudPlatform()).thenReturn(CloudPlatform.AZURE)
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    when(billingProfileManagerDAO.getBillingProfile(billingProfileId, requestContext)).thenReturn(Some(billingProfile))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(requestContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        requestContext
      )
    ).thenReturn(Future(true))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        requestContext
      )
    ).thenReturn(Future(true))
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceRequest.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val service = spy(
      new MultiCloudWorkspaceService(
        requestContext,
        mock[WorkspaceManagerDAO],
        billingProfileManagerDAO,
        samDAO,
        mock[MultiCloudWorkspaceConfig],
        mock[LeonardoDAO],
        "MultiCloudWorkspaceService-test",
        mock[WorkspaceManagerResourceMonitorRecordDao],
        workspaceRepository,
        billingRepository
      )
    )
    val destWorkspace = mock[Workspace]
    doReturn(Future(destWorkspace))
      .when(service)
      .cloneAzureWorkspaceAsync(
        ArgumentMatchers.eq(sourceWorkspace),
        ArgumentMatchers.eq(billingProfile),
        ArgumentMatchers.eq(destWorkspaceRequest),
        ArgumentMatchers.any()
      )

    val result = Await.result(
      service.cloneMultiCloudWorkspaceAsync(
        mock[WorkspaceService],
        sourceWorkspace.toWorkspaceName,
        destWorkspaceRequest
      ),
      Duration.Inf
    )

    result shouldBe WorkspaceDetails.fromWorkspaceAndOptions(destWorkspace,
                                                             Some(Set.empty),
                                                             useAttributes = true,
                                                             Some(WorkspaceCloudPlatform.Azure)
    )
    verify(service).cloneAzureWorkspaceAsync(
      ArgumentMatchers.eq(sourceWorkspace),
      ArgumentMatchers.eq(billingProfile),
      ArgumentMatchers.eq(destWorkspaceRequest),
      ArgumentMatchers.any()
    )
  }

  it should "pass a request to clone a GCP workspace to cloneWorkspace in workspaceService" in {
    // Static data
    val sourceWorkspaceName = "source-name"
    val sourceWorkspaceNamespace = "source-namespace"
    val sourceWorkspace = Workspace.buildWorkspace(
      sourceWorkspaceNamespace,
      sourceWorkspaceName,
      UUID.randomUUID().toString,
      DateTime.now(),
      DateTime.now(),
      "creator",
      Map(),
      WorkspaceState.Ready,
      WorkspaceType.RawlsWorkspace
    )
    val authDomain = Some(Set(ManagedGroupRef(RawlsGroupName("Test-Realm"))))
    val destWorkspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map(), authorizationDomain = authDomain)
    val billingProfileId = UUID.randomUUID()
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceRequest.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(billingProfileId.toString)
    )
    // Mocks
    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCloudPlatform).thenReturn(CloudPlatform.GCP)
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    when(billingProfileManagerDAO.getBillingProfile(billingProfileId, requestContext)).thenReturn(Some(billingProfile))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(requestContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        requestContext
      )
    ).thenReturn(Future(true))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        requestContext
      )
    ).thenReturn(Future(true))
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceRequest.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val workspaceService = mock[WorkspaceService]
    val destWorkspace = mock[Workspace]
    when(
      workspaceService
        .cloneWorkspace(
          ArgumentMatchers.eq(sourceWorkspace),
          ArgumentMatchers.eq(billingProject),
          ArgumentMatchers.eq(destWorkspaceRequest),
          ArgumentMatchers.any()
        )
    ).thenReturn(Future(destWorkspace))
    val service = new MultiCloudWorkspaceService(
      requestContext,
      mock[WorkspaceManagerDAO],
      billingProfileManagerDAO,
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val result = Await.result(
      service.cloneMultiCloudWorkspaceAsync(
        workspaceService,
        sourceWorkspace.toWorkspaceName,
        destWorkspaceRequest
      ),
      Duration.Inf
    )
    result shouldBe WorkspaceDetails.fromWorkspaceAndOptions(
      destWorkspace,
      authDomain,
      useAttributes = true,
      Some(WorkspaceCloudPlatform.Gcp)
    )
    verify(workspaceService).cloneWorkspace(
      ArgumentMatchers.eq(sourceWorkspace),
      ArgumentMatchers.eq(billingProject),
      ArgumentMatchers.eq(destWorkspaceRequest),
      ArgumentMatchers.any()
    )
  }

  behavior of "cloneAzureWorkspaceAsync"

  it should "delete the new workspace on failures" in {
    val sourceWorkspaceName = "source-name"
    val sourceWorkspaceNamespace = "source-namespace"
    val sourceWorkspace = Workspace.buildMcWorkspace(
      sourceWorkspaceNamespace,
      sourceWorkspaceName,
      UUID.randomUUID().toString,
      DateTime.now(),
      DateTime.now(),
      "creator",
      Map(),
      WorkspaceState.Ready
    )
    val destWorkspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map())
    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    doAnswer(_ => throw new ApiException())
      .when(workspaceManagerDAO)
      .cloneWorkspace(any(), any(), any(), any(), any(), any(), any())
    val workspaceRepository = mock[WorkspaceRepository]
    val service = new MultiCloudWorkspaceService(
      requestContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCreatedDate).thenReturn(DateTime.now().toString)

    val destWorkspace = mock[Workspace]

    doReturn(Future(destWorkspace))
      .when(workspaceRepository)
      .createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(WorkspaceState.Cloning)
      )(ArgumentMatchers.any())

    when(workspaceRepository.deleteWorkspace(destWorkspace)).thenReturn(Future(true))

    intercept[ApiException] {
      Await.result(
        service.cloneAzureWorkspaceAsync(sourceWorkspace, billingProfile, destWorkspaceRequest, requestContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository).deleteWorkspace(destWorkspace)
  }

  it should "doesn't try to delete the workspace when creating the new db record in rawls fails" in {
    val sourceWorkspaceName = "source-name"
    val sourceWorkspaceNamespace = "source-namespace"
    val sourceWorkspace = Workspace.buildMcWorkspace(
      sourceWorkspaceNamespace,
      sourceWorkspaceName,
      UUID.randomUUID().toString,
      DateTime.now(),
      DateTime.now(),
      "creator",
      Map(),
      WorkspaceState.Ready
    )
    val destWorkspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map())
    val workspaceRepository = mock[WorkspaceRepository]
    val service = new MultiCloudWorkspaceService(
      requestContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCreatedDate).thenReturn(DateTime.now().toString)

    val destWorkspace = mock[Workspace]
    doReturn(Future(new Exception()))
      .when(workspaceRepository)
      .createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(WorkspaceState.Cloning)
      )(ArgumentMatchers.any())

    intercept[Exception] {
      Await.result(
        service.cloneAzureWorkspaceAsync(sourceWorkspace, billingProfile, destWorkspaceRequest, requestContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository, never()).deleteWorkspace(destWorkspace)
  }

  it should "create the async clone job from the result in WSM" in {
    val sourceWorkspace = Workspace.buildMcWorkspace(
      "source-namespace",
      "source-name",
      UUID.randomUUID().toString,
      DateTime.now(),
      DateTime.now(),
      "creator",
      Map(),
      WorkspaceState.Ready
    )
    val userInfo = mock[UserInfo]
    when(userInfo.userEmail).thenReturn(RawlsUserEmail("user-email"))
    when(requestContext.userInfo).thenReturn(userInfo)
    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCreatedDate).thenReturn(DateTime.now().toString)
    val policies = List(WorkspacePolicy("test-name", "test-namespace", List()))
    val destWorkspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map(), policies = Some(policies))

    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val wsmResult = new CloneWorkspaceResult().jobReport(new JobReport().id("test-id-that-isn't-a-uuid"))
    when(
      workspaceManagerDAO.cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest.name),
        ArgumentMatchers.eq(Some(billingProfile)),
        ArgumentMatchers.eq(destWorkspaceRequest.namespace),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(Some(new WsmPolicyInputs().inputs(policies.map(p => p.toWsmPolicyInput()).asJava)))
      )
    ).thenReturn(wsmResult)

    val workspaceManagerResourceMonitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val record: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      record.userEmail shouldBe Some("user-email")
      record.jobType shouldBe JobType.CloneWorkspaceInit
      Future.successful()
    }.when(workspaceManagerResourceMonitorRecordDao).create(any())

    val workspaceRepository = mock[WorkspaceRepository]
    val service = new MultiCloudWorkspaceService(
      requestContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      workspaceManagerResourceMonitorRecordDao,
      workspaceRepository,
      mock[BillingRepository]
    )
    val destWorkspace = mock[Workspace]
    doReturn(Future.successful(destWorkspace))
      .when(workspaceRepository)
      .createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(WorkspaceState.Cloning)
      )(ArgumentMatchers.any())

    Await.result(
      service.cloneAzureWorkspaceAsync(sourceWorkspace, billingProfile, destWorkspaceRequest, requestContext),
      Duration.Inf
    ) shouldBe destWorkspace

    verify(workspaceManagerResourceMonitorRecordDao).create(any())
  }

  it should "merge together source and destination attributes" in {
    val sourceAttributes = Map(
      AttributeName.withDefaultNS("description") -> AttributeString("source description")
    )
    val sourceWorkspace = Workspace.buildMcWorkspace(
      "source-namespace",
      "source-name",
      UUID.randomUUID().toString,
      DateTime.now(),
      DateTime.now(),
      "creator",
      sourceAttributes,
      WorkspaceState.Ready
    )
    val userInfo = mock[UserInfo]
    when(userInfo.userEmail).thenReturn(RawlsUserEmail("user-email"))
    when(requestContext.userInfo).thenReturn(userInfo)
    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCreatedDate).thenReturn(DateTime.now().toString)
    val policies = List(WorkspacePolicy("test-name", "test-namespace", List()))
    val destinationAttributes = Map(
      AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
    )
    val destWorkspaceRequest =
      WorkspaceRequest("dest-namespace", "dest-name", destinationAttributes, policies = Some(policies))

    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    val wsmResult = new CloneWorkspaceResult().jobReport(new JobReport().id("test-id-that-isn't-a-uuid"))
    when(
      workspaceManagerDAO.cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest.name),
        ArgumentMatchers.eq(Some(billingProfile)),
        ArgumentMatchers.eq(destWorkspaceRequest.namespace),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(Some(new WsmPolicyInputs().inputs(policies.map(p => p.toWsmPolicyInput()).asJava)))
      )
    ).thenReturn(wsmResult)

    val workspaceManagerResourceMonitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doAnswer { a =>
      val record: WorkspaceManagerResourceMonitorRecord = a.getArgument(0)
      record.userEmail shouldBe Some("user-email")
      record.jobType shouldBe JobType.CloneWorkspaceInit
      Future.successful()
    }.when(workspaceManagerResourceMonitorRecordDao).create(any())
    val workspaceRepository = mock[WorkspaceRepository]
    val service = new MultiCloudWorkspaceService(
      requestContext,
      workspaceManagerDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      workspaceManagerResourceMonitorRecordDao,
      workspaceRepository,
      mock[BillingRepository]
    )
    val destWorkspace = mock[Workspace]
    val mergedAttributes = Map(
      AttributeName.withDefaultNS("description") -> AttributeString("source description"),
      AttributeName.withDefaultNS("destination") -> AttributeString("destination only")
    )
    val mergedWorkspaceRequest =
      WorkspaceRequest("dest-namespace", "dest-name", mergedAttributes, policies = Some(policies))
    doReturn(Future.successful(destWorkspace))
      .when(workspaceRepository)
      .createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(mergedWorkspaceRequest.toWorkspaceName),
        ArgumentMatchers.eq(mergedAttributes),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(WorkspaceState.Cloning)
      )(ArgumentMatchers.any())

    Await.result(
      service.cloneAzureWorkspaceAsync(sourceWorkspace, billingProfile, destWorkspaceRequest, requestContext),
      Duration.Inf
    ) shouldBe destWorkspace

    verify(workspaceManagerResourceMonitorRecordDao).create(any())
  }
}
