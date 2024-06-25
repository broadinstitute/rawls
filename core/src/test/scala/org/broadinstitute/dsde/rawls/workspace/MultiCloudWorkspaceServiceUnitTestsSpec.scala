package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import bio.terra.profile.model.{CloudPlatform, ProfileModel}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{CloneWorkspaceResult, JobReport, WsmPolicyInputs}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{MultiCloudWorkspaceConfig, WorkspaceServiceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{ExecutionServiceCluster, ExecutionServiceDAO, GoogleServicesDAO, LeonardoDAO, MethodRepoDAO, RequesterPaysSetupService, SamDAO, SlickDataSource, SubmissionCostService, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, CreationStatuses, ManagedGroupRef, RawlsBillingProject, RawlsBillingProjectName, RawlsGroupName, RawlsRequestContext, RawlsUserEmail, SamWorkspaceActions, UserInfo, Workspace, WorkspaceCloudPlatform, WorkspaceDetails, WorkspacePolicy, WorkspaceRequest, WorkspaceState, WorkspaceType}
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
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

  behavior of "cloneMultiCloudWorkspaceAsync"

  it should "pass a request to clone an azure workspace to cloneAzureWorkspaceAsync" in {
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

    val requestContext = mock[RawlsRequestContext]
    when(requestContext.otelContext).thenReturn(None)
    val service = spy(
      MultiCloudWorkspaceService.constructor(
        mock[SlickDataSource],
        mock[WorkspaceManagerDAO],
        mock[BillingProfileManagerDAO],
        mock[SamDAO],
        mock[MultiCloudWorkspaceConfig],
        mock[LeonardoDAO],
        "MultiCloudWorkspaceService-test"
      )(requestContext)
    )

    doReturn(Future(sourceWorkspace))
      .when(service)
      .getV2WorkspaceContextAndPermissions(sourceWorkspace.toWorkspaceName, SamWorkspaceActions.read, None)

    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceRequest.namespace),
      CreationStatuses.Ready,
      None,
      None
    )

    doReturn(Future(billingProject))
      .when(service)
      .getBillingProjectContext(RawlsBillingProjectName(destWorkspaceRequest.namespace), requestContext)

    doReturn(Future()).when(service).requireCreateWorkspaceAction(billingProject.projectName, requestContext)

    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCloudPlatform()).thenReturn(CloudPlatform.AZURE)
    doReturn(Future(Some(billingProfile))).when(service).getBillingProfile(billingProject, requestContext)

    val destWorkspace = mock[Workspace]
    doReturn(Future(destWorkspace))
      .when(service)
      .cloneAzureWorkspaceAsync(
        ArgumentMatchers.eq(sourceWorkspace),
        ArgumentMatchers.eq(billingProfile),
        ArgumentMatchers.eq(destWorkspaceRequest),
        ArgumentMatchers.any()
      )

    whenReady(
      service.cloneMultiCloudWorkspaceAsync(
        mock[WorkspaceService],
        sourceWorkspace.toWorkspaceName,
        destWorkspaceRequest
      )
    )(
      _ shouldBe WorkspaceDetails.fromWorkspaceAndOptions(destWorkspace,
                                                          Some(Set.empty),
                                                          useAttributes = true,
                                                          Some(WorkspaceCloudPlatform.Azure)
      )
    )
  }

  it should "pass a request to clone a GCP workspace to cloneWorkspace in workspaceService" in {
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

    val requestContext = mock[RawlsRequestContext]
    when(requestContext.otelContext).thenReturn(None)
    val service = spy(
      MultiCloudWorkspaceService.constructor(
        mock[SlickDataSource],
        mock[WorkspaceManagerDAO],
        mock[BillingProfileManagerDAO],
        mock[SamDAO],
        mock[MultiCloudWorkspaceConfig],
        mock[LeonardoDAO],
        "MultiCloudWorkspaceService-test"
      )(requestContext)
    )

    val workspaceService = spy(
      WorkspaceService.constructor(
        dataSource = mock[SlickDataSource],
        methodRepoDAO = mock[MethodRepoDAO],
        cromiamDAO = mock[ExecutionServiceDAO],
        executionServiceCluster = mock[ExecutionServiceCluster],
        execServiceBatchSize = 10,
        workspaceManagerDAO = mock[WorkspaceManagerDAO],
        leonardoService = mock[LeonardoService],
        methodConfigResolver = mock[MethodConfigResolver],
        gcsDAO = mock[GoogleServicesDAO],
        samDAO = mock[SamDAO],
        notificationDAO = mock[NotificationDAO],
        userServiceConstructor = _ => mock[UserService],
        genomicsServiceConstructor = _ => mock[GenomicsService],
        maxActiveWorkflowsTotal = 1000,
        maxActiveWorkflowsPerUser = 100,
        workbenchMetricBaseName = "test",
        submissionCostService = mock[SubmissionCostService],
        config = mock[WorkspaceServiceConfig],
        requesterPaysSetupService = mock[RequesterPaysSetupService],
        entityManager = mock[EntityManager],
        resourceBufferService = mock[ResourceBufferService],
        servicePerimeterService = mock[ServicePerimeterService],
        googleIamDao = mock[GoogleIamDAO],
        terraBillingProjectOwnerRole = "terraBillingProjectOwnerRole",
        terraWorkspaceCanComputeRole = "terraWorkspaceCanComputeRole",
        terraWorkspaceNextflowRole = "terraWorkspaceNextflowRole",
        terraBucketReaderRole = "terraBucketReaderRole",
        terraBucketWriterRole = "terraBucketWriterRole",
        rawlsWorkspaceAclManager = mock[RawlsWorkspaceAclManager],
        multiCloudWorkspaceAclManager = mock[MultiCloudWorkspaceAclManager],
        fastPassServiceConstructor = (_, _) => mock[FastPassService]
      )(requestContext)
    )

    doReturn(Future(sourceWorkspace))
      .when(service)
      .getV2WorkspaceContextAndPermissions(sourceWorkspace.toWorkspaceName, SamWorkspaceActions.read, None)

    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceRequest.namespace),
      CreationStatuses.Ready,
      None,
      None
    )

    doReturn(Future(billingProject))
      .when(service)
      .getBillingProjectContext(RawlsBillingProjectName(destWorkspaceRequest.namespace), requestContext)

    doReturn(Future()).when(service).requireCreateWorkspaceAction(billingProject.projectName, requestContext)

    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCloudPlatform).thenReturn(CloudPlatform.GCP)
    doReturn(Future(Some(billingProfile))).when(service).getBillingProfile(billingProject, requestContext)

    val destWorkspace = mock[Workspace]
    doReturn(Future(destWorkspace))
      .when(workspaceService)
      .cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace),
        ArgumentMatchers.eq(billingProject),
        ArgumentMatchers.eq(destWorkspaceRequest),
        ArgumentMatchers.any()
      )

    whenReady(
      service.cloneMultiCloudWorkspaceAsync(
        workspaceService,
        sourceWorkspace.toWorkspaceName,
        destWorkspaceRequest
      )
    )(
      _ shouldBe WorkspaceDetails.fromWorkspaceAndOptions(destWorkspace,
                                                          authDomain,
                                                          useAttributes = true,
                                                          Some(WorkspaceCloudPlatform.Gcp)
      )
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

    val requestContext = mock[RawlsRequestContext]
    when(requestContext.otelContext).thenReturn(None)

    val workspaceManagerDAO = mock[WorkspaceManagerDAO]
    doAnswer(_ => throw new ApiException())
      .when(workspaceManagerDAO)
      .cloneWorkspace(any(), any(), any(), any(), any(), any(), any())

    val workspaceRepository = mock[WorkspaceRepository]

    val service = spy(
      new MultiCloudWorkspaceService(
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
    )

    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCreatedDate).thenReturn(DateTime.now().toString)

    val destWorkspace = mock[Workspace]

    doReturn(Future(destWorkspace))
      .when(workspaceRepository)
      .createNewMCWorkspaceRecord(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(WorkspaceState.Cloning)
      )(ArgumentMatchers.any())

    when(workspaceRepository.deleteWorkspaceRecord(destWorkspace)).thenReturn(Future(true))

    intercept[ApiException] {
      Await.result(
        service.cloneAzureWorkspaceAsync(sourceWorkspace, billingProfile, destWorkspaceRequest, requestContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository).deleteWorkspaceRecord(destWorkspace)
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

    val requestContext = mock[RawlsRequestContext]
    when(requestContext.otelContext).thenReturn(None)

    val workspaceRepository = mock[WorkspaceRepository]
    val service = spy(
      new MultiCloudWorkspaceService(
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
    )

    val billingProfile = mock[ProfileModel]
    when(billingProfile.getCreatedDate).thenReturn(DateTime.now().toString)

    val destWorkspace = mock[Workspace]
    doReturn(Future(new Exception()))
      .when(workspaceRepository)
      .createNewMCWorkspaceRecord(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest),
        ArgumentMatchers.eq(requestContext),
        ArgumentMatchers.eq(WorkspaceState.Cloning)
      )(ArgumentMatchers.any())

    intercept[Exception] {
      Await.result(
        service.cloneAzureWorkspaceAsync(sourceWorkspace, billingProfile, destWorkspaceRequest, requestContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository, never()).deleteWorkspaceRecord(destWorkspace)
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
    val requestContext = mock[RawlsRequestContext]
    when(requestContext.otelContext).thenReturn(None)
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
    val service = spy(
      new MultiCloudWorkspaceService(
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
    )
    val destWorkspace = mock[Workspace]
    doReturn(Future.successful(destWorkspace))
      .when(workspaceRepository)
      .createNewMCWorkspaceRecord(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(destWorkspaceRequest),
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
    val requestContext = mock[RawlsRequestContext]
    when(requestContext.otelContext).thenReturn(None)
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
    val service = spy(
      new MultiCloudWorkspaceService(
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
      .createNewMCWorkspaceRecord(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(mergedWorkspaceRequest),
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
