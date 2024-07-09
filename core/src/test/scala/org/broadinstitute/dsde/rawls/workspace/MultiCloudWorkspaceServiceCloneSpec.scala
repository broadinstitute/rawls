package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.{CloudPlatform, ProfileModel}
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{
  AccessScope,
  CloneControlledAzureStorageContainerResult,
  CloneWorkspaceResult,
  CloningInstructionsEnum,
  ControlledResourceMetadata,
  JobReport,
  ResourceDescription,
  ResourceList,
  ResourceMetadata
}
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeString,
  CreationStatuses,
  ErrorReport,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceActions,
  UserInfo,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspaceDetails,
  WorkspaceName,
  WorkspaceRequest,
  WorkspaceState,
  WorkspaceType
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, doReturn, spy, verify, when}
import org.scalatest.OptionValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.language.postfixOps

class MultiCloudWorkspaceServiceCloneSpec
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

  behavior of "cloneMultiCloudWorkspace"

  it should "fail if the user does not have access to the source workspace" in {
    val sourceWorkspace = defaultWorkspace
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(false))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = intercept[NoSuchWorkspaceException] {
      Await.result(
        service.cloneMultiCloudWorkspace(
          mock[WorkspaceService],
          sourceWorkspace.toWorkspaceName,
          WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
        ),
        Duration.Inf
      )
    }

    result.workspace shouldBe (sourceWorkspace.toWorkspaceName.toString)
  }

  it should "return forbidden if the user does not have the createWorkspace action for the billing project" in {
    val sourceWorkspace = defaultWorkspace
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(UUID.randomUUID().toString)
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(false))

    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneMultiCloudWorkspace(
          mock[WorkspaceService],
          sourceWorkspace.toWorkspaceName,
          WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
        ),
        Duration.Inf
      )
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.Forbidden)
  }

  it should "throw an exception if the billing profile id cannot be parsed into a UUID" in {
    val sourceWorkspace = defaultWorkspace
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))

    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some("not a uuid")
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))

    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(true))

    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneMultiCloudWorkspace(
          mock[WorkspaceService],
          sourceWorkspace.toWorkspaceName,
          WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
        ),
        Duration.Inf
      )
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    result.errorReport.message should include("Invalid billing profile id")
  }

  it should "call WorkspaceService to clone a rawls workspace with no billing profile" in {
    val sourceWorkspace = defaultWorkspace.copy(workspaceType = WorkspaceType.RawlsWorkspace)
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(true))

    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )
    val clonedWorkspace = mock[Workspace]
    val cloneRequest = WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
    val workspaceService = mock[WorkspaceService]
    when(workspaceService.cloneWorkspace(sourceWorkspace, billingProject, cloneRequest, testContext))
      .thenReturn(Future(clonedWorkspace))
    val result = Await.result(
      service.cloneMultiCloudWorkspace(
        workspaceService,
        sourceWorkspace.toWorkspaceName,
        cloneRequest
      ),
      Duration.Inf
    )

    result shouldBe WorkspaceDetails.fromWorkspaceAndOptions(clonedWorkspace,
                                                             Some(Set.empty),
                                                             useAttributes = true,
                                                             Some(WorkspaceCloudPlatform.Gcp)
    )
    verify(workspaceService).cloneWorkspace(sourceWorkspace, billingProject, cloneRequest, testContext)
  }

  it should "call WorkspaceService to clone a rawls workspace with a gcp billing profile" in {
    val sourceWorkspace = defaultWorkspace.copy(workspaceType = WorkspaceType.RawlsWorkspace)
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(CloudPlatform.GCP)
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(billingProfile.getId.toString)
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(bpmDAO.getBillingProfile(billingProfile.getId, testContext)).thenReturn(Some(billingProfile))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(true))
    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      bpmDAO,
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )
    val clonedWorkspace = mock[Workspace]
    val cloneRequest = WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
    val workspaceService = mock[WorkspaceService]
    when(workspaceService.cloneWorkspace(sourceWorkspace, billingProject, cloneRequest, testContext))
      .thenReturn(Future(clonedWorkspace))

    val result = Await.result(
      service.cloneMultiCloudWorkspace(
        workspaceService,
        sourceWorkspace.toWorkspaceName,
        cloneRequest
      ),
      Duration.Inf
    )

    result shouldBe WorkspaceDetails.fromWorkspaceAndOptions(clonedWorkspace,
                                                             Some(Set.empty),
                                                             useAttributes = true,
                                                             Some(WorkspaceCloudPlatform.Gcp)
    )
    verify(workspaceService).cloneWorkspace(sourceWorkspace, billingProject, cloneRequest, testContext)
  }

  it should "call cloneAzureWorkspace for an azure workspace and an azure billing project" in {
    val sourceWorkspace = defaultWorkspace
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(CloudPlatform.AZURE)
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(billingProfile.getId.toString)
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(true))
    val bpmDAO = mock[BillingProfileManagerDAO]
    when(bpmDAO.getBillingProfile(billingProfile.getId, testContext)).thenReturn(Some(billingProfile))
    val service = spy(
      new MultiCloudWorkspaceService(
        testContext,
        mock[WorkspaceManagerDAO],
        bpmDAO,
        samDAO,
        mock[MultiCloudWorkspaceConfig],
        mock[LeonardoDAO],
        "MultiCloudWorkspaceService-test",
        mock[WorkspaceManagerResourceMonitorRecordDao],
        workspaceRepository,
        billingRepository
      )
    )
    val request = WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
    val clonedWorkspace = mock[Workspace]
    doReturn(Future(clonedWorkspace))
      .when(service)
      .cloneAzureWorkspace(sourceWorkspace, billingProfile, request, testContext)

    val result = Await.result(
      service.cloneMultiCloudWorkspace(mock[WorkspaceService], sourceWorkspace.toWorkspaceName, request),
      Duration.Inf
    )

    result shouldBe WorkspaceDetails.fromWorkspaceAndOptions(clonedWorkspace,
                                                             Some(Set.empty),
                                                             useAttributes = true,
                                                             Some(WorkspaceCloudPlatform.Azure)
    )
    verify(service).cloneAzureWorkspace(sourceWorkspace, billingProfile, request, testContext)
  }

  it should "throw an exception for an azure workspace with no billing profile id" in {
    val sourceWorkspace = defaultWorkspace
    val destWorkspaceName = WorkspaceName("dest-namespace", "dest-name")
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getWorkspace(sourceWorkspace.toWorkspaceName, None))
      .thenReturn(Future(Some(sourceWorkspace)))
    val billingProject = RawlsBillingProject(
      RawlsBillingProjectName(destWorkspaceName.namespace),
      CreationStatuses.Ready,
      None,
      None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(RawlsBillingProjectName(destWorkspaceName.namespace)))
      .thenReturn(Future(Some(billingProject)))
    val samDAO = mock[SamDAO]
    when(samDAO.getUserStatus(testContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", enabled = true))))
    when(
      samDAO.userHasAction(
        SamResourceTypeNames.workspace,
        sourceWorkspace.workspaceId,
        SamWorkspaceActions.read,
        testContext
      )
    ).thenReturn(Future(true))

    when(
      samDAO.userHasAction(
        SamResourceTypeNames.billingProject,
        billingProject.projectName.value,
        SamBillingProjectActions.createWorkspace,
        testContext
      )
    ).thenReturn(Future.successful(true))

    val service = new MultiCloudWorkspaceService(
      testContext,
      mock[WorkspaceManagerDAO],
      mock[BillingProfileManagerDAO],
      samDAO,
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      billingRepository
    )

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneMultiCloudWorkspace(
          mock[WorkspaceService],
          sourceWorkspace.toWorkspaceName,
          WorkspaceRequest(destWorkspaceName.namespace, destWorkspaceName.name, Map.empty)
        ),
        Duration.Inf
      )
    }

    result.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
    result.errorReport.message should include("Cloud platform mismatch")
  }

  behavior of "cloneAzureWorkspace"

  // happy path
  // correct parameters for external calls are verified in separate tests; this one just cares everything gets called
  it should "create clone the workspace with all the required resources" in {
    // since this is already the happy path, test that attributes are merged
    val sourceWorkspace = defaultWorkspace.copy(attributes =
      Map(
        AttributeName.withDefaultNS("sourceAttributeNamespace") -> AttributeString("source"),
        AttributeName.withDefaultNS("overriddenAttributeNamespace") -> AttributeString("source")
      )
    )
    val workspaceRequest = WorkspaceRequest(
      "dest-namespace",
      "dest-name",
      Map(
        AttributeName.withDefaultNS("destAttributeNamespace") -> AttributeString("dest"),
        AttributeName.withDefaultNS("overriddenAttributeNamespace") -> AttributeString("dest")
      )
    )
    val expectedMergedAttributes = Map(
      AttributeName.withDefaultNS("sourceAttributeNamespace") -> AttributeString("source"),
      AttributeName.withDefaultNS("destAttributeNamespace") -> AttributeString("dest"),
      AttributeName.withDefaultNS("overriddenAttributeNamespace") -> AttributeString("dest")
    )

    val billingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
      .createdDate("2023-09-12T22:20:48.949Z")
    val workspaceRepository = mock[WorkspaceRepository]
    val destWorkspace = mock[Workspace]
    when(workspaceRepository.createMCWorkspace(any, any, ArgumentMatchers.eq(expectedMergedAttributes), any, any)(any))
      .thenReturn(Future(destWorkspace))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val wsmCloneJobId = UUID.randomUUID().toString
    val cloneJob = new CloneWorkspaceResult().jobReport(new JobReport().id(wsmCloneJobId).status(StatusEnum.SUCCEEDED))
    when(wsmDAO.cloneWorkspace(any, any, any, any, any, any, any)).thenReturn(cloneJob)
    when(wsmDAO.getCloneWorkspaceResult(any, any, any)).thenReturn(cloneJob)
    when(wsmDAO.enumerateStorageContainers(any, any, any, any)).thenReturn(
      new ResourceList().addResourcesItem(
        new ResourceDescription().metadata(
          new ResourceMetadata()
            .resourceId(UUID.randomUUID())
            .name(MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspace.workspaceIdAsUUID))
            .description("correct name, correct access")
            .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
        )
      )
    )
    val cloneResult = new CloneControlledAzureStorageContainerResult()
      .jobReport(new JobReport().id(UUID.randomUUID().toString))
    when(wsmDAO.cloneAzureStorageContainer(any, any, any, any, any, any, any)).thenReturn(cloneResult)
    val leonardoDAO = mock[LeonardoDAO]
    doNothing().when(leonardoDAO).createWDSInstance(any, any, any)
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(monitorRecordDao.create(any)).thenReturn(Future())

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      monitorRecordDao,
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = Await.result(
      service.cloneAzureWorkspace(sourceWorkspace, billingProfile, workspaceRequest, testContext),
      Duration.Inf
    )

    result shouldBe destWorkspace
    verify(workspaceRepository)
      .createMCWorkspace(any, any, ArgumentMatchers.eq(expectedMergedAttributes), any, any)(any)
    verify(wsmDAO).getCloneWorkspaceResult(any(), ArgumentMatchers.eq(wsmCloneJobId), ArgumentMatchers.eq(testContext))
    verify(wsmDAO).cloneWorkspace(
      ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
      any(),
      ArgumentMatchers.eq(workspaceRequest.name),
      any(),
      ArgumentMatchers.eq(workspaceRequest.namespace),
      ArgumentMatchers.eq(testContext),
      any()
    )
    // verify clone container
    verify(wsmDAO).cloneAzureStorageContainer(any, any, any, any, any, any, any)
    verify(leonardoDAO).createWDSInstance(any, any, any)
    verify(monitorRecordDao).create(any)
  }

  it should "not fail on a WDS creation failure" in {
    val workspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
    val billingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
    val workspaceRepository = mock[WorkspaceRepository]
    val destWorkspace = mock[Workspace]
    when(workspaceRepository.createMCWorkspace(any, any, any, any, any)(any)).thenReturn(Future(destWorkspace))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val sourceWorkspace = defaultWorkspace
    val wsmCloneJobId = UUID.randomUUID().toString
    val cloneJob = new CloneWorkspaceResult().jobReport(new JobReport().id(wsmCloneJobId).status(StatusEnum.SUCCEEDED))
    when(wsmDAO.cloneWorkspace(any, any, any, any, any, any, any)).thenReturn(cloneJob)
    when(wsmDAO.getCloneWorkspaceResult(any, any, any)).thenReturn(cloneJob)
    when(wsmDAO.enumerateStorageContainers(any, any, any, any)).thenReturn(
      new ResourceList().addResourcesItem(
        new ResourceDescription().metadata(
          new ResourceMetadata()
            .resourceId(UUID.randomUUID())
            .name(MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspace.workspaceIdAsUUID))
            .description("correct name, correct access")
            .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
        )
      )
    )
    val cloneResult = new CloneControlledAzureStorageContainerResult()
      .jobReport(new JobReport().id(UUID.randomUUID().toString))
    when(wsmDAO.cloneAzureStorageContainer(any, any, any, any, any, any, any)).thenReturn(cloneResult)
    val leonardoDAO = mock[LeonardoDAO]
    when(leonardoDAO.createWDSInstance(any, any, any)).thenAnswer(_ => throw new Exception("WDS didn't start"))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(monitorRecordDao.create(any)).thenReturn(Future())

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      leonardoDAO,
      "MultiCloudWorkspaceService-test",
      monitorRecordDao,
      workspaceRepository,
      mock[BillingRepository]
    )

    val result = Await.result(
      service.cloneAzureWorkspace(sourceWorkspace, billingProfile, workspaceRequest, testContext),
      Duration.Inf
    )

    result shouldBe destWorkspace
    verify(wsmDAO).getCloneWorkspaceResult(any(), ArgumentMatchers.eq(wsmCloneJobId), ArgumentMatchers.eq(testContext))
    verify(wsmDAO).cloneWorkspace(
      ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
      any(),
      ArgumentMatchers.eq(workspaceRequest.name),
      any(),
      ArgumentMatchers.eq(workspaceRequest.namespace),
      ArgumentMatchers.eq(testContext),
      any()
    )
    // verify clone container
    verify(wsmDAO).cloneAzureStorageContainer(any, any, any, any, any, any, any)
    verify(leonardoDAO).createWDSInstance(any, any, any)
    verify(monitorRecordDao).create(any)
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

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneAzureWorkspace(
          defaultWorkspace,
          new ProfileModel().createdDate("2023-09-11T22:20:48.949Z"),
          WorkspaceRequest("dest-namespace", "dest-name", Map.empty),
          testContext
        ),
        Duration.Inf
      )
    }

    result.errorReport.statusCode.value shouldBe StatusCodes.Forbidden
  }

  it should "fail if the destination workspace already exists" in {
    // This is covered in the tests for the workspace repository
  }

  it should "clean up the dest workspace record if the request to Workspace Manager fails" in {
    val workspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
    val billingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .tenantId(UUID.randomUUID())
      .subscriptionId(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
      .managedResourceGroupId("fake-mrg")
      .createdDate("2023-09-12T22:20:48.949Z")
    val workspaceRepository = mock[WorkspaceRepository]
    val destWorkspace = mock[Workspace]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(workspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(destWorkspace))
    when(workspaceRepository.deleteWorkspace(destWorkspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val sourceWorkspace = defaultWorkspace
    when(
      wsmDAO.cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
        any(),
        ArgumentMatchers.eq(workspaceRequest.name),
        any(),
        ArgumentMatchers.eq(workspaceRequest.namespace),
        ArgumentMatchers.eq(testContext),
        any()
      )
    ).thenAnswer(_ => throw RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.ImATeapot, "short and stout")))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      mock[MultiCloudWorkspaceConfig],
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneAzureWorkspace(sourceWorkspace, billingProfile, workspaceRequest, testContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository).deleteWorkspace(destWorkspace)
  }

  it should "clean up the dest workspace record if the Workspace Manager job fails asynchronously" in {
    val workspaceRequest = WorkspaceRequest("dest-namespace", "dest-name", Map.empty)
    val billingProfile = new ProfileModel()
      .id(UUID.randomUUID())
      .tenantId(UUID.randomUUID())
      .subscriptionId(UUID.randomUUID())
      .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
      .managedResourceGroupId("fake-mrg")
      .createdDate("2023-09-12T22:20:48.949Z")
    val workspaceRepository = mock[WorkspaceRepository]
    val destWorkspace = mock[Workspace]
    when(
      workspaceRepository.createMCWorkspace(
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(workspaceRequest.toWorkspaceName),
        ArgumentMatchers.any(),
        ArgumentMatchers.eq(testContext),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future(destWorkspace))
    when(workspaceRepository.deleteWorkspace(destWorkspace)).thenReturn(Future(true))
    val wsmDAO = mock[WorkspaceManagerDAO]
    val sourceWorkspace = defaultWorkspace
    val jobId = UUID.randomUUID().toString
    val cloneJob = new CloneWorkspaceResult().jobReport(new JobReport().id(jobId).status(StatusEnum.FAILED))
    when(
      wsmDAO.cloneWorkspace(
        ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
        any(),
        ArgumentMatchers.eq(workspaceRequest.name),
        any(),
        ArgumentMatchers.eq(workspaceRequest.namespace),
        ArgumentMatchers.eq(testContext),
        any()
      )
    ).thenReturn(cloneJob)
    when(wsmDAO.getCloneWorkspaceResult(any(), ArgumentMatchers.eq(jobId), ArgumentMatchers.eq(testContext)))
      .thenReturn(cloneJob)

    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      workspaceRepository,
      mock[BillingRepository]
    )

    intercept[WorkspaceManagerOperationFailureException] {
      Await.result(
        service.cloneAzureWorkspace(sourceWorkspace, billingProfile, workspaceRequest, testContext),
        Duration.Inf
      )
    }

    verify(workspaceRepository).deleteWorkspace(destWorkspace)
    verify(wsmDAO).getCloneWorkspaceResult(any(), ArgumentMatchers.eq(jobId), ArgumentMatchers.eq(testContext))
    verify(wsmDAO).cloneWorkspace(
      ArgumentMatchers.eq(sourceWorkspace.workspaceIdAsUUID),
      any(),
      ArgumentMatchers.eq(workspaceRequest.name),
      any(),
      ArgumentMatchers.eq(workspaceRequest.namespace),
      ArgumentMatchers.eq(testContext),
      any()
    )
  }

  behavior of "cloneWorkspaceStorageContainer"

  it should "clone the matching storage container into the new workspace" in {
    val sourceWorkspaceId = UUID.randomUUID()
    val destWorkspaceId = UUID.randomUUID()
    val wsmDAO = mock[WorkspaceManagerDAO]
    val sourceStorageContainerId = UUID.randomUUID()
    when(wsmDAO.enumerateStorageContainers(ArgumentMatchers.eq(sourceWorkspaceId), any(), any(), any()))
      .thenReturn(
        new ResourceList()
          .addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name("not the correct name")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
          .addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name(MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId))
                .description("correct name, but wrong access")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.PRIVATE_ACCESS))
            )
          )
          .addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(sourceStorageContainerId)
                .name(MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId))
                .description("correct name, correct access")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
      )
    val destContainerName = MultiCloudWorkspaceService.getStorageContainerName(destWorkspaceId)
    val cloneResult = new CloneControlledAzureStorageContainerResult()
      .jobReport(new JobReport().id(UUID.randomUUID().toString))
    when(
      wsmDAO.cloneAzureStorageContainer(
        sourceWorkspaceId,
        destWorkspaceId,
        sourceStorageContainerId,
        destContainerName,
        CloningInstructionsEnum.RESOURCE,
        None,
        testContext
      )
    ).thenReturn(cloneResult)
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    val result = Await.result(
      service.cloneWorkspaceStorageContainer(sourceWorkspaceId, destWorkspaceId, None, testContext),
      Duration.Inf
    )

    result shouldBe cloneResult
  }

  it should "throw an exception if the workspace has no storage containers" in {
    val sourceWorkspaceId = UUID.randomUUID()
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.enumerateStorageContainers(ArgumentMatchers.eq(sourceWorkspaceId), any(), any(), any()))
      .thenReturn(new ResourceList().resources(List().asJava))
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneWorkspaceStorageContainer(sourceWorkspaceId, UUID.randomUUID(), None, testContext),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(wsmDAO).enumerateStorageContainers(ArgumentMatchers.eq(sourceWorkspaceId), any(), any(), any())
  }

  it should "throw an exception if there is no storage container with the correct name and shared access" in {
    val sourceWorkspaceId = UUID.randomUUID()
    val wsmDAO = mock[WorkspaceManagerDAO]
    when(wsmDAO.enumerateStorageContainers(ArgumentMatchers.eq(sourceWorkspaceId), any(), any(), any()))
      .thenReturn(
        new ResourceList()
          .addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name("not the correct name")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.SHARED_ACCESS))
            )
          )
          .addResourcesItem(
            new ResourceDescription().metadata(
              new ResourceMetadata()
                .resourceId(UUID.randomUUID())
                .name(MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId))
                .description("correct name, but wrong access")
                .controlledResourceMetadata(new ControlledResourceMetadata().accessScope(AccessScope.PRIVATE_ACCESS))
            )
          )
      )
    val service = new MultiCloudWorkspaceService(
      testContext,
      wsmDAO,
      mock[BillingProfileManagerDAO],
      mock[SamDAO],
      MultiCloudWorkspaceConfig(MultiCloudWorkspaceManagerConfig("app", 1 seconds, 1 seconds), mock[AzureConfig]),
      mock[LeonardoDAO],
      "MultiCloudWorkspaceService-test",
      mock[WorkspaceManagerResourceMonitorRecordDao],
      mock[WorkspaceRepository],
      mock[BillingRepository]
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        service.cloneWorkspaceStorageContainer(sourceWorkspaceId, UUID.randomUUID(), None, testContext),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Some(StatusCodes.InternalServerError)
    verify(wsmDAO).enumerateStorageContainers(ArgumentMatchers.eq(sourceWorkspaceId), any(), any(), any())
  }

}
