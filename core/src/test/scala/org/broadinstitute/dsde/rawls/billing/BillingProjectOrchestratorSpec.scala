package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.BpmBillingProjectDelete
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ErrorReport,
  ProjectAccessUpdate,
  ProjectRoles,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamBillingProjectActions,
  SamBillingProjectPolicyNames,
  SamCreateResourceResponse,
  SamResourceTypeNames,
  UserInfo
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.SQLSyntaxErrorException
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingProjectOrchestratorSpec extends AnyFlatSpec {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val azConfig: AzureConfig = AzureConfig(
    "fake-landing-zone-definition",
    "fake-landing-zone-version",
    None,
    Map("fake_parameter" -> "fake_value")
  )

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)

  behavior of "creation request validation"

  it should "fail when the billing project fails validation" in {
    val samDAO = mock[SamDAO]
    val billingRepository = mock[BillingRepository]
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("!@B#$"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val gbp = mock[BillingProjectLifecycle]
    when(gbp.validateBillingProjectCreationRequest(createRequest, testContext))
      .thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "failed"))))
    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      mock[NotificationDAO],
      billingRepository,
      gbp,
      mock[BpmBillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  behavior of "billing project creation"

  it should "create a billing project record when provided a valid request and set the correct creation status" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val bpCreator = mock[BillingProjectLifecycle]
    val bpCreatorReturnedStatus = CreationStatuses.CreatingLandingZone
    val multiCloudWorkspaceConfig = new MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    when(bpCreator.validateBillingProjectCreationRequest(createRequest, testContext)).thenReturn(Future.successful())
    when(bpCreator.postCreationSteps(createRequest, multiCloudWorkspaceConfig, testContext))
      .thenReturn(Future.successful(bpCreatorReturnedStatus))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName)))
      .thenReturn(Future.successful(None))
    when(billingRepository.createBillingProject(any[RawlsBillingProject])).thenReturn(
      Future.successful(
        RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value),
                            CreationStatuses.Creating,
                            None,
                            None
        )
      )
    )
    when(
      billingRepository.updateCreationStatus(ArgumentMatchers.eq(createRequest.projectName),
                                             ArgumentMatchers.eq(bpCreatorReturnedStatus),
                                             any()
      )
    ).thenReturn(Future.successful(1))
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.buildBillingProjectPolicies(Set.empty, testContext)),
        ArgumentMatchers.eq(Set.empty),
        any[RawlsRequestContext],
        ArgumentMatchers.eq(None)
      )
    ).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))
    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      mock[NotificationDAO],
      billingRepository,
      bpCreator,
      mock[BillingProjectLifecycle],
      multiCloudWorkspaceConfig,
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)

    verify(billingRepository, Mockito.times(1)).updateCreationStatus(ArgumentMatchers.eq(createRequest.projectName),
                                                                     ArgumentMatchers.eq(bpCreatorReturnedStatus),
                                                                     ArgumentMatchers.eq(None)
    )
  }

  it should "fail when a duplicate project already exists" in {
    val samDAO = mock[SamDAO]
    val gcsDAO = mock[GoogleServicesDAO]
    when(gcsDAO.testBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getBillingProject(ArgumentMatchers.eq(createRequest.projectName))).thenReturn(
      Future.successful(Some(RawlsBillingProject(RawlsBillingProjectName("fake"), CreationStatuses.Ready, None, None)))
    )
    val bpCreator = mock[BillingProjectLifecycle]
    when(bpCreator.validateBillingProjectCreationRequest(createRequest, testContext)).thenReturn(Future.successful())

    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      mock[NotificationDAO],
      billingRepository,
      bpCreator,
      mock[BillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[DuplicateBillingProjectException] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Conflict)) {
      ex.errorReport.statusCode
    }
  }

  it should "fail when provided an invalid billing project name" in {
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("!@B#$"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val bpo = new BillingProjectOrchestrator(
      testContext,
      mock[SamDAO],
      mock[NotificationDAO],
      mock[BillingRepository],
      mock[BillingProjectLifecycle],
      mock[BillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest)) {
      ex.errorReport.statusCode
    }
  }

  it should "delete the billing project and throw an exception if post creation steps fail" in {
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val creator = mock[BillingProjectLifecycle]
    val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    when(
      creator.validateBillingProjectCreationRequest(ArgumentMatchers.eq(createRequest),
                                                    ArgumentMatchers.eq(testContext)
      )
    ).thenReturn(Future.successful())
    when(creator.postCreationSteps(createRequest, multiCloudWorkspaceConfig, testContext))
      .thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, "Failed"))))
    when(creator.unregisterBillingProject(createRequest.projectName, testContext)).thenReturn(Future.successful())
    val repo = mock[BillingRepository]
    when(repo.getBillingProject(ArgumentMatchers.eq(createRequest.projectName)))
      .thenReturn(Future.successful(None))
    when(repo.createBillingProject(any[RawlsBillingProject])).thenReturn(
      Future.successful(
        RawlsBillingProject(RawlsBillingProjectName(createRequest.projectName.value),
                            CreationStatuses.Ready,
                            None,
                            None
        )
      )
    )
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(BillingProjectOrchestrator.buildBillingProjectPolicies(Set.empty, testContext)),
        ArgumentMatchers.eq(Set.empty),
        any[RawlsRequestContext],
        ArgumentMatchers.eq(None)
      )
    ).thenReturn(Future.successful(SamCreateResourceResponse("test", "test", Set.empty, Set.empty)))
    when(
      samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail(userInfo.userEmail.value) -> Seq())))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      mock[NotificationDAO],
      repo,
      creator,
      mock[BillingProjectLifecycle],
      multiCloudWorkspaceConfig,
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadGateway)) {
      ex.errorReport.statusCode
    }
    verify(creator).unregisterBillingProject(createRequest.projectName, testContext)
  }

  behavior of "billing project deletion"

  // happy path resources

  def alwaysGiveAccessSamDao: SamDAO = {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAO.userHasAction(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(true))
    samDAO
  }

  def initiateDeleteLifecycle(returnValue: Future[Option[UUID]]): BillingProjectLifecycle = {
    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.deleteJobType).thenReturn(BpmBillingProjectDelete)
    when(billingProjectLifecycle.initiateDelete(ArgumentMatchers.any(), ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(returnValue)
    billingProjectLifecycle
  }

  def happyBillingRepository(profileId: Option[String]): BillingRepository = {
    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful())
    when(billingRepository.getBillingProfileId(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful(profileId))
    when(billingRepository.updateCreationStatus(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Future.successful(1))
    when(billingRepository.getCreationStatus(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful(CreationStatuses.Ready))
    billingRepository
  }

  def happyMonitorRecordDao: WorkspaceManagerResourceMonitorRecordDao = {
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(ArgumentMatchers.any())).thenReturn(Future.successful())
    monitorRecordDao
  }

  it should "fail when the user does not have deletion permission" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    when(
      samDAO.userHasAction(SamResourceTypeNames.billingProject,
                           billingProjectName.value,
                           SamBillingProjectActions.deleteBillingProject,
                           testContext
      )
    ).thenReturn(Future.successful(false))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      samDAO,
      mock[NotificationDAO],
      mock[BillingRepository],
      mock[BillingProjectLifecycle],
      mock[BillingProjectLifecycle],
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    }

    assertResult(Some(StatusCodes.Forbidden))(ex.errorReport.statusCode)

    verify(samDAO).userHasAction(SamResourceTypeNames.billingProject,
                                 billingProjectName.value,
                                 SamBillingProjectActions.deleteBillingProject,
                                 testContext
    )
  }

  it should "fail when workspaces attached to the billing project exist" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(billingProjectName)(executionContext))
      .thenReturn(
        Future.failed(
          new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, "Project cannot be deleted because it contains workspaces.")
          )
        )
      )
    // Mock Google project
    when(billingRepository.getBillingProfileId(billingProjectName)(executionContext))
      .thenReturn(Future.successful(None))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingRepository,
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS),
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS),
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest))(ex.errorReport.statusCode)
    verify(billingRepository, Mockito.times(1)).failUnlessHasNoWorkspaces(billingProjectName)(executionContext)
  }

  it should "fail if initiateDelete throws an exception " in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")

    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(billingProjectName)(executionContext))
      .thenReturn(Future.successful())
    when(billingRepository.getBillingProfileId(billingProjectName)(executionContext))
      .thenReturn(Future.successful(Some("fake-id")))
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Ready))
    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.initiateDelete(billingProjectName, testContext)).thenReturn(
      Future.failed(new SQLSyntaxErrorException("failed"))
    )

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingRepository,
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS),
      billingProjectLifecycle,
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[SQLSyntaxErrorException] {
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    }

    verify(billingProjectLifecycle).initiateDelete(billingProjectName, testContext)
    verify(billingRepository, never()).deleteBillingProject(billingProjectName)
  }

  it should "call initiateDelete and finializeDelete for the google lifecycle for a google project" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.initiateDelete(billingProjectName, testContext)).thenReturn(Future.successful(None))
    when(billingProjectLifecycle.finalizeDelete(billingProjectName, testContext)).thenReturn(Future.successful())
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      happyBillingRepository(None),
      billingProjectLifecycle, // google
      mock[BillingProjectLifecycle], // bpm
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao] // nothing mocked - will fail if called
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(billingProjectLifecycle).initiateDelete(billingProjectName, testContext)
    verify(billingProjectLifecycle).finalizeDelete(billingProjectName, testContext)
  }

  it should "call initiateDelete and finializeDelete when the BPM lifecyle returns a jobId of None" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.initiateDelete(billingProjectName, testContext)).thenReturn(Future.successful(None))
    when(billingProjectLifecycle.finalizeDelete(billingProjectName, testContext)).thenReturn(Future.successful())
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      happyBillingRepository(Some(UUID.randomUUID().toString)),
      mock[BillingProjectLifecycle], // google
      billingProjectLifecycle, // bpm
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao] // nothing mocked - will fail if called
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(billingProjectLifecycle).initiateDelete(billingProjectName, testContext)
    verify(billingProjectLifecycle).finalizeDelete(billingProjectName, testContext)
  }

  it should "call the BPM lifecycle to initiate delete of an Azure project" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val jobId = UUID.fromString("c1024c05-40a6-4a12-b12e-028e445aec3b")

    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.initiateDelete(billingProjectName, testContext))
      .thenReturn(Future.successful(Some(jobId)))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      happyBillingRepository(Some("inconsequential_id")),
      mock[BillingProjectLifecycle], // google
      billingProjectLifecycle, // bpm
      mock[MultiCloudWorkspaceConfig],
      happyMonitorRecordDao
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(billingProjectLifecycle).initiateDelete(billingProjectName, testContext)
  }

  it should "create a job to delete the Azure project after calling initiateDelete" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val jobId = UUID.randomUUID()

    def matchedExpectedEvent(e: WorkspaceManagerResourceMonitorRecord) =
      e.jobControlId.toString == jobId.toString &&
        e.billingProjectId.get == billingProjectName.value &&
        e.userEmail.get == testContext.userInfo.userEmail.value &&
        e.jobType == BpmBillingProjectDelete
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(ArgumentMatchers.argThat(matchedExpectedEvent))).thenReturn(Future.successful())
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      happyBillingRepository(Some("inconsequential_id")),
      mock[BillingProjectLifecycle], // google
      initiateDeleteLifecycle(Future.successful(Some(jobId))), // bpm
      mock[MultiCloudWorkspaceConfig],
      monitorRecordDao
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(monitorRecordDao).create(ArgumentMatchers.argThat(matchedExpectedEvent))
  }

  it should "not create a job to delete the Azure project after calling initiateDelete fails" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      happyBillingRepository(Some("inconsequential_id")),
      mock[BillingProjectLifecycle], // google
      initiateDeleteLifecycle(Future.failed(new Exception)), // bpm
      mock[MultiCloudWorkspaceConfig],
      monitorRecordDao
    )

    intercept[Exception](Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf))

    verify(monitorRecordDao, never).create(ArgumentMatchers.any())
  }

  it should "set the status of the billing project to Deleting after successful delete initializing" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val jobId = UUID.randomUUID()
    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(billingProjectName)(executionContext))
      .thenReturn(Future.successful())
    when(billingRepository.getBillingProfileId(billingProjectName)(executionContext))
      .thenReturn(Future.successful(Some("inconsequential_id")))
    when(billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Deleting, None))
      .thenReturn(Future.successful(1))
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Ready))
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingRepository,
      mock[BillingProjectLifecycle], // google
      initiateDeleteLifecycle(Future.successful(Some(jobId))), // bpm
      mock[MultiCloudWorkspaceConfig],
      happyMonitorRecordDao
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(billingRepository).updateCreationStatus(billingProjectName, CreationStatuses.Deleting, None)
  }

  it should "fail when the status of the billing project is not in a terminal state" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val jobId = UUID.randomUUID()
    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(billingProjectName)(executionContext))
      .thenReturn(Future.successful())
    when(billingRepository.getBillingProfileId(billingProjectName)(executionContext))
      .thenReturn(Future.successful(Some("inconsequential_id")))
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Deleting))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingRepository,
      mock[BillingProjectLifecycle], // google
      initiateDeleteLifecycle(Future.successful(Some(jobId))), // bpm
      mock[MultiCloudWorkspaceConfig],
      happyMonitorRecordDao
    )

    intercept[BillingProjectDeletionException](
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    )
  }

  behavior of "buildBillingProjectPolicies"

  it should "build billing project policies that always include the creator as an owner" in {
    val user1Email = "user1@foo.bar"
    val user2Email = "user2@foo.bar"
    val membersToAdd =
      Set(ProjectAccessUpdate(user1Email, ProjectRoles.Owner), ProjectAccessUpdate(user2Email, ProjectRoles.User))

    val resultingPolicies = BillingProjectOrchestrator.buildBillingProjectPolicies(membersToAdd, testContext)

    // Validate owner policy
    assert(
      resultingPolicies(SamBillingProjectPolicyNames.owner).memberEmails
        .contains(WorkbenchEmail(userInfo.userEmail.value))
    )
    assert(resultingPolicies(SamBillingProjectPolicyNames.owner).memberEmails.contains(WorkbenchEmail(user1Email)))
    assert(resultingPolicies(SamBillingProjectPolicyNames.owner).memberEmails.size == 2)

    // Validate user (workspaceCreator) policy
    assert(
      resultingPolicies(SamBillingProjectPolicyNames.workspaceCreator).memberEmails.contains(WorkbenchEmail(user2Email))
    )
    assert(resultingPolicies(SamBillingProjectPolicyNames.workspaceCreator).memberEmails.size == 1)
  }

}
