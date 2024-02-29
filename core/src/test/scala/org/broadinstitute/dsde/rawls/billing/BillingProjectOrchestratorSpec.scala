package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.ProfileModel
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
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
    "fake-protected-landing-zone-definition",
    "fake-landing-zone-version",
    Map("fake_parameter" -> "fake_value"),
    Map("fake_parameter" -> "fake_value"),
    landingZoneAllowAttach = false
  )

  val billingProfileId = UUID.randomUUID()
  val azureBillingProfile = new ProfileModel()
    .id(billingProfileId)
    .tenantId(UUID.randomUUID())
    .subscriptionId(UUID.randomUUID())
    .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
    .managedResourceGroupId("fake-mrg")
    .createdDate("2023-09-12T22:20:48.949Z")

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)
  val multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    MultiCloudWorkspaceManagerConfig("fake_app_id", Duration(1, "second"), Duration(1, "second")),
    azConfig
  )

  behavior of "creation request validation"

  it should "fail when the billing project fails validation" in {
    val samDAO = mock[SamDAO]
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
      mock[BillingProfileManagerDAO],
      mock[BillingRepository],
      gbp,
      mock[AzureBillingProjectLifecycle],
      mock[BillingProjectDeletion],
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
    when(gcsDAO.testTerraAndUserBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(true))
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_project_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )
    val billingProjectDeletion = mock[BillingProjectDeletion]

    val bpCreator = mock[BillingProjectLifecycle]
    val bpCreatorReturnedStatus = CreationStatuses.CreatingLandingZone

    when(bpCreator.validateBillingProjectCreationRequest(createRequest, testContext)).thenReturn(Future.successful())
    when(bpCreator.postCreationSteps(createRequest, multiCloudWorkspaceConfig, billingProjectDeletion, testContext))
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
      mock[BillingProfileManagerDAO],
      billingRepository,
      bpCreator,
      mock[BillingProjectLifecycle],
      billingProjectDeletion,
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
    when(gcsDAO.testTerraAndUserBillingAccountAccess(any[RawlsBillingAccountName], ArgumentMatchers.eq(userInfo)))
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
      mock[BillingProfileManagerDAO],
      billingRepository,
      bpCreator,
      mock[BillingProjectLifecycle],
      mock[BillingProjectDeletion],
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
      mock[BillingProfileManagerDAO],
      mock[BillingRepository],
      mock[BillingProjectLifecycle],
      mock[BillingProjectLifecycle],
      mock[BillingProjectDeletion],
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
    val billingProjectDeletion = mock[BillingProjectDeletion]
    when(billingProjectDeletion.unregisterBillingProject(createRequest.projectName, testContext))
      .thenReturn(Future.successful())

    val creator = mock[BillingProjectLifecycle]
    when(
      creator.validateBillingProjectCreationRequest(ArgumentMatchers.eq(createRequest),
                                                    ArgumentMatchers.eq(testContext)
      )
    ).thenReturn(Future.successful())
    when(creator.postCreationSteps(createRequest, multiCloudWorkspaceConfig, billingProjectDeletion, testContext))
      .thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, "Failed"))))

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
      mock[BillingProfileManagerDAO],
      repo,
      creator,
      mock[BillingProjectLifecycle],
      billingProjectDeletion,
      multiCloudWorkspaceConfig,
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.createBillingProjectV2(createRequest), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadGateway)) {
      ex.errorReport.statusCode
    }
    verify(billingProjectDeletion).unregisterBillingProject(createRequest.projectName, testContext)
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
    when(
      billingProjectLifecycle.maybeCleanupResources(ArgumentMatchers.any(),
                                                    ArgumentMatchers.eq(false),
                                                    ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    )
      .thenReturn(returnValue)
    billingProjectLifecycle
  }

  def happyBillingRepository(billingProfile: Option[ProfileModel],
                             bpmDAO: BillingProfileManagerDAO
  ): BillingRepository = {
    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful())
    when(billingRepository.updateCreationStatus(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Future.successful(1))
    when(billingRepository.getCreationStatus(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful(CreationStatuses.Ready))
    billingProfile match {
      case Some(profile) =>
        val profileId = profile.getId
        when(billingRepository.getBillingProfileId(ArgumentMatchers.any())(ArgumentMatchers.any()))
          .thenReturn(Future.successful(Some(profileId.toString)))
        when(bpmDAO.getBillingProfile(profileId, testContext)).thenReturn(Some(profile))
      case None =>
        when(billingRepository.getBillingProfileId(ArgumentMatchers.any())(ArgumentMatchers.any()))
          .thenReturn(Future.successful(None))
    }
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
      mock[BillingProfileManagerDAO],
      mock[BillingRepository],
      mock[BillingProjectLifecycle],
      mock[BillingProjectLifecycle],
      mock[BillingProjectDeletion],
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
    // Mock Google project with no billing profile
    when(billingRepository.getBillingProfileId(billingProjectName)(executionContext))
      .thenReturn(Future.successful(None))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      mock[BillingProfileManagerDAO],
      billingRepository,
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS),
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS),
      mock[BillingProjectDeletion],
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val ex = intercept[RawlsExceptionWithErrorReport] {
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    }

    assertResult(Some(StatusCodes.BadRequest))(ex.errorReport.statusCode)
    verify(billingRepository, Mockito.times(1)).failUnlessHasNoWorkspaces(billingProjectName)(executionContext)
  }

  it should "fail if maybeCleanupResources throws an exception for an Azure billing project " in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val bpmDAO = mock[BillingProfileManagerDAO]
    // Mock billing project having a billing profile.
    when(bpmDAO.getBillingProfile(billingProfileId, testContext)).thenReturn(Some(azureBillingProfile))

    val billingRepository = mock[BillingRepository]
    // Mock billing project having a billing profile.
    when(billingRepository.getBillingProfileId(billingProjectName)(executionContext))
      .thenReturn(Future.successful(Some(billingProfileId.toString)))
    when(billingRepository.failUnlessHasNoWorkspaces(billingProjectName)(executionContext))
      .thenReturn(Future.successful())
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Ready))
    val azureBillingProjectLifecycle = mock[BillingProjectLifecycle]
    when(
      azureBillingProjectLifecycle.maybeCleanupResources(billingProjectName, maybeGoogleProject = false, testContext)
    ).thenReturn(
      Future.failed(new SQLSyntaxErrorException("failed"))
    )

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      bpmDAO,
      billingRepository,
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS),
      azureBillingProjectLifecycle,
      mock[BillingProjectDeletion],
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[SQLSyntaxErrorException] {
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    }

    verify(azureBillingProjectLifecycle).maybeCleanupResources(billingProjectName,
                                                               maybeGoogleProject = false,
                                                               testContext
    )
    verify(billingRepository, never()).deleteBillingProject(billingProjectName)
  }

  it should "call maybeCleanupResources and finalizeDelete for a google project/lifecycle" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.maybeCleanupResources(billingProjectName, maybeGoogleProject = true, testContext))
      .thenReturn(Future.successful(None))

    val billingProjectDeletion = mock[BillingProjectDeletion]
    when(billingProjectDeletion.finalizeDelete(billingProjectName, testContext, billingProfileExpected = false))
      .thenReturn(Future.successful())

    val bpmDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      bpmDAO,
      happyBillingRepository(None, bpmDAO),
      billingProjectLifecycle, // google
      billingProjectLifecycle, // azure, but maybeCleanupResources will be called to see if landing zone cleanup is needed
      billingProjectDeletion,
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao] // nothing mocked - will fail if called
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    // maybeCleanupResources will get called with both lifecycles
    verify(billingProjectLifecycle, Mockito.times(2))
      .maybeCleanupResources(billingProjectName, maybeGoogleProject = true, testContext)
    verify(billingProjectDeletion).finalizeDelete(billingProjectName, testContext, billingProfileExpected = false)
  }

  it should "call finalizeDelete when the Azure lifecycle maybeCleanupResources returns a jobId of None" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")

    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    when(billingProjectLifecycle.maybeCleanupResources(billingProjectName, maybeGoogleProject = false, testContext))
      .thenReturn(Future.successful(None))

    val billingProjectDeletion = mock[BillingProjectDeletion]
    when(billingProjectDeletion.finalizeDelete(billingProjectName, testContext, true)).thenReturn(Future.successful())

    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      happyBillingRepository(Some(azureBillingProfile), billingProfileManagerDAO),
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS), // google
      billingProjectLifecycle, // azure
      billingProjectDeletion,
      mock[MultiCloudWorkspaceConfig],
      mock[WorkspaceManagerResourceMonitorRecordDao] // nothing mocked - will fail if called
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    // Will only call Azure maybeCleanupResources because we know cloud context is Azure
    verify(billingProjectLifecycle).maybeCleanupResources(billingProjectName, maybeGoogleProject = false, testContext)
    verify(billingProjectDeletion).finalizeDelete(billingProjectName, testContext, true)
  }

  it should "call the Azure lifecycle to initiate delete of an Azure project when maybeCleanupResources returns a jobId" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val jobId = UUID.fromString("c1024c05-40a6-4a12-b12e-028e445aec3b")

    val billingProjectLifecycle = initiateDeleteLifecycle(Future.successful(Some(jobId)))
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      happyBillingRepository(Some(azureBillingProfile), billingProfileManagerDAO),
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS), // google
      billingProjectLifecycle, // azure
      mock[BillingProjectDeletion],
      mock[MultiCloudWorkspaceConfig],
      happyMonitorRecordDao
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(billingProjectLifecycle).maybeCleanupResources(billingProjectName, maybeGoogleProject = false, testContext)
  }

  it should "create a job to delete the Azure project after calling maybeCleanupResources" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val jobId = UUID.randomUUID()

    def matchedExpectedEvent(e: WorkspaceManagerResourceMonitorRecord) =
      e.jobControlId.toString == jobId.toString &&
        e.billingProjectId.get == billingProjectName.value &&
        e.userEmail.get == testContext.userInfo.userEmail.value &&
        e.jobType == BpmBillingProjectDelete
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    when(monitorRecordDao.create(ArgumentMatchers.argThat(matchedExpectedEvent))).thenReturn(Future.successful())
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      happyBillingRepository(Some(azureBillingProfile), billingProfileManagerDAO),
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS), // google
      initiateDeleteLifecycle(Future.successful(Some(jobId))), // azure
      mock[BillingProjectDeletion],
      mock[MultiCloudWorkspaceConfig],
      monitorRecordDao
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    verify(monitorRecordDao).create(ArgumentMatchers.argThat(matchedExpectedEvent))
  }

  it should "not create a job to delete the Azure project after calling maybeCleanupResources fails" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      happyBillingRepository(Some(azureBillingProfile), billingProfileManagerDAO),
      mock[BillingProjectLifecycle](RETURNS_SMART_NULLS), // google
      initiateDeleteLifecycle(Future.failed(new Exception)), // azure
      mock[BillingProjectDeletion],
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
    when(billingRepository.getBillingProfileId(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful(Some(azureBillingProfile.getId.toString)))
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    // Mock billing project having a billing profile.
    when(billingProfileManagerDAO.getBillingProfile(billingProfileId, testContext))
      .thenReturn(Some(azureBillingProfile))
    when(billingRepository.updateCreationStatus(billingProjectName, CreationStatuses.Deleting, None))
      .thenReturn(Future.successful(1))
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Ready))
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      billingRepository,
      mock[BillingProjectLifecycle], // google
      initiateDeleteLifecycle(Future.successful(Some(jobId))), // azure
      mock[BillingProjectDeletion],
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
    // Mock billing project having a billing profile.
    when(billingRepository.getBillingProfileId(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful(Some(azureBillingProfile.getId.toString)))
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Deleting))

    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    when(billingProfileManagerDAO.getBillingProfile(billingProfileId, testContext))
      .thenReturn(Some(azureBillingProfile))

    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      billingRepository,
      mock[BillingProjectLifecycle], // google
      initiateDeleteLifecycle(Future.successful(Some(jobId))), // azure
      mock[BillingProjectDeletion],
      mock[MultiCloudWorkspaceConfig],
      happyMonitorRecordDao
    )

    intercept[BillingProjectDeletionException](
      Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)
    )
  }

  it should "delete the billing project if a billing profile ID exists, but the profile cannot be obtained" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val billingRepository = mock[BillingRepository]
    when(billingRepository.failUnlessHasNoWorkspaces(billingProjectName)(executionContext))
      .thenReturn(Future.successful())
    // Mock billing project having a billing profile.
    when(billingRepository.getBillingProfileId(ArgumentMatchers.any())(ArgumentMatchers.any()))
      .thenReturn(Future.successful(Some(azureBillingProfile.getId.toString)))
    // Billing project is in a status where it could be deleted.
    when(billingRepository.getCreationStatus(billingProjectName)(executionContext))
      .thenReturn(Future.successful(CreationStatuses.Ready))

    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    // No billing profile is returned, cloud context cannot be obtained
    when(billingProfileManagerDAO.getBillingProfile(billingProfileId, testContext))
      .thenReturn(None)

    // jobID of None would allow billing project deletion to continue (if billing profile was returned).
    val billingProjectLifecycle = mock[BillingProjectLifecycle]
    // maybeGoogleProject = true because no cloud platform can be obtained (no billing profile)
    when(billingProjectLifecycle.maybeCleanupResources(billingProjectName, true, testContext))
      .thenReturn(Future.successful(None))

    val billingProjectDeletion = mock[BillingProjectDeletion]
    when(billingProjectDeletion.finalizeDelete(billingProjectName, testContext, true)).thenReturn(Future.successful())
    val bpo = new BillingProjectOrchestrator(
      testContext,
      alwaysGiveAccessSamDao,
      mock[NotificationDAO],
      billingProfileManagerDAO,
      billingRepository,
      billingProjectLifecycle, // google, will be called for cleanup since no cloud platform can be determined
      billingProjectLifecycle, // azure, will be called for cleanup since no cloud platform can be determined
      billingProjectDeletion,
      mock[MultiCloudWorkspaceConfig],
      happyMonitorRecordDao
    )

    Await.result(bpo.deleteBillingProjectV2(billingProjectName), Duration.Inf)

    // maybeCleanupResources will get called with both lifecycles
    verify(billingProjectLifecycle, Mockito.times(2))
      .maybeCleanupResources(billingProjectName, maybeGoogleProject = true, testContext)
    verify(billingProjectDeletion).finalizeDelete(billingProjectName, testContext, billingProfileExpected = true)
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
