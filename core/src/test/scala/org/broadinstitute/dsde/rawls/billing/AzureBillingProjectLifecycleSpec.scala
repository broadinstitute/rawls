package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.client.{ApiException => BpmApiException}
import bio.terra.profile.model.{AzureManagedAppModel, ProfileModel}
import bio.terra.workspace.model.{CreateLandingZoneResult, DeleteAzureLandingZoneResult, ErrorReport, JobReport}
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.HttpWorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  ProjectAccessUpdate,
  ProjectRoles,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.mockito.ArgumentMatchers.{any, anyString, argThat}
import org.mockito.Mockito.{doNothing, doReturn, verify, when}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.SQLException
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class AzureBillingProjectLifecycleSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)
  val coords: AzureManagedAppCoordinates = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
  val billingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("fake_name")
  val createRequest: CreateRawlsV2BillingProjectFullRequest = CreateRawlsV2BillingProjectFullRequest(
    billingProjectName,
    None,
    None,
    Some(coords),
    None,
    None
  )
  val createProtectedRequest: CreateRawlsV2BillingProjectFullRequest = CreateRawlsV2BillingProjectFullRequest(
    billingProjectName,
    None,
    None,
    Some(coords),
    None,
    None,
    Some(true)
  )
  val profileModel: ProfileModel = new ProfileModel().id(UUID.randomUUID())
  val landingZoneDefinition = "fake-landing-zone-definition"
  val protectedLandingZoneDefinition = "fake-protected-landing-zone-definition"
  val landingZoneVersion = "fake-landing-zone-version"
  val landingZoneParameters: Map[String, String] = Map("fake_parameter" -> "fake_value")
  val costSavingLandingZoneParameters: Map[String, String] = Map("fake_parameter" -> "false")
  val azConfig: AzureConfig = AzureConfig(
    landingZoneDefinition,
    protectedLandingZoneDefinition,
    landingZoneVersion,
    landingZoneParameters,
    costSavingLandingZoneParameters,
    landingZoneAllowAttach = false
  )
  val landingZoneId: UUID = UUID.randomUUID()
  val landingZoneJobId: UUID = UUID.randomUUID()
  val multiCloudWorkspaceConfig = new MultiCloudWorkspaceConfig(
    MultiCloudWorkspaceManagerConfig("fake_app_id", Duration(1, "second"), Duration(1, "second")),
    azConfig
  )

  behavior of "validateBillingProjectCreationRequest"

  it should "fail when provided GCP billing info" in {
    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      mock[BillingRepository],
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )
    val gcpCreateRequest = CreateRawlsV2BillingProjectFullRequest(
      billingProjectName,
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None,
      None,
      None
    )

    intercept[NotImplementedError] {
      Await.result(
        bp.validateBillingProjectCreationRequest(gcpCreateRequest, mock[BillingProfileManagerDAO], testContext),
        Duration.Inf
      )
    }
  }

  it should "fail when no matching managed app is found" in {
    val bpm = mock[BillingProfileManagerDAO]
    when(bpm.listManagedApps(coords.subscriptionId, false, testContext))
      .thenReturn(Seq())
    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      mock[BillingRepository],
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[ManagedAppNotFoundException] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, bpm, testContext), Duration.Inf)
    }
  }

  it should "fail when BPM errors out" in {
    val bpm = mock[BillingProfileManagerDAO]
    when(bpm.listManagedApps(coords.subscriptionId, false, testContext))
      .thenThrow(new RuntimeException("failed"))

    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      mock[BillingRepository],
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[RuntimeException] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, bpm, testContext), Duration.Inf)
    }
  }

  it should "succeed when a matching managed app is found" in {
    val bpm = mock[BillingProfileManagerDAO]
    when(bpm.listManagedApps(coords.subscriptionId, false, testContext))
      .thenReturn(
        Seq(
          new AzureManagedAppModel()
            .subscriptionId(coords.subscriptionId)
            .managedResourceGroupId(coords.managedResourceGroupId)
            .tenantId(coords.tenantId)
        )
      )
    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      mock[BillingRepository],
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    Await.result(bp.validateBillingProjectCreationRequest(createRequest, bpm, testContext), Duration.Inf)
  }

  behavior of "postCreationSteps"

  it should "fail if a landing zone ID is provided and Rawls is not configured to attach to existing landing zones" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val createRequestWithExistingLz = CreateRawlsV2BillingProjectFullRequest(
      billingProjectName,
      None,
      None,
      Some(AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake", Some(UUID.randomUUID()))),
      None,
      None
    )
    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequestWithExistingLz.projectName.value),
        ArgumentMatchers.eq(createRequestWithExistingLz.billingInfo),
        any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]

    val bp = new AzureBillingProjectLifecycle(mock[SamDAO], repo, workspaceManagerDAO, monitorRecordDao)

    intercept[LandingZoneCreationException] {
      Await.result(bp.postCreationSteps(
                     createRequestWithExistingLz,
                     multiCloudWorkspaceConfig,
                     bpm,
                     testContext
                   ),
                   Duration.Inf
      )
    }

    verify(workspaceManagerDAO, Mockito.times(0)).createLandingZone(anyString(),
                                                                    anyString(),
                                                                    any[Map[String, String]](),
                                                                    any[UUID],
                                                                    any[RawlsRequestContext],
                                                                    any[Option[UUID]]
    )
  }

  it should "attach the provided landing zone ID if configured" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val lzId = UUID.randomUUID()
    val lzAttachAzConfig =
      AzureConfig(
        landingZoneDefinition,
        protectedLandingZoneDefinition,
        landingZoneVersion,
        landingZoneParameters,
        costSavingLandingZoneParameters,
        landingZoneAllowAttach = true
      )
    val createRequestWithExistingLz = CreateRawlsV2BillingProjectFullRequest(
      billingProjectName,
      None,
      None,
      Some(
        AzureManagedAppCoordinates(coords.tenantId, coords.subscriptionId, coords.managedResourceGroupId, Some(lzId))
      ),
      None,
      None
    )
    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequestWithExistingLz.projectName.value),
        ArgumentMatchers.eq(createRequestWithExistingLz.billingInfo),
        any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    val expectedLzParams = landingZoneParameters ++ Map("attach" -> "true")
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            expectedLzParams,
                                            profileModel.getId,
                                            testContext,
                                            Some(lzId)
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(repo.updateLandingZoneId(createRequestWithExistingLz.projectName, Option(landingZoneId)))
      .thenReturn(Future.successful(1))
    when(repo.setBillingProfileId(createRequestWithExistingLz.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))
    val monitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    doReturn(Future.successful())
      .when(monitorRecordDao)
      .create(any)
    val bp = new AzureBillingProjectLifecycle(mock[SamDAO], repo, workspaceManagerDAO, monitorRecordDao)

    assertResult(CreationStatuses.CreatingLandingZone) {
      Await.result(bp.postCreationSteps(
                     createRequestWithExistingLz,
                     new MultiCloudWorkspaceConfig(null, lzAttachAzConfig),
                     bpm,
                     testContext
                   ),
                   Duration.Inf
      )
    }

    verify(workspaceManagerDAO, Mockito.times(1)).createLandingZone(landingZoneDefinition,
                                                                    landingZoneVersion,
                                                                    expectedLzParams,
                                                                    profileModel.getId,
                                                                    testContext,
                                                                    Some(lzId)
    )
  }

  it should "store the landing zone ID and job creation ID and link the profile ID to the billing project record" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequest.projectName.value),
        ArgumentMatchers.eq(createRequest.billingInfo),
        ArgumentMatchers.eq(Map[String, List[(String, String)]]()),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(repo.updateLandingZoneId(createRequest.projectName, Option(landingZoneId))).thenReturn(Future.successful(1))
    when(repo.setBillingProfileId(createRequest.projectName, profileModel.getId)).thenReturn(Future.successful(1))

    val wsmResouceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]

    doReturn(Future.successful())
      .when(wsmResouceRecordDao)
      .create(any)

    val bp = new AzureBillingProjectLifecycle(mock[SamDAO], repo, workspaceManagerDAO, wsmResouceRecordDao)

    assertResult(CreationStatuses.CreatingLandingZone) {
      Await.result(bp.postCreationSteps(
                     createRequest,
                     multiCloudWorkspaceConfig,
                     bpm,
                     testContext
                   ),
                   Duration.Inf
      )
    }
    verify(workspaceManagerDAO, Mockito.times(1)).createLandingZone(landingZoneDefinition,
                                                                    landingZoneVersion,
                                                                    landingZoneParameters,
                                                                    profileModel.getId,
                                                                    testContext,
                                                                    None
    )
    verify(bpm, Mockito.times(1)).createBillingProfile(createRequest.projectName.value,
                                                       createRequest.billingInfo,
                                                       Map[String, List[(String, String)]](),
                                                       testContext
    )
    verify(repo, Mockito.times(1)).updateLandingZoneId(createRequest.projectName, Option(landingZoneId))
    verify(repo, Mockito.times(1)).setBillingProfileId(createRequest.projectName, profileModel.getId)
    verify(wsmResouceRecordDao, Mockito.times(1))
      .create(argThat { (job: WorkspaceManagerResourceMonitorRecord) =>
        job.jobType == JobType.AzureLandingZoneResult &&
        job.jobControlId == landingZoneJobId &&
        job.billingProjectId.contains(createRequest.projectName.value)
      })
  }

  it should "add additional members to the BPM policy if specified during billing project creation" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val wsmResourceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    val bp = new AzureBillingProjectLifecycle(mock[SamDAO], repo, workspaceManagerDAO, wsmResourceRecordDao)

    val user1Email = "user1@foo.bar"
    val user2Email = "user2@foo.bar"
    val user3Email = "user3@foo.bar"

    val createRequestWithMembers = createRequest.copy(members =
      Some(
        Set(
          ProjectAccessUpdate(user1Email, ProjectRoles.Owner),
          ProjectAccessUpdate(user2Email, ProjectRoles.Owner),
          ProjectAccessUpdate(user3Email, ProjectRoles.User)
        )
      )
    )

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createRequestWithMembers.projectName.value),
        ArgumentMatchers.eq(createRequestWithMembers.billingInfo),
        any(),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(repo.updateLandingZoneId(createRequestWithMembers.projectName, Option(landingZoneId)))
      .thenReturn(Future.successful(1))
    when(repo.setBillingProfileId(createRequestWithMembers.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))

    doReturn(Future.successful())
      .when(wsmResourceRecordDao)
      .create(any)

    Await.result(bp.postCreationSteps(
                   createRequestWithMembers,
                   multiCloudWorkspaceConfig,
                   bpm,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(bpm).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.Owner),
      ArgumentMatchers.eq(user1Email),
      ArgumentMatchers.any[RawlsRequestContext]
    )
    verify(bpm).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.Owner),
      ArgumentMatchers.eq(user2Email),
      ArgumentMatchers.any[RawlsRequestContext]
    )
    verify(bpm).addProfilePolicyMember(
      ArgumentMatchers.eq(profileModel.getId),
      ArgumentMatchers.eq(ProfilePolicy.User),
      ArgumentMatchers.eq(user3Email),
      ArgumentMatchers.any[RawlsRequestContext]
    )
  }

  it should "wrap exceptions thrown by synchronous calls in a Future" in {
    val bpm = mock[BillingProfileManagerDAO]
    val thrownExceptionMessage = "Exception from BPM"
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               any(),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenThrow(new RuntimeException(thrownExceptionMessage))

    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      mock[BillingRepository],
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val result = bp.postCreationSteps(
      createRequest,
      mock[MultiCloudWorkspaceConfig],
      bpm,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception.getMessage.shouldBe(thrownExceptionMessage)
    }
  }

  it should "create a protected data landing zone and attach a protected-data policy to the billing profile if requested" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val expectedPolicy = Map("protected-data" -> List[(String, String)]())

    when(
      bpm.createBillingProfile(
        ArgumentMatchers.eq(createProtectedRequest.projectName.value),
        ArgumentMatchers.eq(createProtectedRequest.billingInfo),
        ArgumentMatchers.eq(expectedPolicy),
        ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(protectedLandingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(repo.updateLandingZoneId(createProtectedRequest.projectName, Option(landingZoneId)))
      .thenReturn(Future.successful(1))
    when(repo.setBillingProfileId(createProtectedRequest.projectName, profileModel.getId))
      .thenReturn(Future.successful(1))

    val wsmResouceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]

    doReturn(Future.successful())
      .when(wsmResouceRecordDao)
      .create(any)

    val bp = new AzureBillingProjectLifecycle(mock[SamDAO], repo, workspaceManagerDAO, wsmResouceRecordDao)

    assertResult(CreationStatuses.CreatingLandingZone) {
      Await.result(bp.postCreationSteps(
                     createProtectedRequest,
                     multiCloudWorkspaceConfig,
                     bpm,
                     testContext
                   ),
                   Duration.Inf
      )
    }
    verify(workspaceManagerDAO, Mockito.times(1)).createLandingZone(protectedLandingZoneDefinition,
                                                                    landingZoneVersion,
                                                                    landingZoneParameters,
                                                                    profileModel.getId,
                                                                    testContext,
                                                                    None
    )
    verify(bpm, Mockito.times(1)).createBillingProfile(createProtectedRequest.projectName.value,
                                                       createProtectedRequest.billingInfo,
                                                       expectedPolicy,
                                                       testContext
    )
  }

  it should "handle landing zone creation errors and delete the billing profile" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val landingZoneErrorMessage = "Error from creating landing zone"
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               any(),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult().errorReport(new ErrorReport().statusCode(500).message(landingZoneErrorMessage))
    )
    when(repo.getBillingProjectsWithProfile(Some(profileModel.getId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(createRequest.projectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          )
        )
      )
    )
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )
    val result = bp.postCreationSteps(
      createRequest,
      multiCloudWorkspaceConfig,
      bpm,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[LandingZoneCreationException]
      assert(exception.getMessage.contains(landingZoneErrorMessage))
      verify(workspaceManagerDAO, Mockito.times(0)).deleteLandingZone(landingZoneId, testContext)
      verify(bpm, Mockito.times(1)).deleteBillingProfile(profileModel.getId, testContext)
    }
  }

  it should "handle CreateLandingZoneResult missing the job report and delete resources" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               any(),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
    )
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(Some(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id"))))
    when(repo.getBillingProjectsWithProfile(Some(profileModel.getId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(createRequest.projectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          )
        )
      )
    )
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )
    val result = bp.postCreationSteps(
      createRequest,
      multiCloudWorkspaceConfig,
      bpm,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[LandingZoneCreationException]
      verify(workspaceManagerDAO, Mockito.times(1)).deleteLandingZone(landingZoneId, testContext)
      verify(bpm, Mockito.times(1)).deleteBillingProfile(profileModel.getId, testContext)
    }
  }

  it should "handle landing zone unexpected errors and delete the billing profile" in {
    val bpm = mock[BillingProfileManagerDAO]
    val repo = mock[BillingRepository]
    val unexpectedError = "Error from WSM"
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               any(),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenThrow(new RuntimeException(unexpectedError))
    when(repo.getBillingProjectsWithProfile(Some(profileModel.getId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(createRequest.projectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          )
        )
      )
    )
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )
    val result = bp.postCreationSteps(
      createRequest,
      multiCloudWorkspaceConfig,
      bpm,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[RuntimeException]
      assert(exception.getMessage.contains(unexpectedError))
      verify(workspaceManagerDAO, Mockito.times(0)).deleteLandingZone(landingZoneId, testContext)
      verify(bpm, Mockito.times(1)).deleteBillingProfile(profileModel.getId, testContext)
    }
  }

  it should "handle errors after landing zone creation and delete resources" in {
    val bpm = mock[BillingProfileManagerDAO]
    val repo = mock[BillingRepository]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val billingRepoError = "Error from billing repository"
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               any(),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(Some(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id"))))
    when(repo.updateLandingZoneId(createRequest.projectName, Option(landingZoneId)))
      .thenReturn(Future.failed(new RuntimeException(billingRepoError)))
    when(repo.getBillingProjectsWithProfile(Some(profileModel.getId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(createRequest.projectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          )
        )
      )
    )

    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )
    val result = bp.postCreationSteps(
      createRequest,
      multiCloudWorkspaceConfig,
      bpm,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[RuntimeException]
      assert(exception.getMessage.contains(billingRepoError))
      verify(bpm, Mockito.times(1)).deleteBillingProfile(profileModel.getId, testContext)
      verify(workspaceManagerDAO, Mockito.times(1)).deleteLandingZone(landingZoneId, testContext)
    }
  }

  it should "return the original error if resource deletion also errors" in {
    val bpm = mock[BillingProfileManagerDAO]
    val repo = mock[BillingRepository]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val billingRepoError = "SQLException from billing repository"
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               any(),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    // Throw exception when deleting profile during cleanup code.
    when(bpm.deleteBillingProfile(profileModel.getId, testContext))
      .thenThrow(new RuntimeException("BPM profile deletion"))
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext,
                                            None
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    // Deletion of landing zone during cleanup does not error.
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(Some(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id"))))
    // Exception thrown after creation of billing profile and landing zone.
    // This exception should be visible to the user.
    when(repo.updateLandingZoneId(createRequest.projectName, Option(landingZoneId)))
      .thenReturn(Future.failed(new SQLException(billingRepoError)))
    when(repo.getBillingProjectsWithProfile(Some(profileModel.getId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(createRequest.projectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(profileModel.getId.toString)
          )
        )
      )
    )

    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )
    val result = bp.postCreationSteps(
      createRequest,
      multiCloudWorkspaceConfig,
      bpm,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[SQLException]
      assert(exception.getMessage.contains(billingRepoError))
      verify(bpm, Mockito.times(1)).deleteBillingProfile(profileModel.getId, testContext)
      verify(workspaceManagerDAO, Mockito.times(1)).deleteLandingZone(landingZoneId, testContext)
    }
  }

  behavior of "initiateDelete"

  it should "return None for the jobId if the landing zone does not exist" in {
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )

    val jobId = Await.result(bp.initiateDelete(billingProjectName, testContext), Duration.Inf)

    assert(jobId.isEmpty)

    verify(workspaceManagerDAO, Mockito.never()).deleteLandingZone(ArgumentMatchers.any(), ArgumentMatchers.any())
    verify(bpm, Mockito.never()).deleteBillingProfile(ArgumentMatchers.any[UUID], ArgumentMatchers.eq(testContext))
  }

  it should "delete the landing zone if the id exists" in {
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(Some(landingZoneId.toString)))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val jobReportId = UUID.randomUUID()
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(
        Some(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id(jobReportId.toString)))
      )
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )

    Await.result(bp.initiateDelete(billingProjectName, testContext), Duration.Inf) shouldBe (Some(jobReportId))

    verify(workspaceManagerDAO).deleteLandingZone(landingZoneId, testContext)
  }

  it should "succeed returning None when there is no landing zone for the landing zone id" in {
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(Some(landingZoneId.toString)))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    // The WSM DAO will return None if the landing zone deletion returns 404 or 403, indicating the landing zone does not exist
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenAnswer(_ => None)
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )

    Await.result(bp.initiateDelete(billingProjectName, testContext), Duration.Inf) shouldBe None

    verify(workspaceManagerDAO).deleteLandingZone(landingZoneId, testContext)
  }

  it should "handle the landing zone error reports" in {
    val billingProfileId = profileModel.getId
    val landingZoneErrorMessage = "error from deleting landing zone"

    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(Some(landingZoneId.toString)))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(Some(billingProfileId.toString)))
    when(repo.getBillingProjectsWithProfile(Some(billingProfileId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(billingProjectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(billingProfileId.toString)
          )
        )
      )
    )

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext)).thenReturn(
      Some(
        new DeleteAzureLandingZoneResult()
          .landingZoneId(UUID.randomUUID())
          .errorReport(new ErrorReport().statusCode(500).message(landingZoneErrorMessage))
      )
    )
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )

    val e = intercept[RawlsExceptionWithErrorReport](
      Await.result(bp.initiateDelete(billingProjectName, testContext), Duration.Inf)
    )

    assert(e.errorReport.statusCode.get == StatusCode.int2StatusCode(500))
    assert(e.errorReport.message.contains(landingZoneErrorMessage))
  }

  behavior of "finalizeDelete"

  it should "delete the billing profile if other no projects reference it" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")
    val billingProfileId = profileModel.getId
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(Some(billingProfileId.toString)))
    when(repo.deleteBillingProject(ArgumentMatchers.any())).thenReturn(Future.successful(true))
    when(repo.getBillingProjectsWithProfile(Some(billingProfileId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(
            billingProjectName,
            CreationStatuses.Ready,
            None,
            None,
            billingProfileId = Some(billingProfileId.toString)
          )
        )
      )
    )
    val bpm = mock[BillingProfileManagerDAO]
    doNothing().when(bpm).deleteBillingProfile(ArgumentMatchers.eq(billingProfileId), ArgumentMatchers.eq(testContext))
    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      repo,
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)

    verify(bpm).deleteBillingProfile(ArgumentMatchers.eq(billingProfileId), ArgumentMatchers.eq(testContext))
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "not delete the billing profile if other projects reference it" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")

    val repo = mock[BillingRepository]
    val billingProfileId = profileModel.getId
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(Some(billingProfileId.toString)))
    when(repo.deleteBillingProject(ArgumentMatchers.any())).thenReturn(Future.successful(true))
    when(repo.getBillingProjectsWithProfile(Some(billingProfileId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(billingProjectName,
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(billingProfileId.toString)
          ),
          RawlsBillingProject(RawlsBillingProjectName("other_billing_project"),
                              CreationStatuses.Ready,
                              None,
                              None,
                              billingProfileId = Some(billingProfileId.toString)
          )
        )
      )
    )

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val bp =
      new AzureBillingProjectLifecycle(mock[SamDAO],
                                       repo,
                                       workspaceManagerDAO,
                                       mock[WorkspaceManagerResourceMonitorRecordDao]
      )

    Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)

    verify(bpm, Mockito.never).deleteBillingProfile(billingProfileId, testContext)
    // Billing project is still deleted
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "succeed if the billing profile id does not exist" in {
    val repo = mock[BillingRepository]
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.deleteBillingProject(ArgumentMatchers.eq(billingProjectName))).thenReturn(Future.successful(true))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val bp =
      new AzureBillingProjectLifecycle(
        mock[SamDAO],
        repo,
        workspaceManagerDAO,
        mock[WorkspaceManagerResourceMonitorRecordDao]
      )

    Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)

    verify(bpm, Mockito.never()).deleteBillingProfile(ArgumentMatchers.any[UUID], ArgumentMatchers.eq(testContext))
    verify(repo).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }

  it should "fail on non-404 errors from BPM" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")
    val billingProfileId = profileModel.getId
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(Some(billingProfileId.toString)))
    when(repo.deleteBillingProject(ArgumentMatchers.any())).thenReturn(Future.successful(true))
    when(repo.getBillingProjectsWithProfile(Some(billingProfileId))).thenReturn(
      Future.successful(
        Seq(
          RawlsBillingProject(
            billingProjectName,
            CreationStatuses.Ready,
            None,
            None,
            billingProfileId = Some(billingProfileId.toString)
          )
        )
      )
    )
    val bpm = mock[BillingProfileManagerDAO]

    when(bpm.deleteBillingProfile(ArgumentMatchers.eq(billingProfileId), ArgumentMatchers.eq(testContext)))
      .thenAnswer(_ => throw new BpmApiException(HttpStatus.SC_FORBIDDEN, "forbidden"))

    val bp = new AzureBillingProjectLifecycle(
      mock[SamDAO],
      repo,
      mock[HttpWorkspaceManagerDAO],
      mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[BpmApiException] {
      Await.result(bp.finalizeDelete(billingProjectName, bpm, testContext), Duration.Inf)
    }
    verify(bpm).deleteBillingProfile(ArgumentMatchers.eq(billingProfileId), ArgumentMatchers.eq(testContext))
    verify(repo, Mockito.never).deleteBillingProject(ArgumentMatchers.eq(billingProjectName))
  }
}
