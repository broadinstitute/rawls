package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.{AzureManagedAppModel, ProfileModel}
import bio.terra.workspace.model.{CreateLandingZoneResult, DeleteAzureLandingZoneResult, ErrorReport, JobReport}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.HttpWorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{doReturn, verify, when}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.SQLException
import java.util.UUID
import scala.collection.immutable.Map
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BpmBillingProjectLifecycleSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)
  val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
  val billingProjectName = RawlsBillingProjectName("fake_name")
  val createRequest = CreateRawlsV2BillingProjectFullRequest(
    billingProjectName,
    None,
    None,
    Some(coords)
  )
  val profileModel = new ProfileModel().id(UUID.randomUUID())
  val landingZoneDefinition = "fake-landing-zone-definition"
  val landingZoneVersion = "fake-landing-zone-version"
  val landingZoneParameters = Map("fake_parameter" -> "fake_value")
  val azConfig: AzureConfig = AzureConfig(
    landingZoneDefinition,
    landingZoneVersion,
    landingZoneParameters
  )
  val landingZoneId = UUID.randomUUID()
  val landingZoneJobId = UUID.randomUUID()

  behavior of "validateBillingProjectCreationRequest"

  it should "fail when provided GCP billing info" in {
    val bp = new BpmBillingProjectLifecycle(mock[BillingRepository],
                                            mock[BillingProfileManagerDAO],
                                            mock[HttpWorkspaceManagerDAO],
                                            mock[WorkspaceManagerResourceMonitorRecordDao]
    )
    val gcpCreateRequest = CreateRawlsV2BillingProjectFullRequest(
      billingProjectName,
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )

    intercept[NotImplementedError] {
      Await.result(bp.validateBillingProjectCreationRequest(gcpCreateRequest, testContext), Duration.Inf)
    }
  }

  it should "fail when no matching managed app is found" in {
    val bpm = mock[BillingProfileManagerDAO]
    when(bpm.listManagedApps(coords.subscriptionId, false, testContext))
      .thenReturn(Seq())
    val bp = new BpmBillingProjectLifecycle(mock[BillingRepository],
                                            bpm,
                                            mock[HttpWorkspaceManagerDAO],
                                            mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[ManagedAppNotFoundException] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
    }
  }

  it should "fail when BPM errors out" in {
    val bpm = mock[BillingProfileManagerDAO]
    when(bpm.listManagedApps(coords.subscriptionId, false, testContext))
      .thenThrow(new RuntimeException("failed"))

    val bp = new BpmBillingProjectLifecycle(mock[BillingRepository],
                                            bpm,
                                            mock[HttpWorkspaceManagerDAO],
                                            mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    intercept[RuntimeException] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
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
    val bp = new BpmBillingProjectLifecycle(mock[BillingRepository],
                                            bpm,
                                            mock[HttpWorkspaceManagerDAO],
                                            mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    Await.result(bp.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
  }

  behavior of "postCreationSteps"

  it should "store the landing zone ID and job creation ID and link the profile ID to the billing project record" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]

    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(repo.updateLandingZoneId(createRequest.projectName, landingZoneId)).thenReturn(Future.successful(1))
    when(repo.setBillingProfileId(createRequest.projectName, profileModel.getId)).thenReturn(Future.successful(1))

    val wsmResouceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]

    doReturn(Future.successful())
      .when(wsmResouceRecordDao)
      .create(any)

    val bp = new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, wsmResouceRecordDao)

    assertResult(CreationStatuses.CreatingLandingZone) {
      Await.result(bp.postCreationSteps(
                     createRequest,
                     new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
                     testContext
                   ),
                   Duration.Inf
      )
    }
    verify(workspaceManagerDAO, Mockito.times(1)).createLandingZone(landingZoneDefinition,
                                                                    landingZoneVersion,
                                                                    landingZoneParameters,
                                                                    profileModel.getId,
                                                                    testContext
    )
    verify(repo, Mockito.times(1)).updateLandingZoneId(createRequest.projectName, landingZoneId)
    verify(repo, Mockito.times(1)).setBillingProfileId(createRequest.projectName, profileModel.getId)
    verify(wsmResouceRecordDao, Mockito.times(1))
      .create(argThat { (job: WorkspaceManagerResourceMonitorRecord) =>
        job.jobType == JobType.AzureLandingZoneResult &&
        job.jobControlId == landingZoneJobId &&
        job.billingProjectId.contains(createRequest.projectName.value)
      })
  }

  it should "wrap exceptions thrown by synchronous calls in a Future" in {
    val bpm = mock[BillingProfileManagerDAO]
    val thrownExceptionMessage = "Exception from BPM"
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenThrow(new RuntimeException(thrownExceptionMessage))

    val bp = new BpmBillingProjectLifecycle(mock[BillingRepository],
                                            bpm,
                                            mock[HttpWorkspaceManagerDAO],
                                            mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    val result = bp.postCreationSteps(
      createRequest,
      mock[MultiCloudWorkspaceConfig],
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception.getMessage.shouldBe(thrownExceptionMessage)
    }
  }

  it should "handle landing zone creation errors and delete the billing profile" in {
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val landingZoneErrorMessage = "Error from creating landing zone"
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext
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
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])
    val result = bp.postCreationSteps(
      createRequest,
      new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
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
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
    )
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id")))
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
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])
    val result = bp.postCreationSteps(
      createRequest,
      new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
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
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext
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
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])
    val result = bp.postCreationSteps(
      createRequest,
      new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
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
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition,
                                            landingZoneVersion,
                                            landingZoneParameters,
                                            profileModel.getId,
                                            testContext
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id")))
    when(repo.updateLandingZoneId(createRequest.projectName, landingZoneId))
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
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])
    val result = bp.postCreationSteps(
      createRequest,
      new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
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
      bpm.createBillingProfile(createRequest.projectName.value, createRequest.billingInfo, testContext)
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
                                            testContext
      )
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    // Deletion of landing zone during cleanup does not error.
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id")))
    // Exception thrown after creation of billing profile and landing zone.
    // This exception should be visible to the user.
    when(repo.updateLandingZoneId(createRequest.projectName, landingZoneId))
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
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])
    val result = bp.postCreationSteps(
      createRequest,
      new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[SQLException]
      assert(exception.getMessage.contains(billingRepoError))
      verify(bpm, Mockito.times(1)).deleteBillingProfile(profileModel.getId, testContext)
      verify(workspaceManagerDAO, Mockito.times(1)).deleteLandingZone(landingZoneId, testContext)
    }
  }

  behavior of "preDeletionSteps"

  it should "error if the landing zone is still being created" in {
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.CreatingLandingZone))
    val landingZoneErrorMessage = "cannot be deleted because its landing zone is still being created"
    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val bp =
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    val result = bp.preDeletionSteps(
      billingProjectName,
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception shouldBe a[BillingProjectDeletionException]
      assert(exception.getMessage.contains(landingZoneErrorMessage))
    }
  }

  it should "succeed if the landing zone and billing profiles id do not exist" in {
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(None))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    val bp =
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    Await.result(bp.preDeletionSteps(
                   billingProjectName,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(workspaceManagerDAO, Mockito.times(0))
      .deleteLandingZone(ArgumentMatchers.any[UUID], ArgumentMatchers.eq(testContext))
    verify(bpm, Mockito.times(0)).deleteBillingProfile(ArgumentMatchers.any[UUID], ArgumentMatchers.eq(testContext))
  }

  it should "delete the landing zone if the id exists" in {
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(Some(landingZoneId.toString)))
    // Mock no associated billing profile to delete
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(None))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext))
      .thenReturn(new DeleteAzureLandingZoneResult().jobReport(new JobReport().id("fake-id")))
    val bp =
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    Await.result(bp.preDeletionSteps(
                   billingProjectName,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(workspaceManagerDAO, Mockito.times(1)).deleteLandingZone(landingZoneId, testContext)
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
      new DeleteAzureLandingZoneResult().errorReport(new ErrorReport().statusCode(500).message(landingZoneErrorMessage))
    )
    val bp =
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    Await.result(bp.preDeletionSteps(
                   billingProjectName,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(repo, Mockito.times(1)).getBillingProfileId(billingProjectName)
    verify(repo, Mockito.times(1)).getBillingProjectsWithProfile(Some(billingProfileId))
    verify(bpm, Mockito.times(1)).deleteBillingProfile(
      ArgumentMatchers.eq(billingProfileId),
      ArgumentMatchers.any()
    )
  }

  it should "delete the billing profile if no other project references it" in {
    val billingProfileId = profileModel.getId
    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))
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
    val bp =
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    Await.result(bp.preDeletionSteps(
                   billingProjectName,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(repo, Mockito.times(1)).getBillingProfileId(billingProjectName)
    verify(repo, Mockito.times(1)).getBillingProjectsWithProfile(Some(billingProfileId))
    verify(bpm, Mockito.times(1)).deleteBillingProfile(
      ArgumentMatchers.eq(billingProfileId),
      ArgumentMatchers.any()
    )
  }

  it should "not delete the billing profile if other projects reference it" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")

    val repo = mock[BillingRepository]
    val billingProfileId = profileModel.getId
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(None))
    when(repo.getBillingProfileId(billingProjectName)).thenReturn(Future.successful(Some(billingProfileId.toString)))
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
      new BpmBillingProjectLifecycle(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    Await.result(bp.preDeletionSteps(
                   billingProjectName,
                   testContext
                 ),
                 Duration.Inf
    )

    verify(bpm, Mockito.times(0)).deleteBillingProfile(billingProfileId, testContext)
  }
}
