package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.{AzureManagedAppModel, ProfileModel}
import bio.terra.workspace.model.{CreateLandingZoneResult, DeleteAzureLandingZoneResult, ErrorReport, JobReport}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
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
import org.mockito.Mockito.{verify, when}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BpmBillingProjectLifecycleSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)

  behavior of "validateBillingProjectCreationRequest"

  it should "fail when provided GCP billing info" in {
    val bp = new BpmBillingProjectLifecycle(mock[BillingRepository],
                                            mock[BillingProfileManagerDAO],
                                            mock[HttpWorkspaceManagerDAO],
                                            mock[WorkspaceManagerResourceMonitorRecordDao]
    )
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )

    intercept[NotImplementedError] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
    }
  }

  it should "fail when no matching managed app is found" in {
    val bpm = mock[BillingProfileManagerDAO]
    val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      None,
      None,
      Some(coords)
    )
    when(bpm.listManagedApps(ArgumentMatchers.eq(coords.subscriptionId), ArgumentMatchers.eq(testContext)))
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
    val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      None,
      None,
      Some(coords)
    )
    when(bpm.listManagedApps(ArgumentMatchers.eq(coords.subscriptionId), ArgumentMatchers.eq(testContext)))
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
    val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      None,
      None,
      Some(coords)
    )
    when(bpm.listManagedApps(ArgumentMatchers.eq(coords.subscriptionId), ArgumentMatchers.eq(testContext)))
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
    val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      None,
      None,
      Some(coords)
    )
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val profileModel = new ProfileModel().id(UUID.randomUUID())

    val landingZoneDefinition = "fake-landing-zone-definition"
    val landingZoneVersion = "fake-landing-zone-version"
    val azConfig: AzureConfig = AzureConfig(
      "fake-alpha-feature-group",
      "eastus",
      landingZoneDefinition,
      landingZoneVersion
    )
    val landingZoneId = UUID.randomUUID()
    val landingZoneJobId = UUID.randomUUID()
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]

    when(
      bpm.createBillingProfile(ArgumentMatchers.eq(createRequest.projectName.value),
                               ArgumentMatchers.eq(createRequest.billingInfo),
                               ArgumentMatchers.eq(testContext)
      )
    )
      .thenReturn(profileModel)
    when(
      workspaceManagerDAO.createLandingZone(landingZoneDefinition, landingZoneVersion, profileModel.getId, testContext)
    ).thenReturn(
      new CreateLandingZoneResult()
        .landingZoneId(landingZoneId)
        .jobReport(new JobReport().id(landingZoneJobId.toString))
    )
    when(repo.updateLandingZoneId(createRequest.projectName, landingZoneId)).thenReturn(Future.successful(1))
    when(repo.setBillingProfileId(createRequest.projectName, profileModel.getId)).thenReturn(Future.successful(1))

    val wsmResouceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(wsmResouceRecordDao.create(landingZoneJobId, JobType.AzureLandingZoneResult, createRequest.projectName.value))
      .thenReturn(Future.successful())
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
                                                                    profileModel.getId,
                                                                    testContext
    )
    verify(repo, Mockito.times(1)).updateLandingZoneId(createRequest.projectName, landingZoneId)
    verify(repo, Mockito.times(1)).setBillingProfileId(createRequest.projectName, profileModel.getId)
    verify(wsmResouceRecordDao, Mockito.times(1))
      .create(landingZoneJobId, JobType.AzureLandingZoneResult, createRequest.projectName.value)
  }

  it should "wrap exceptions thrown by synchronous calls in a Future" in {
    val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      None,
      None,
      Some(coords)
    )
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

  it should "handle landing zone creation errors" in {
    val coords = AzureManagedAppCoordinates(UUID.randomUUID, UUID.randomUUID, "fake")
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      None,
      None,
      Some(coords)
    )
    val repo = mock[BillingRepository]
    val bpm = mock[BillingProfileManagerDAO]
    val profileModel = new ProfileModel().id(UUID.randomUUID())

    val landingZoneDefinition = "fake-landing-zone-definition"
    val landingZoneVersion = "fake-landing-zone-version"
    val azConfig: AzureConfig = AzureConfig(
      "fake-alpha-feature-group",
      "eastus",
      landingZoneDefinition,
      landingZoneVersion
    )
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
      workspaceManagerDAO.createLandingZone(landingZoneDefinition, landingZoneVersion, profileModel.getId, testContext)
    ).thenReturn(
      new CreateLandingZoneResult().errorReport(new ErrorReport().statusCode(500).message(landingZoneErrorMessage))
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
    }
  }

  behavior of "preDeletionSteps"

  it should "error if the landing zone is still being created" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")

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

  it should "succeed if the landing zone billing profiles id do not exist" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")

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
    val billingProjectName = RawlsBillingProjectName("fake_name")
    val landingZoneId = UUID.randomUUID()

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
    val billingProjectName = RawlsBillingProjectName("fake_name")
    val landingZoneId = UUID.randomUUID()
    val landingZoneErrorMessage = "error from deleting landing zone"

    val repo = mock[BillingRepository]
    when(repo.getCreationStatus(billingProjectName)).thenReturn(Future.successful(CreationStatuses.Ready))
    when(repo.getLandingZoneId(billingProjectName)).thenReturn(Future.successful(Some(landingZoneId.toString)))

    val bpm = mock[BillingProfileManagerDAO]
    val workspaceManagerDAO = mock[HttpWorkspaceManagerDAO]
    when(workspaceManagerDAO.deleteLandingZone(landingZoneId, testContext)).thenReturn(
      new DeleteAzureLandingZoneResult().errorReport(new ErrorReport().statusCode(500).message(landingZoneErrorMessage))
    )
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

  it should "delete the billing profile if no other project references it" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")

    val repo = mock[BillingRepository]
    val billingProfileId = UUID.randomUUID()
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

    verify(bpm, Mockito.times(1)).deleteBillingProfile(billingProfileId, testContext)
  }

  it should "not delete the billing profile if other projects reference it" in {
    val billingProjectName = RawlsBillingProjectName("fake_name")

    val repo = mock[BillingRepository]
    val billingProfileId = UUID.randomUUID()
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
