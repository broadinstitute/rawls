package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.{AzureManagedAppModel, ProfileModel}
import bio.terra.workspace.model.{AzureLandingZoneResult, ErrorReport, JobReport}
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
  RawlsBillingProjectName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BpmBillingProjectCreatorSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  val testContext = RawlsRequestContext(userInfo)

  behavior of "validateBillingProjectCreationRequest"

  it should "fail when provided GCP billing info" in {
    val bp = new BpmBillingProjectCreator(mock[BillingRepository],
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

    val bp = new BpmBillingProjectCreator(mock[BillingRepository],
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

    val bp = new BpmBillingProjectCreator(mock[BillingRepository],
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
    val bp = new BpmBillingProjectCreator(mock[BillingRepository],
                                          bpm,
                                          mock[HttpWorkspaceManagerDAO],
                                          mock[WorkspaceManagerResourceMonitorRecordDao]
    )

    Await.result(bp.validateBillingProjectCreationRequest(createRequest, testContext), Duration.Inf)
  }

  behavior of "postCreationSteps"

  it should "store the landing zone creation job ID and link the profile ID to the billing project record" in {
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
    ).thenReturn(new AzureLandingZoneResult().jobReport(new JobReport().id(landingZoneJobId.toString)))
    when(repo.setBillingProfileId(createRequest.projectName, profileModel.getId)).thenReturn(Future.successful(1))

    val wsmResouceRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao]
    when(wsmResouceRecordDao.create(landingZoneJobId, JobType.AzureLandingZoneResult, createRequest.projectName.value))
      .thenReturn(Future.successful())
    val bp = new BpmBillingProjectCreator(repo, bpm, workspaceManagerDAO, wsmResouceRecordDao)

    assertResult(CreationStatuses.CreatingLandingZone) {
      Await.result(bp.postCreationSteps(
                     createRequest,
                     new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
                     testContext
                   ),
                   Duration.Inf
      )
    }
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

    val bp = new BpmBillingProjectCreator(mock[BillingRepository],
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
      new AzureLandingZoneResult().errorReport(new ErrorReport().statusCode(500).message(landingZoneErrorMessage))
    )

    val bp =
      new BpmBillingProjectCreator(repo, bpm, workspaceManagerDAO, mock[WorkspaceManagerResourceMonitorRecordDao])

    val result = bp.postCreationSteps(
      createRequest,
      new MultiCloudWorkspaceConfig(true, None, Some(azConfig)),
      testContext
    )
    ScalaFutures.whenReady(result.failed) { exception =>
      exception.getMessage.contains(landingZoneErrorMessage)
    }
  }
}
