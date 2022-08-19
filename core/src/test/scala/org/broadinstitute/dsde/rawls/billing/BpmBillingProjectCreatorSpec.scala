package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.model.{AzureManagedAppModel, ProfileModel}
import cromwell.client.ApiException
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, CreateRawlsV2BillingProjectFullRequest, RawlsBillingAccountName, RawlsBillingProjectName, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BpmBillingProjectCreatorSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val userInfo: UserInfo = UserInfo(
    RawlsUserEmail("fake@example.com"),
    OAuth2BearerToken("fake_token"),
    0,
    RawlsUserSubjectId("sub"),
    None)

  behavior of "validateBillingProjectCreationRequest"

  it should "fail when provided GCP billing info" in {
    val bp = new BpmBillingProjectCreator(mock[BillingRepository], mock[BillingProfileManagerDAO])
    val createRequest = CreateRawlsV2BillingProjectFullRequest(
      RawlsBillingProjectName("fake_name"),
      Some(RawlsBillingAccountName("fake_billing_account_name")),
      None,
      None
    )

    intercept[NotImplementedError] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, userInfo), Duration.Inf)
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
    when(bpm.listManagedApps(ArgumentMatchers.eq(coords.subscriptionId), ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(Seq()))
    val bp = new BpmBillingProjectCreator(mock[BillingRepository], bpm)

    intercept[ManagedAppNotFoundException] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, userInfo), Duration.Inf)
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
    when(bpm.listManagedApps(ArgumentMatchers.eq(coords.subscriptionId), ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.failed(new ApiException("failed")))

    val bp = new BpmBillingProjectCreator(mock[BillingRepository], bpm)

    intercept[ApiException] {
      Await.result(bp.validateBillingProjectCreationRequest(createRequest, userInfo), Duration.Inf)
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
    when(bpm.listManagedApps(ArgumentMatchers.eq(coords.subscriptionId), ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(Seq(
        new AzureManagedAppModel().subscriptionId(coords.subscriptionId)
          .managedResourceGroupId(coords.managedResourceGroupId)
          .tenantId(coords.tenantId)))
      )
    val bp = new BpmBillingProjectCreator(mock[BillingRepository], bpm)

    Await.result(bp.validateBillingProjectCreationRequest(createRequest, userInfo), Duration.Inf)
  }

  behavior of "postCreationSteps"

  it should "link the profile ID to the billing project record" in {
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
    when(bpm.createBillingProfile(
      ArgumentMatchers.eq(createRequest.projectName.value),
      ArgumentMatchers.eq(createRequest.billingInfo),
      ArgumentMatchers.eq(userInfo)))
      .thenReturn(Future.successful(profileModel))
    when(repo.setBillingProfileId(
      createRequest.projectName,
      profileModel.getId)).thenReturn(Future.successful(1))
    val bp = new BpmBillingProjectCreator(repo, bpm)

    bp.postCreationSteps(createRequest, userInfo)
  }
}
