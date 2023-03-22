package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.api.{AzureApi, ProfileApi, SpendReportingApi}
import bio.terra.profile.model._
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig}
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ProjectRoles,
  RawlsBillingAccountName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamResourceAction,
  SamResourceTypeNames,
  UserInfo
}
import org.joda.time.DateTime
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{key, _}
import org.scalatestplus.mockito.MockitoSugar
import spray.json.{enrichAny, JsObject, JsValue}

import java.util.{Date, UUID}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.SeqHasAsJava
import spray.json._
import BpmAzureReportErrorMessageJsonProtocol._
import akka.http.scaladsl.model.StatusCodes
import bio.terra.profile.client.ApiException
import org.mockito.ArgumentMatchers.any

class BillingProfileManagerDAOSpec extends AnyFlatSpec with MockitoSugar {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val azConfig: AzureConfig = AzureConfig(
    "fake-landing-zone-definition",
    "fake-landing-zone-version",
    Map("fake_parameter" -> "fake_value")
  )
  val userInfo: UserInfo = UserInfo(
    RawlsUserEmail("fake@example.com"),
    OAuth2BearerToken("fake_token"),
    0,
    RawlsUserSubjectId("sub"),
    None
  )
  val testContext = RawlsRequestContext(userInfo)

  behavior of "getAllBillingProfiles"

  it should "return all profiles from listBillingProfiles when the profiles exceeds the request batch size" in {
    def constructProfileList(n: Int): ProfileModelList =
      new ProfileModelList()
        .items((0 until n).map(_ => new ProfileModel()).asJava)
        .total(n)

    val profileApi = mock[ProfileApi]
    when(profileApi.listProfiles(ArgumentMatchers.eq(0), ArgumentMatchers.any()))
      .thenReturn(constructProfileList(BillingProfileManagerDAO.BillingProfileRequestBatchSize))
    when(
      profileApi.listProfiles(ArgumentMatchers.eq(BillingProfileManagerDAO.BillingProfileRequestBatchSize),
                              ArgumentMatchers.any()
      )
    )
      .thenReturn(constructProfileList(1))

    val apiProvider = mock[BillingProfileManagerClientProvider]
    when(apiProvider.getProfileApi(ArgumentMatchers.any())).thenReturn(profileApi)

    val billingProfileManagerDAO =
      new BillingProfileManagerDAOImpl(apiProvider, MultiCloudWorkspaceConfig(true, None, Some(azConfig)))

    val result = Await.result(billingProfileManagerDAO.getAllBillingProfiles(testContext), Duration.Inf)

    result.length should be(BillingProfileManagerDAO.BillingProfileRequestBatchSize + 1)
  }

  behavior of "createBillingProfile"

  it should "fail when provided with Google billing account information" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val config = new MultiCloudWorkspaceConfig(true, None, None)
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, config)

    intercept[NotImplementedError] {
      bpmDAO.createBillingProfile("fake", Left(RawlsBillingAccountName("fake")), testContext)
    }
  }

  it should "create the profile in billing profile manager" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val expectedProfile = new ProfileModel().id(UUID.randomUUID())
    val coords = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg")
    when(profileApi.createProfile(ArgumentMatchers.any[CreateProfileRequest])).thenReturn(expectedProfile)
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)
    val config = new MultiCloudWorkspaceConfig(true, None, None)
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, config)

    val profile = bpmDAO.createBillingProfile("fake", Right(coords), testContext)

    assertResult(expectedProfile)(profile)
    verify(profileApi, times(1)).createProfile(ArgumentMatchers.any[CreateProfileRequest])
  }

  behavior of "deleteBillingProfile"

  it should "call BPM's delete method" in {
    val profileId = UUID.randomUUID()

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)

    billingProfileManagerDAO.deleteBillingProfile(profileId, testContext)
    verify(profileApi).deleteProfile(profileId)
  }

  behavior of "listManagedApps"

  it should "return the list of managed apps from billing profile manager" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val azureApi = mock[AzureApi](RETURNS_SMART_NULLS)
    val subscriptionId = UUID.randomUUID()
    val expectedApp = new AzureManagedAppModel().subscriptionId(subscriptionId)
    when(azureApi.getManagedAppDeployments(subscriptionId, true)).thenReturn(
      new AzureManagedAppsResponseModel().managedApps(
        java.util.List.of(
          expectedApp
        )
      )
    )
    when(provider.getAzureApi(ArgumentMatchers.eq(testContext))).thenReturn(azureApi)
    val config = new MultiCloudWorkspaceConfig(true, None, None)
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, config)

    val apps = bpmDAO.listManagedApps(subscriptionId, true, testContext)

    assertResult(Seq(expectedApp))(apps)
  }

  it should "return all profiles from listBillingProfiles when the profiles exceeds the request batch size" in {
    def constructProfileList(n: Int): ProfileModelList =
      new ProfileModelList()
        .items((0 until n).map(_ => new ProfileModel()).asJava)
        .total(n)

    val profileApi = mock[ProfileApi]
    when(profileApi.listProfiles(ArgumentMatchers.eq(0), ArgumentMatchers.any()))
      .thenReturn(constructProfileList(BillingProfileManagerDAO.BillingProfileRequestBatchSize))
    when(
      profileApi.listProfiles(ArgumentMatchers.eq(BillingProfileManagerDAO.BillingProfileRequestBatchSize),
                              ArgumentMatchers.any()
      )
    )
      .thenReturn(constructProfileList(1))

    val apiProvider = mock[BillingProfileManagerClientProvider]
    when(apiProvider.getProfileApi(ArgumentMatchers.any())).thenReturn(profileApi)

    val billingProfileManagerDAO =
      new BillingProfileManagerDAOImpl(apiProvider, MultiCloudWorkspaceConfig(true, None, Some(azConfig)))

    val result = Await.result(billingProfileManagerDAO.getAllBillingProfiles(testContext), Duration.Inf)

    result.length should be(BillingProfileManagerDAO.BillingProfileRequestBatchSize + 1)

  }

  behavior of "addProfilePolicyMember"

  it should "call BPM's add method with the correct policy names" in {
    val profileId = UUID.randomUUID()
    val memberEmail = "email@test.com"
    val memberRequest = new PolicyMemberRequest().email(memberEmail)

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)

    billingProfileManagerDAO.addProfilePolicyMember(profileId, ProfilePolicy.Owner, memberEmail, testContext)
    verify(profileApi).addProfilePolicyMember(memberRequest, profileId, "owner")

    reset(profileApi)

    billingProfileManagerDAO.addProfilePolicyMember(profileId, ProfilePolicy.User, memberEmail, testContext)
    verify(profileApi).addProfilePolicyMember(memberRequest, profileId, "user")
  }

  behavior of "deleteProfilePolicyMember"

  it should "call BPM's delete method with the correct policy names" in {
    val profileId = UUID.randomUUID()
    val memberEmail = "email@test.com"

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)

    billingProfileManagerDAO.deleteProfilePolicyMember(profileId, ProfilePolicy.Owner, memberEmail, testContext)
    verify(profileApi).deleteProfilePolicyMember(profileId, "owner", memberEmail)

    reset(profileApi)

    billingProfileManagerDAO.deleteProfilePolicyMember(profileId, ProfilePolicy.User, memberEmail, testContext)
    verify(profileApi).deleteProfilePolicyMember(profileId, "user", memberEmail)
  }

  behavior of "getAzureSpendReport"

  it should "invoke getSpendReport with correct parameters" in {
    val billingProfileId = UUID.randomUUID();
    val startDate = DateTime.now().minusMonths(2)
    val endDate = startDate.plusMonths(1)

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val spendReportingApi = mock[SpendReportingApi](RETURNS_SMART_NULLS)

    when(provider.getSpendReportingApi(ArgumentMatchers.eq(testContext))).thenReturn(spendReportingApi)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )

    billingProfileManagerDAO.getAzureSpendReport(billingProfileId, startDate.toDate, endDate.toDate, testContext)

    val billingProfileIdCapture: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    val startDateCapture: ArgumentCaptor[Date] = ArgumentCaptor.forClass(classOf[Date])
    val endDateCapture: ArgumentCaptor[Date] = ArgumentCaptor.forClass(classOf[Date])
    verify(spendReportingApi, times(1)).getSpendReport(billingProfileIdCapture.capture(),
                                                       startDateCapture.capture(),
                                                       endDateCapture.capture()
    )
    billingProfileIdCapture.getValue shouldBe billingProfileId
    startDateCapture.getValue shouldBe startDate.toDate
    endDateCapture.getValue shouldBe endDate.toDate
  }

  it should "rethrow any non client exceptions" in {
    val billingProfileId = UUID.randomUUID();
    val startDate = DateTime.now().minusMonths(2)
    val endDate = startDate.plusMonths(1)

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val spendReportingApi = mock[SpendReportingApi](RETURNS_SMART_NULLS)

    when(provider.getSpendReportingApi(ArgumentMatchers.eq(testContext))).thenReturn(spendReportingApi)
    val exceptionMessage = "too many request. please retry later."
    val spendReportApiException = new ApiException(StatusCodes.TooManyRequests.intValue, exceptionMessage)
    when(spendReportingApi.getSpendReport(any(), any(), any())).thenThrow(spendReportApiException)

    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )

    // should throw exception
    val e = intercept[BpmAzureSpendReportApiException] {
      billingProfileManagerDAO.getAzureSpendReport(billingProfileId, startDate.toDate, endDate.toDate, testContext)
    }

    e shouldNot equal(null)
    e.getMessage shouldBe exceptionMessage
  }

  it should "rethrow any runtime exceptions" in {
    val billingProfileId = UUID.randomUUID();
    val startDate = DateTime.now().minusMonths(2)
    val endDate = startDate.plusMonths(1)

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val spendReportingApi = mock[SpendReportingApi](RETURNS_SMART_NULLS)

    when(provider.getSpendReportingApi(ArgumentMatchers.eq(testContext))).thenReturn(spendReportingApi)
    val exceptionMessage = "something went wrong"
    val spendReportApiException = new RuntimeException(exceptionMessage)
    when(spendReportingApi.getSpendReport(any(), any(), any())).thenThrow(spendReportApiException)

    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      MultiCloudWorkspaceConfig(true, None, Some(azConfig))
    )

    // should throw exception
    val e = intercept[BpmAzureSpendReportApiException] {
      billingProfileManagerDAO.getAzureSpendReport(billingProfileId, startDate.toDate, endDate.toDate, testContext)
    }

    e shouldNot equal(null)
    e.getMessage shouldBe exceptionMessage
  }

  behavior of "BpmAzureReportErrorMessageJsonProtocol"

  it should "deserialize json into BpmAzureReportErrorMessage" in {
    val bpmError = BpmAzureReportErrorMessage("customError", 400).toJson
    val value = bpmError.convertTo[BpmAzureReportErrorMessage]

    value shouldNot equal(null)
    value.message shouldBe "customError"
    value.statusCode shouldBe 400
  }

  it should "convert raw json into BpmAzureReportErrorMessage" in {
    val bpmErrorJsonString =
      "{\"message\":\"End date should be greater than start date.\",\"statusCode\":400,\"causes\":[]}"
    val bpmErrorJson = bpmErrorJsonString.parseJson

    val bpmError = bpmErrorJson.convertTo[BpmAzureReportErrorMessage]

    bpmError shouldNot equal(null)
    bpmError.message shouldBe "End date should be greater than start date."
    bpmError.statusCode shouldBe 400
  }
}
