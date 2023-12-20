package org.broadinstitute.dsde.rawls.billing

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.api.{AzureApi, ProfileApi, SpendReportingApi}
import bio.terra.profile.client.ApiException
import bio.terra.profile.model._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.billing.BpmAzureReportErrorMessageJsonProtocol._
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  RawlsBillingAccountName,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar
import spray.json._

import java.util.{Date, UUID}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters.SeqHasAsJava

class BillingProfileManagerDAOSpec extends AnyFlatSpec with MockitoSugar with MockitoTestUtils {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  val azConfig: AzureConfig = AzureConfig(
    "fake-landing-zone-definition",
    "fake-protected-landing-zone-definition",
    "fake-landing-zone-version",
    Map("fake_parameter" -> "fake_value"),
    landingZoneAllowAttach = false
  )
  val userInfo: UserInfo = UserInfo(
    RawlsUserEmail("fake@example.com"),
    OAuth2BearerToken("fake_token"),
    0,
    RawlsUserSubjectId("sub"),
    None
  )
  val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)
  val multiCloudWorkspaceConfig: MultiCloudWorkspaceConfig = MultiCloudWorkspaceConfig(
    MultiCloudWorkspaceManagerConfig("fake_app_id", Duration(1, "second"), Duration(1, "second")),
    azConfig
  )

  behavior of "getBillingProfile"

  it should "return the billing profile if present" in {
    val profileId = UUID.randomUUID()
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val apiProvider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    when(apiProvider.getProfileApi(any())).thenReturn(profileApi)
    when(profileApi.getProfile(any())).thenReturn(new ProfileModel().id(profileId))
    val billingProfileManagerDAO =
      new BillingProfileManagerDAOImpl(apiProvider, multiCloudWorkspaceConfig)

    val result = billingProfileManagerDAO.getBillingProfile(profileId, testContext)

    result.get.getId shouldBe profileId
  }

  it should "return None if not present" in {
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val apiProvider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    when(apiProvider.getProfileApi(any())).thenReturn(profileApi)
    when(profileApi.getProfile(any())).thenThrow(new ApiException(StatusCodes.NotFound.intValue, "not found"))
    val billingProfileManagerDAO =
      new BillingProfileManagerDAOImpl(apiProvider, multiCloudWorkspaceConfig)

    val result = billingProfileManagerDAO.getBillingProfile(UUID.randomUUID(), testContext)

    result shouldBe None
  }

  it should "throw an exception for other errors" in {
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val apiProvider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    when(apiProvider.getProfileApi(any())).thenReturn(profileApi)
    when(profileApi.getProfile(any())).thenThrow(new ApiException(StatusCodes.BadRequest.intValue, "bad request"))
    val billingProfileManagerDAO =
      new BillingProfileManagerDAOImpl(apiProvider, multiCloudWorkspaceConfig)

    intercept[BpmException] {
      billingProfileManagerDAO.getBillingProfile(UUID.randomUUID(), testContext)
    }
  }

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
      new BillingProfileManagerDAOImpl(apiProvider, multiCloudWorkspaceConfig)

    val result = Await.result(billingProfileManagerDAO.getAllBillingProfiles(testContext), Duration.Inf)

    result.length should be(BillingProfileManagerDAO.BillingProfileRequestBatchSize + 1)
  }

  behavior of "createBillingProfile"

  it should "fail when provided with Google billing account information" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, multiCloudWorkspaceConfig)

    intercept[NotImplementedError] {
      bpmDAO.createBillingProfile("fake", Left(RawlsBillingAccountName("fake")), Map.empty, testContext)
    }
  }

  it should "create the profile in billing profile manager" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val expectedProfile = new ProfileModel().id(UUID.randomUUID())
    val coords = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg")
    when(profileApi.createProfile(ArgumentMatchers.any[CreateProfileRequest])).thenReturn(expectedProfile)
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, multiCloudWorkspaceConfig)

    val profile = bpmDAO.createBillingProfile("fake", Right(coords), Map.empty, testContext)

    assertResult(expectedProfile)(profile)
    verify(profileApi, times(1)).createProfile(ArgumentMatchers.any[CreateProfileRequest])
  }

  it should "include an empty set of policy inputs if no policies are present" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val dummyProfile = new ProfileModel().id(UUID.randomUUID())
    when(profileApi.createProfile(ArgumentMatchers.any[CreateProfileRequest])).thenReturn(dummyProfile)
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)
    val coords = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg")
    val policies = Map[String, List[(String, String)]]()
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, multiCloudWorkspaceConfig)

    val createProfileRequestCaptor = captor[CreateProfileRequest]
    bpmDAO.createBillingProfile("fake", Right(coords), policies, testContext)

    val expectedPolicies = new BpmApiPolicyInputs().inputs(
      List.empty.asJava
    )

    verify(profileApi).createProfile(createProfileRequestCaptor.capture)
    assertResult(expectedPolicies)(createProfileRequestCaptor.getValue.getPolicies)
  }

  it should "include any policies that are present" in {
    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val dummyProfile = new ProfileModel().id(UUID.randomUUID())
    when(profileApi.createProfile(ArgumentMatchers.any[CreateProfileRequest])).thenReturn(dummyProfile)
    when(provider.getProfileApi(ArgumentMatchers.eq(testContext))).thenReturn(profileApi)
    val coords = AzureManagedAppCoordinates(UUID.randomUUID(), UUID.randomUUID(), "fake_mrg")
    val policies = Map("protected-data" -> List[(String, String)](),
                       "group-constraint" -> List(("group", "myFakeGroup"), ("group", "myOtherFakeGroup"))
    )
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, multiCloudWorkspaceConfig)

    val createProfileRequestCaptor = captor[CreateProfileRequest]
    bpmDAO.createBillingProfile("fake", Right(coords), policies, testContext)

    val expectedPolicies = new BpmApiPolicyInputs().inputs(
      List(
        new BpmApiPolicyInput().namespace("terra").name("protected-data").additionalData(List.empty.asJava),
        new BpmApiPolicyInput()
          .namespace("terra")
          .name("group-constraint")
          .additionalData(
            List(new BpmApiPolicyPair().key("group").value("myFakeGroup"),
                 new BpmApiPolicyPair().key("group").value("myOtherFakeGroup")
            ).asJava
          )
      ).asJava
    )

    verify(profileApi).createProfile(createProfileRequestCaptor.capture)
    assertResult(expectedPolicies)(createProfileRequestCaptor.getValue.getPolicies)
  }

  behavior of "deleteBillingProfile"

  it should "call BPM's delete method" in {
    val profileId = UUID.randomUUID()

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val profileApi = mock[ProfileApi](RETURNS_SMART_NULLS)
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      multiCloudWorkspaceConfig
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
    val bpmDAO = new BillingProfileManagerDAOImpl(provider, multiCloudWorkspaceConfig)

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
      new BillingProfileManagerDAOImpl(apiProvider, multiCloudWorkspaceConfig)

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
      multiCloudWorkspaceConfig
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
      multiCloudWorkspaceConfig
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
      multiCloudWorkspaceConfig
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

  it should "handle/rethrow BPM client exceptions" in {
    val billingProfileId = UUID.randomUUID();
    val startDate = DateTime.now().minusMonths(2)
    val endDate = startDate.plusMonths(1)

    val provider = mock[BillingProfileManagerClientProvider](RETURNS_SMART_NULLS)
    val spendReportingApi = mock[SpendReportingApi](RETURNS_SMART_NULLS)

    when(provider.getSpendReportingApi(ArgumentMatchers.eq(testContext))).thenReturn(spendReportingApi)
    val bpmErrorMessage = "something went wrong."
    val exceptionMessage =
      s"{\"message\":\"${bpmErrorMessage}\",\"statusCode\":400,\"causes\":[]}"
    val spendReportApiException = new ApiException(StatusCodes.TooManyRequests.intValue, exceptionMessage)
    when(spendReportingApi.getSpendReport(any(), any(), any())).thenThrow(spendReportApiException)

    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      provider,
      multiCloudWorkspaceConfig
    )

    // should throw exception
    val e = intercept[BpmAzureSpendReportApiException] {
      billingProfileManagerDAO.getAzureSpendReport(billingProfileId, startDate.toDate, endDate.toDate, testContext)
    }

    e shouldNot equal(null)
    e.getMessage shouldBe bpmErrorMessage
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
