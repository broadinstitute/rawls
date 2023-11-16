package org.broadinstitute.dsde.rawls.consumer

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody
import au.com.dius.pact.consumer.dsl._
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import bio.terra.profile.client.ApiClient
import bio.terra.profile.model.{CloudPlatform, SystemStatus, SystemStatusSystems}
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAOImpl, HttpBillingProfileManagerClientProvider}
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.consumer.PactHelper.{
  buildInteraction,
  jsonRequestHeaders,
  jsonRequestHeadersWithBody,
  jsonResponseHeaders
}
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pact4s.scalatest.RequestResponsePactForger

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.function.Consumer
import scala.concurrent.ExecutionContext

class BpmClientSpec extends AnyFlatSpec with Matchers with RequestResponsePactForger {
  /*
    Define the folder that the pact contracts get written to upon completion of this test suite.
   */
  override val pactTestExecutionContext: PactTestExecutionContext =
    new PactTestExecutionContext(
      "./target/pacts"
    )

  private val subSystems = List("CloudSQL", "Sam")
  val subSystemStatus = new java.util.HashMap[String, SystemStatusSystems]()
  for (s <- subSystems) subSystemStatus.put(s, new SystemStatusSystems().ok(true))
  val okSystemStatus: SystemStatus = new SystemStatus().ok(true).systems(subSystemStatus)

  private val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
  private val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig.apply(conf)

  // The actual values of these are not significant.
  // They will be replaced by provider-side values when the pact is verified
  private val dummySubscriptionId = UUID.fromString("4a5afeaa-b3b2-fa51-8e4e-9dbf294b7837")
  private val dummyTenantId = UUID.fromString("1a5afeaa-b3b2-fa51-8e4e-9dbf294b7837")
  private val dummyMrgId = "dummyMrgId"
  private val dummyBillingProfileId = UUID.fromString("9a5afeaa-b3b2-fa51-8e4e-9dbf294b7837")
  private val dummyPolicy = ProfilePolicy.User
  val dummyDate = DateTime.parse("2020-01-30T16:58:56.389Z").toDate

  private val managedAppCoordinates = AzureManagedAppCoordinates(dummyTenantId, dummySubscriptionId, dummyMrgId)

  private val userInfo: UserInfo =
    UserInfo(RawlsUserEmail("fake@example.com"), OAuth2BearerToken("fake_token"), 0, RawlsUserSubjectId("sub"), None)
  private val testContext = RawlsRequestContext(userInfo)

  // --- End of fixtures section

  // ---- Dsl for specifying pacts between consumer and provider
  // Lambda Dsl: required for generating matching rules.
  // Favored over old-style Pact Dsl using PactDslJsonBody.
  val okSystemStatusResponse: DslPart = newJsonBody { o =>
    o.booleanType("ok", true)
    o.`object`("systems",
               s =>
                 for (subsystem <- subSystems)
                   s.`object`(subsystem, o => o.booleanType("ok", true))
    )
  }.build()

  val managedAppsExistResponse: DslPart = newJsonBody { o =>
    o.`array`(
      "managedApps",
      a =>
        a.`object` { ao =>
          ao.stringType("applicationDeploymentName")
          ao.stringType("managedResourceGroupId")
          ao.uuid("subscriptionId")
            .valueFromProviderState("subscriptionId", "${subscriptionId}", dummySubscriptionId.toString)
          ao.uuid("tenantId")
          ao.booleanType("assigned", false)
          ao.stringType("region")
        }
    )
  }.build()

  val profileRequestBody: DslPart = newJsonBody { o =>
    o.uuid("id")
    o.uuid("subscriptionId")
    o.uuid("tenantId")
    o.stringType("managedResourceGroupId")
    o.stringType("biller")
    o.stringType("displayName")
    // It appears to be necessary to specify the value because the enum doesn't get translated to string automatically
    o.stringType("cloudPlatform", CloudPlatform.AZURE.toString)
    o.`object`("policies",
      po =>
        po.array("inputs",
          pio =>
            pio.`object` { p =>
              p.stringType("namespace")
              p.stringType("name")
              p.array("additionalData",
                pad =>
                  pad.`object` { ad =>
                    ad.stringType("key")
                    ad.stringType("value")
                  }
              )
            }
        )
    )
  }.build()

  val profileModelResponse: DslPart = newJsonBody { o =>
    o.uuid("id").valueFromProviderState("id", "${azureProfileId}", dummyBillingProfileId.toString)
    o.uuid("subscriptionId").valueFromProviderState("subscriptionId", "${subscriptionId}", dummySubscriptionId.toString)
    o.uuid("tenantId").valueFromProviderState("tenantId", "${tenantId}", dummyTenantId.toString)
    o.stringType("managedResourceGroupId")
      .valueFromProviderState("managedResourceGroupId", "${managedResourceGroupId}", dummyMrgId)
    o.stringType("biller")
    o.stringType("displayName")
    o.stringType("cloudPlatform", CloudPlatform.AZURE.toString)
    o.`object`("policies",
      po =>
        po.array("inputs",
          pio =>
            pio.`object` { p =>
              p.stringType("namespace")
              p.stringType("name")
              p.array("additionalData",
                pad =>
                  pad.`object` { ad =>
                    ad.stringType("key")
                    ad.stringType("value")
                  }
              )
            }
        )
    )
  }.build()

  val minimalProfile: Consumer[LambdaDslObject] = o => {
    o.uuid("id")
    o.stringType("cloudPlatform")
  }

  val allProfilesModelResponse: DslPart = newJsonBody { o =>
    o.`minArrayLike`(
      "items",
      2,
      minimalProfile
    )
  }.build()

  val policyMemberRequestBody: DslPart = newJsonBody { o =>
    o.stringType("email")
  }.build()

  val policyMemberResponse: DslPart = new PactDslJsonBody()
    .stringType("name")
    .valueFromProviderState("name", "${policyName}", dummyPolicy.toString)
    .array("members")
    // couldn't figure out how to do this in lambda DSL
    .valueFromProviderState("${userEmail}", userInfo.userEmail.value)
    .closeArray()

  val emptyPolicyMemberResponse: DslPart = new PactDslJsonBody()
    .stringType("name")
    .valueFromProviderState("name", "${policyName}", dummyPolicy.toString)
    .array("members")
    .closeArray()

  val spendReportResponse: DslPart = newJsonBody { o =>
    o.`array`(
      "spendDetails",
      a =>
        a.`object` { ao =>
          ao.stringType("aggregationKey", "Category")
          ao.`array`(
            "spendData",
            sp =>
              sp.`object` { spo =>
                spo.stringType("category")
                spo.stringType("cost")
                spo.stringType("credits")
                spo.stringType("currency")
              }
          )
        }
    )
    o.`object`(
      "spendSummary",
      { so =>
        so.stringType("startTime")
          .valueFromProviderState("startTime", "${startTime}", new ApiClient().formatDate(dummyDate))
        so.stringType("endTime").valueFromProviderState("endTime", "${endTime}", new ApiClient().formatDate(dummyDate))
        so.stringType("cost")
        so.stringType("credits")
        so.stringType("currency")
      }
    )
  }.build()

  val consumerPactBuilder: ConsumerPactBuilder = ConsumerPactBuilder.consumer("rawls")

  val pactProvider: PactDslWithProvider = consumerPactBuilder.hasPactWith("bpm")

  // stateParams provides the desired subsystem states
  // for BPM provider to generate the expected response
  var pactDslResponse: PactDslResponse = buildInteraction(
    pactProvider,
    state = "BPM is ok",
    stateParams = subSystems.map(s => s -> "ok").toMap,
    uponReceiving = "Request to BPM ok status",
    method = "GET",
    path = "/status",
    status = 200,
    okSystemStatusResponse
  )

  pactDslResponse = pactDslResponse
    .`given`("a managed app exists")
    .uponReceiving("Request to list managed apps")
    .method("GET")
    .path("/api/azure/v1/managedApps")
    .headers(jsonRequestHeaders)
    .query("includeAssignedApplications=false")
    .queryParameterFromProviderState("azureSubscriptionId", "${subscriptionId}", dummySubscriptionId.toString)
    .willRespondWith()
    .status(200)
    .headers(jsonResponseHeaders)
    .body(managedAppsExistResponse)

  pactDslResponse = pactDslResponse
    .`given`("a JobService that supports profile creation")
    .uponReceiving("Request to create a billing profile")
    .method("POST")
    .path("/api/profiles/v1")
    .headers(jsonRequestHeadersWithBody)
    .body(profileRequestBody)
    .willRespondWith()
    .status(201)
    .headers(jsonResponseHeaders)
    .body(profileModelResponse)

  pactDslResponse = pactDslResponse
    .`given`("an Azure billing profile")
    .`given`("a JobService that supports profile deletion")
    .uponReceiving("Request to delete a billing profile")
    .method("DELETE")
    .pathFromProviderState("/api/profiles/v1/${azureProfileId}", s"/api/profiles/v1/${dummyBillingProfileId}")
    .willRespondWith()
    .status(204)

  pactDslResponse = pactDslResponse
    .`given`("an Azure billing profile")
    .uponReceiving("Request to return billing profile")
    .method("GET")
    .pathFromProviderState("/api/profiles/v1/${azureProfileId}", s"/api/profiles/v1/${dummyBillingProfileId}")
    .headers(jsonRequestHeaders)
    .willRespondWith()
    .status(200)
    .headers(jsonResponseHeaders)
    .body(profileModelResponse)

  pactDslResponse = pactDslResponse
    .`given`("two billing profiles exist")
    .uponReceiving("Request to return all billing profiles")
    .method("GET")
    .path("/api/profiles/v1")
    .query("offset=0&limit=1000")
    .headers(jsonRequestHeaders)
    .willRespondWith()
    .status(200)
    .headers(jsonResponseHeaders)
    .body(allProfilesModelResponse)

  pactDslResponse = pactDslResponse
    .`given`("an Azure billing profile")
    .`given`("a Sam service that supports profile policy member addition")
    .uponReceiving("Request to add a profile policy member")
    .method("POST")
    .pathFromProviderState(
      "/api/profiles/v1/${azureProfileId}/policies/${policyName}/members",
      s"/api/profiles/v1/${dummyBillingProfileId}/policies/${dummyPolicy.toString}/members"
    )
    .headers(jsonRequestHeadersWithBody)
    .body(policyMemberRequestBody)
    .willRespondWith()
    .status(201)
    .headers(jsonResponseHeaders)
    .body(policyMemberResponse)

  pactDslResponse = pactDslResponse
    .`given`("an Azure billing profile")
    .`given`("a Sam service that supports profile policy member deletion")
    .uponReceiving("Request to delete a profile policy member")
    .method("DELETE")
    .pathFromProviderState(
      "/api/resources/v1/profiles/${azureProfileId}/policies/${policyName}/members/${userEmail}",
      s"/api/resources/v1/profiles/${dummyBillingProfileId}/policies/${dummyPolicy.toString}/members/${URLEncoder
          .encode(userInfo.userEmail.value, UTF_8.name)}"
    )
    .headers(jsonRequestHeaders)
    .willRespondWith()
    .status(200)
    .headers(jsonResponseHeaders)
    .body(emptyPolicyMemberResponse)

  pactDslResponse = pactDslResponse
    .`given`("an Azure billing profile")
    .`given`("an Azure spend report service exists")
    .uponReceiving("Request for spend report")
    .method("GET")
    .pathFromProviderState("/api/profiles/v1/${azureProfileId}/spendReport",
                           s"/api/profiles/v1/${dummyBillingProfileId}/spendReport"
    )
    .headers(jsonRequestHeaders)
    .queryParameterFromProviderState("spendReportStartDate", "${startTime}", new ApiClient().formatDate(dummyDate))
    .queryParameterFromProviderState("spendReportEndDate", "${endTime}", new ApiClient().formatDate(dummyDate))
    .willRespondWith()
    .status(200)
    .headers(jsonResponseHeaders)
    .body(spendReportResponse)

  override val pact: RequestResponsePact = pactDslResponse.toPact

  it should "get BPM ok status" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    val systemStatus = billingProfileManagerDAO.getStatus()
    systemStatus.isOk shouldBe true
  }

  it should "list managed apps" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    val managedApps = billingProfileManagerDAO.listManagedApps(dummySubscriptionId, false, testContext)
    managedApps.length shouldBe 1
    managedApps.headOption.get.isAssigned shouldBe false
  }

  it should "return the proper response from creating a billing project" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    val profileModel =
      billingProfileManagerDAO.createBillingProfile("billingProfile", Right(managedAppCoordinates), Map[String, Map[String, String]](), testContext)
    profileModel.getSubscriptionId shouldBe dummySubscriptionId
    profileModel.getTenantId shouldBe dummyTenantId
    profileModel.getManagedResourceGroupId shouldBe dummyMrgId
    profileModel.getCloudPlatform shouldBe CloudPlatform.AZURE
  }

  it should "return the proper response from deleting a billing project" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    billingProfileManagerDAO.deleteBillingProfile(dummyBillingProfileId, testContext)
  }

  it should "return an existing Azure billing profiles" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    val profileModel = billingProfileManagerDAO.getBillingProfile(dummyBillingProfileId, testContext);
    profileModel.get.getId shouldBe dummyBillingProfileId
    profileModel.get.getSubscriptionId shouldBe dummySubscriptionId
    profileModel.get.getCloudPlatform shouldBe CloudPlatform.AZURE
  }

  it should "return all billing profiles" in {
    implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    for {
      billingProfiles <- billingProfileManagerDAO.getAllBillingProfiles(testContext)
    } yield billingProfiles.length shouldBe 2
  }

  it should "allow the addition of a profile policy member" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    billingProfileManagerDAO.addProfilePolicyMember(dummyBillingProfileId,
                                                    dummyPolicy,
                                                    userInfo.userEmail.value,
                                                    testContext
    )
  }

  it should "allow the deletion of a profile policy member" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    billingProfileManagerDAO.deleteProfilePolicyMember(dummyBillingProfileId,
                                                       dummyPolicy,
                                                       userInfo.userEmail.value,
                                                       testContext
    )
  }

  it should "return a spend report" in {
    val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
      new HttpBillingProfileManagerClientProvider(Some(mockServer.getUrl)),
      multiCloudWorkspaceConfig
    )
    val spendReport =
      billingProfileManagerDAO.getAzureSpendReport(dummyBillingProfileId, dummyDate, dummyDate, testContext)
    spendReport.getSpendDetails.isEmpty shouldBe false
  }
}
