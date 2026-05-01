package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import cats.effect.IO
import com.google.api.Metric
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.compute.Compute
import com.google.api.services.compute.model.Region
import com.google.cloud.monitoring.v3.MetricServiceClient.ListTimeSeriesPagedResponse
import com.google.cloud.storage.{Cors, HttpMethod, StorageClass}
import com.google.monitoring.v3.{Point, TimeInterval, TimeSeries, TypedValue}
import com.google.protobuf.Timestamp
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO._
import org.broadinstitute.dsde.rawls.google.GoogleUtilities
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented.GoogleCounters
import org.broadinstitute.dsde.rawls.model.{
  BucketMetric,
  GoogleProjectId,
  RawlsBillingAccount,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{times, verify, when, RETURNS_SMART_NULLS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.StringReader
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.jdk.CollectionConverters._

class HttpGoogleServicesDAOSpec extends AnyFlatSpec with Matchers with MockitoTestUtils {

  private def await[T](f: Future[T]): T = Await.result(f, 5 minutes)

  implicit val mockActorSystem: ActorSystem = ActorSystem("HttpGoogleServicesDAOSpec")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val httpGoogleServicesDao = new MockHttpGoogleServicesDAO(
    GoogleClientSecrets.load(GsonFactory.getDefaultInstance, new StringReader("{}")),
    "fakeClientEmail",
    "fakeSubEmail",
    "fakePemFile",
    "fakeAppsDomain",
    "fakeGroupPrefix",
    "fakeAppName",
    "fakeServiceProject",
    "fakeBillingPemEmail",
    "fakeBillingPemFile",
    "fakeBillingEmail",
    "fakeBillingGroupEmail",
    "fakeCredentialsJson",
    "fakeResourceBufferJsonFile"
  )

  behavior of "getUserCredential"

  it should "get a credential for a Google user" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"),
                            OAuth2BearerToken("some-token"),
                            300,
                            RawlsUserSubjectId("193481341723041"),
                            None
    )
    val cred = getUserCredential(userInfo)
    cred shouldBe defined
    cred.get.getExpiresInSeconds.toInt should (be >= 0 and be <= 300)
  }

  it should "get a credential for a Google user through B2C" in {
    val userInfo = UserInfo(
      RawlsUserEmail("fake@email.com"),
      OAuth2BearerToken("some-jwt"),
      300,
      RawlsUserSubjectId("704ef594-9669-45f4-b605-82b499065a49"),
      Some(OAuth2BearerToken("some-token"))
    )
    val cred = getUserCredential(userInfo)
    cred shouldBe defined
    cred.get.getAccessToken shouldBe "some-token"
    cred.get.getExpiresInSeconds.toInt should (be >= 0 and be <= 300)
  }

  it should "not get a credential for an Azure user through B2C" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"),
                            OAuth2BearerToken("some-jwt"),
                            300,
                            RawlsUserSubjectId("704ef594-9669-45f4-b605-82b499065a49"),
                            None
    )
    val cred = getUserCredential(userInfo)
    cred shouldBe None
  }

  behavior of "listBillingAccounts"

  it should "return open billing projects the user has access to, respecting firecloudHasAccess and handling Google API pagination" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"),
                            OAuth2BearerToken("some-token"),
                            300,
                            RawlsUserSubjectId("193481341723041"),
                            None
    )
    val billingAccountWithAccess =
      RawlsBillingAccount(httpGoogleServicesDao.accessibleBillingAccountName, true, "testBillingAccount")
    val billingAccountNoAccess =
      RawlsBillingAccount(httpGoogleServicesDao.inaccessibleBillingAccountName, false, "testBillingAccount")

    await(httpGoogleServicesDao.listBillingAccounts(userInfo)) shouldBe List(billingAccountWithAccess,
                                                                             billingAccountNoAccess
    )
    await(httpGoogleServicesDao.listBillingAccounts(userInfo, Some(true))) shouldBe List(billingAccountWithAccess)
    await(httpGoogleServicesDao.listBillingAccounts(userInfo, Some(false))) shouldBe List(billingAccountNoAccess)
  }

  behavior of "setupWorkspace"

  it should "create the workspace's GCS bucket" in {
    val userInfo = UserInfo(RawlsUserEmail("fake@email.com"),
                            OAuth2BearerToken("some-token"),
                            300,
                            RawlsUserSubjectId("193481341723041"),
                            None
    )

    val googleStorageService = mock[GoogleStorageService[IO]](RETURNS_SMART_NULLS)
    val googleProjectId = "project-id"
    val bucketName = GcsBucketName("fc-bucket-name")
    val expectedCorsPolicy = List(
      Cors
        .newBuilder()
        .setOrigins(List(Cors.Origin.of("*")).asJava)
        .setMethods(List(HttpMethod.GET).asJava)
        .setResponseHeaders(List("*").asJava)
        .setMaxAgeSeconds(0)
        .build()
    )
    when(
      googleStorageService.insertBucket(
        ArgumentMatchers.eq(GoogleProject(googleProjectId)),
        ArgumentMatchers.eq(bucketName),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        autoclassEnabled = ArgumentMatchers.eq(true),
        autoclassTerminalStorageClass = ArgumentMatchers.eq(Option(StorageClass.ARCHIVE)),
        cors = ArgumentMatchers.eq(expectedCorsPolicy)
      )
    ).thenReturn(fs2.Stream.unit)

    val googleServicesDAO = new HttpGoogleServicesDAO(
      GoogleClientSecrets.load(GsonFactory.getDefaultInstance, new StringReader("{}")),
      "clientEmail",
      "subEmail",
      "pemFile",
      "appsDomain",
      "groupsPrefix",
      "appName",
      "serviceProject",
      "billingPemEmail",
      "billingPemFile",
      "billingEmail",
      "billingGroupEmail",
      "credentialsJson",
      200,
      googleStorageService,
      "workbenchMetricBaseName",
      "proxyNamePrefix",
      "terraBucketReaderRole",
      "terraBucketWriterRole",
      null,
      "resourceBufferJsonFile"
    )

    googleServicesDAO.setupWorkspace(userInfo,
                                     GoogleProjectId(googleProjectId),
                                     Map.empty,
                                     bucketName,
                                     Map.empty,
                                     RawlsRequestContext(userInfo, None),
                                     None
    )

    verify(googleStorageService, times(1)).insertBucket(
      ArgumentMatchers.eq(GoogleProject(googleProjectId)),
      ArgumentMatchers.eq(bucketName),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      autoclassEnabled = ArgumentMatchers.eq(true),
      autoclassTerminalStorageClass = ArgumentMatchers.eq(Option(StorageClass.ARCHIVE)),
      cors = ArgumentMatchers.eq(expectedCorsPolicy)
    )
  }

  behavior of "listTimeSeriesPagedResponseToBucketMetricsResponse"

  it should "pull out the bytes and storage classes" in {
    val mockResponse = mock[ListTimeSeriesPagedResponse]
    val labelsMap1 = Map("storage_class" -> "COLDLINE", "type" -> "live-object")

    val metric1 = Metric
      .newBuilder()
      .setType("storage.googleapis.com/storage/v2/total_bytes")
      .putAllLabels(labelsMap1.asJava)
      .build()
    val timeSeries1 = TimeSeries
      .newBuilder()
      .setMetric(metric1)
      .addPoints(
        Point
          .newBuilder()
          .setValue(TypedValue.newBuilder().setDoubleValue(123.45).build())
          .setInterval(
            TimeInterval
              .newBuilder()
              .setStartTime(Timestamp.newBuilder().setSeconds(1609459200).build())
              .setEndTime(Timestamp.newBuilder().setSeconds(1609462800).build())
              .build()
          )
          .build()
      )
      .build()

    val labelsMap2 = Map("storage_class" -> "REGIONAL", "type" -> "soft-deleted-object")

    val metric2 = Metric
      .newBuilder()
      .setType("storage.googleapis.com/storage/v2/total_bytes")
      .putAllLabels(labelsMap2.asJava)
      .build()
    val timeSeries2 = TimeSeries
      .newBuilder()
      .setMetric(metric2)
      .addPoints(
        Point
          .newBuilder()
          .setValue(TypedValue.newBuilder().setDoubleValue(5432.1).build())
          .setInterval(
            TimeInterval
              .newBuilder()
              .setStartTime(Timestamp.newBuilder().setSeconds(1609459200).build())
              .setEndTime(Timestamp.newBuilder().setSeconds(1609462800).build())
              .build()
          )
          .build()
      )
      .build()
    when(mockResponse.iterateAll()).thenReturn(Seq(timeSeries1, timeSeries2).asJava)
    val response = httpGoogleServicesDao.listTimeSeriesPagedResponseToBucketMetricsResponse(mockResponse)
    response.metrics.length shouldBe 2
    val expectedMetrics = Set(
      BucketMetric("COLDLINE", "live-object", 123),
      BucketMetric("REGIONAL", "soft-deleted-object", 5432)
    )
    response.metrics.toSet shouldBe expectedMetrics
  }
  it should "not return zones ending with -ai* in getComputeZonesForRegion" in {
    val mockCompute = mock[Compute]
    val mockRegions = mock[Compute#Regions]
    val mockGet = mock[Compute#Regions#Get]
    val mockRegion = mock[Region]

    val zones = java.util.Arrays.asList(
      "https://www.googleapis.com/compute/v1/projects/project/zones/us-central1-a",
      "https://www.googleapis.com/compute/v1/projects/project/zones/us-central1-b",
      "https://www.googleapis.com/compute/v1/projects/project/zones/us-central-ai1a"
    )

    when(mockCompute.regions()).thenReturn(mockRegions)
    when(mockRegions.get(anyString(), anyString())).thenReturn(mockGet)
    when(mockGet.execute()).thenReturn(mockRegion)
    when(mockRegion.getZones).thenReturn(zones)

    val googleServicesDAO = new MockHttpGoogleServicesDAO(
      GoogleClientSecrets.load(GsonFactory.getDefaultInstance, new StringReader("{}")),
      "fakeClientEmail",
      "fakeSubEmail",
      "fakePemFile",
      "fakeAppsDomain",
      "fakeGroupPrefix",
      "fakeAppName",
      "fakeServiceProject",
      "fakeBillingPemEmail",
      "fakeBillingPemFile",
      "fakeBillingEmail",
      "fakeBillingGroupEmail",
      "fakeCredentialsJson",
      "fakeResourceBufferJsonFile"
    ) {
      override def getComputeManager(credential: com.google.api.client.auth.oauth2.Credential): Compute = mockCompute
      override def executeGoogleRequest[T](request: AbstractGoogleClientRequest[T], logRequest: Boolean)(implicit
        counters: GoogleCounters
      ): T = mockRegion.asInstanceOf[T]
    }

    val result = await(googleServicesDAO.getComputeZonesForRegion(GoogleProjectId("project"), "us-central1"))
    result should contain allOf ("us-central1-a", "us-central1-b")
    result should not contain "us-central-ai1a"
  }
}
