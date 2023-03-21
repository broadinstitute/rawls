package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import cats.effect.IO
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.gson.GsonFactory
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO._
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingAccount, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{RETURNS_SMART_NULLS, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.StringReader
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class HttpGoogleServicesDAOSpec extends AnyFlatSpec with Matchers with MockitoTestUtils {

  behavior of "handleByOperationIdType"

  def v1Handler(opId: String) = "v1"
  def v2alpha1Handler(opId: String) = "v2alpha1"
  def lifeSciencesHandler(opId: String) = "lifeSciences"
  def defaultHandler(opId: String) = "default"

  val cases: List[(String, String)] = List(
    ("operations/abc", "v1"),
    ("projects/abc/operations/def", "v2alpha1"),
    ("projects/abc/locations/def/operations/ghi", "lifeSciences"),
    ("!!no match!!", "default")
  )

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
    "fakeResourceBufferJsonFile"
  )

  cases foreach { case (opId, identification) =>
    it should s"Correctly identify $opId as $identification" in {
      handleByOperationIdType(opId, v1Handler, v2alpha1Handler, lifeSciencesHandler, defaultHandler) should be(
        identification
      )
    }
  }

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
        autoclassEnabled = ArgumentMatchers.eq(true)
      )
    ).thenReturn(fs2.Stream.unit)


    val googleServicesDAO = new HttpGoogleServicesDAO(
      GoogleClientSecrets.load(GsonFactory.getDefaultInstance, new StringReader("{}")),
      "clientEmail",
      "subEmail",
      "pemFile",
      "appsDomain",
      123L,
      "groupsPrefix",
      "appName",
      "serviceProject",
      "billingPemEmail",
      "billingPemFile",
      "billingEmail",
      "billingGroupEmail",
      "billingProbeEmail",
      200,
      googleStorageService,
      "workbenchMetricBaseName",
      "proxyNamePrefix",
      "deploymentMgrProject",
      true,
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
                                     null,
                                     None
    )

    verify(googleStorageService.insertBucket(ArgumentMatchers.eq(GoogleProject(googleProjectId)),
      ArgumentMatchers.eq(bucketName),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      autoclassEnabled = ArgumentMatchers.eq(true)), times(1))
  }
}
