package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  WorkspaceJsonSupport
}
import org.broadinstitute.dsde.workbench.model.WorkbenchGroupName
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.mockserver.model.MediaType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class HttpSamDAOSpec
    extends TestKit(ActorSystem("HttpSamDAOSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  "HttpSamDAO" should "handle no content getting access instructions" in {
    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build(), 1 minute)
    assertResult(None) {
      Await.result(
        dao.getAccessInstructions(
          WorkbenchGroupName("no_instructions"),
          RawlsRequestContext(UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId("")))
        ),
        Duration.Inf
      )
    }
  }

  it should "handle no content getting user id info" in {
    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build(), 1 minute)
    assertResult(SamDAO.NotUser) {
      Await.result(dao.getUserIdInfo(
                     "group@example.com",
                     RawlsRequestContext(UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId("")))
                   ),
                   Duration.Inf
      )
    }
  }

  it should "handle 404 getting user id info" in {
    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build(), 1 minute)
    assertResult(SamDAO.NotFound) {
      Await.result(dao.getUserIdInfo(
                     "dne@example.com",
                     RawlsRequestContext(UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId("")))
                   ),
                   Duration.Inf
      )
    }
  }

  it should "bubble up ErrorReport errors to Rawls' response" in {
    // this tests the error handling in HttpSamDAO.SamApiCallback, which is used by
    // multiple of HttpSamDAO's methods. We'll test one of them here.

    import spray.json._
    import WorkspaceJsonSupport.ErrorReportFormat

    // inviteUser
    val testErrorMessage = "I am an ErrorReport!"
    val testErrorStatus = StatusCodes.ImATeapot
    mockServer.mockServer
      .when(
        request()
          .withMethod("POST")
          .withPath("/api/users/v1/invite/fake-email")
      )
      .respond(
        response()
          .withStatusCode(testErrorStatus.intValue)
          .withBody(ErrorReport(testErrorStatus, testErrorMessage).toJson.prettyPrint)
          .withContentType(MediaType.APPLICATION_JSON)
      )

    val fakeContext =
      RawlsRequestContext(UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId("")))

    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build(), 1 minute)

    val errorReportResponse = intercept[RawlsExceptionWithErrorReport] {
      Await.result(dao.inviteUser("fake-email", fakeContext), Duration.Inf)
    }

    errorReportResponse.errorReport.message shouldBe testErrorMessage
    errorReportResponse.errorReport.statusCode shouldBe Some(testErrorStatus)
  }

  it should "bubble up non-ErrorReport Sam errors to Rawls' response" in {
    // this tests the error handling in HttpSamDAO.SamApiCallback, which is used by
    // multiple of HttpSamDAO's methods. We'll test one of them here.

    val testErrorStatus = StatusCodes.ImATeapot
    mockServer.mockServer
      .when(
        request()
          .withMethod("POST")
          .withPath("/api/users/v1/invite/junk-response")
      )
      .respond(
        response()
          .withStatusCode(testErrorStatus.intValue)
          .withBody("this is not json")
          .withContentType(MediaType.APPLICATION_JSON)
      )

    val fakeContext =
      RawlsRequestContext(UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId("")))

    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build(), 1 minute)

    val junkResponseError = intercept[RawlsExceptionWithErrorReport] {
      Await.result(dao.inviteUser("junk-response", fakeContext), Duration.Inf)
    }

    junkResponseError.errorReport.message shouldBe "Sam call to inviteUser failed with error 'this is not json'"
    junkResponseError.errorReport.statusCode shouldBe Some(testErrorStatus)
  }
}
