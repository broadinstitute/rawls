package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, RawlsUser, RawlsUserEmail, RawlsUserSubjectId, UserInfo, WorkspaceJsonSupport}
import org.broadinstitute.dsde.workbench.model.WorkbenchGroupName
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class HttpSamDAOSpec extends TestKit(ActorSystem("HttpSamDAOSpec"))
  with AnyFlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val materializer = ActorMaterializer()
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
    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build())
    assertResult(None) {
      Await.result(dao.getAccessInstructions(WorkbenchGroupName("no_instructions"), UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId(""))), Duration.Inf)
    }
  }

  it should "handle no content getting user id info" in {
    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build())
    assertResult(SamDAO.NotUser) {
      Await.result(dao.getUserIdInfo("group@example.com", UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId(""))), Duration.Inf)
    }
  }

  it should "handle 404 getting user id info" in {
    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build())
    assertResult(SamDAO.NotFound) {
      Await.result(dao.getUserIdInfo("dne@example.com", UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId(""))), Duration.Inf)
    }
  }

  it should "fetch info for the given user's info" in {
    val userInfo = UserInfo(RawlsUserEmail("example@example.com"),
      OAuth2BearerToken(""),
      0,
      RawlsUserSubjectId("fake_subject_id")
    )
    val rawlsUser = RawlsUser(userInfo)

    mockServer.mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/register/user/v2/self/info")
    ).respond(
      response()
        .withHeaders(mockServer.jsonHeader)
        .withStatusCode(StatusCodes.OK.intValue)
        .withBody(
          s"""
             |{
             |   "userSubjectId": "${rawlsUser.userSubjectId.value}",
             |   "userEmail": "${userInfo.userEmail.value}",
             |   "enabled": true
             |}
             |""".stripMargin)
    )

    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build())
    assertResult(Some(rawlsUser)) {
      Await.result(dao.getUserStatus(userInfo), Duration.Inf)
    }
  }

  it should "bubble up ErrorReport errors to Rawls' response" in {
    // this tests the error handling in HttpSamDAO.doSuccessOrFailureRequest, which is used by
    // multiple of HttpSamDAO's methods. We'll test one of them here.

    import spray.json._
    import WorkspaceJsonSupport.ErrorReportFormat

    // inviteUser
    mockServer.mockServer.when(
      request()
        .withMethod("POST")
        .withPath("/api/users/v1/invite/fake-email")
    ).respond(
      response().withStatusCode(StatusCodes.InternalServerError.intValue).withBody(ErrorReport("I am an ErrorReport!").toJson.prettyPrint)
    )

    val fakeUserInfo = UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId(""))

    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build())

    val inviteErr = intercept[RawlsExceptionWithErrorReport] {
      Await.result(dao.inviteUser("fake-email", fakeUserInfo), Duration.Inf)
    }

    inviteErr.errorReport.message should startWith ("Sam call to")
    inviteErr.errorReport.message should include ("failed with error")
    inviteErr.errorReport.message should include ("I am an ErrorReport!")
  }

  it should "bubble up non-ErrorReport Sam errors to Rawls' response" in {
    // this tests the error handling in HttpSamDAO.doSuccessOrFailureRequest, which is used by
    // multiple of HttpSamDAO's methods. We'll test one of them here.

    // deleteUserPetServiceAccount
    mockServer.mockServer.when(
      request()
        .withMethod("DELETE")
        .withPath("/api/google/v1/user/petServiceAccount/fake-project")
    ).respond(
      response().withStatusCode(StatusCodes.InternalServerError.intValue).withBody("not an ErrorReport")
    )

    val fakeUserInfo = UserInfo(RawlsUserEmail(""), OAuth2BearerToken(""), 0, RawlsUserSubjectId(""))

    val dao = new HttpSamDAO(mockServer.mockServerBaseUrl, new MockGoogleCredential.Builder().build())

    val deletePetError = intercept[RawlsExceptionWithErrorReport] {
      Await.result(dao.deleteUserPetServiceAccount(GoogleProjectId("fake-project"), fakeUserInfo), Duration.Inf)
    }

    deletePetError.errorReport.message should startWith ("Sam call to")
    // note the "also" in the error string below
    deletePetError.errorReport.message should endWith ("failed with error 'not an ErrorReport'")
  }
}
