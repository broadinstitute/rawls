package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchGroupName
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
}
