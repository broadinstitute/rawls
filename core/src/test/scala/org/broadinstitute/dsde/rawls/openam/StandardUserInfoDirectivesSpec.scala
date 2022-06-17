package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.AuthorizationFailedRejection
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.server.Directives.complete
import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail, RawlsUserSubjectId, UserIdInfo, UserInfo}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when

import scala.concurrent.{ExecutionContext, Future}

class StandardUserInfoDirectivesSpec extends AnyFlatSpec with Matchers with StandardUserInfoDirectives with ScalatestRouteTest {
  override implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  override val samDAO: SamDAO = mock[SamDAO]

  val authHeaders = Seq(
    RawHeader("OIDC_access_token", "fake_token"),
    RawHeader("OIDC_CLAIM_user_id", "fake_user_id"),
    RawHeader("OIDC_CLAIM_expires_in", "123"),
    RawHeader("OIDC_CLAIM_email", "example@example.com"),
    RawHeader("OAUTH2_CLAIM_idp_access_token", "idp_access_token")
  )

  "StandardUserInfoDirectives" should "allow requests for enabled users" in {
    when(samDAO.getUserStatus(any[UserInfo])).thenReturn(
      Future.successful(Some(
        RawlsUser(
          RawlsUserSubjectId("fake_sub_id"),
          RawlsUserEmail("example@example.com"))
    )))
    Get("/fake").withHeaders(authHeaders) ~> requireUserInfo()(userInfo => complete(userInfo.userSubjectId.value))  ~> check {
        status shouldEqual StatusCodes.OK
    }
  }

  it should "raise an exception requests for disabled users" in  {
    when(samDAO.getUserStatus(any[UserInfo])).thenReturn(
      Future.successful(Some(
        RawlsUser(
          RawlsUserSubjectId("fake_sub_id"),
          RawlsUserEmail("example@example.com"),
          enabled = false
        )
      )))

    Get("/fake").withHeaders(authHeaders) ~> requireUserInfo()(userInfo => complete(userInfo.userSubjectId.value))  ~> check {
      status shouldEqual StatusCodes.InternalServerError
    }
  }
}
