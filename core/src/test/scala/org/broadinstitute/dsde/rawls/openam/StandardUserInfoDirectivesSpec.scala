package org.broadinstitute.dsde.rawls.openam

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUser, RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.{ExecutionContext, Future}

class StandardUserInfoDirectivesSpec extends AnyFlatSpec with Matchers with StandardUserInfoDirectives with ScalatestRouteTest {

  override implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  override val samDAO: SamDAO = mock[SamDAO]

  "requireUserInfo" should "raise an error when user is disabled" in {
    when(samDAO.getUserStatus(any[UserInfo])).thenReturn(Future.successful(
      Some(RawlsUser(RawlsUserSubjectId("fake_sub"), RawlsUserEmail("example@example.com"), false))
    ))

    Get("/fake") ~> addHeaders(
      RawHeader("OIDC_access_token", "fake_oidc"),
      RawHeader("OIDC_CLAIM_user_id", "fake_user_id"),
      RawHeader("OIDC_CLAIM_expires_in", "1234"),
      RawHeader("OIDC_CLAIM_email", "example@example.com")
    ) ~> requireUserInfo()(_ => complete("fake")) ~> check {
      response.status.intValue() shouldEqual 500
    }
  }
}
