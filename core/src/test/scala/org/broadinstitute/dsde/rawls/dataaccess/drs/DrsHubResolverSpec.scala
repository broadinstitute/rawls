package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import org.scalatestplus.mockito.MockitoSugar.mock
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{RETURNS_SMART_NULLS, doReturn, spy, when}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class DrsHubResolverSpec extends TestKit(ActorSystem("DrsHubResolverSpec")) with AnyFlatSpecLike {
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  val mockDrsHubResolver = spy(new DrsHubResolver("foo@bar.com"))
  val mockUserInfo = mock[UserInfo](RETURNS_SMART_NULLS)

  when(mockUserInfo.accessToken).thenReturn(OAuth2BearerToken("access_token"))
  behavior of "DrsHubResolver"

  it should "get the service account of a drs object" in {
    doReturn(
      Future.successful(
        DrsHubMinimalResponse(Option(ServiceAccountPayload(Option(ServiceAccountEmail("foo@bar.com")))))
      )
    )
      .when(mockDrsHubResolver)
      .executeRequestWithToken(any[OAuth2BearerToken])(any[HttpRequest])(any())
    val response = mockDrsHubResolver.drsServiceAccountEmail("drs://drs-provider.com/v1_foo_bar", mockUserInfo)
    assertResult(Option("foo@bar.com")) {
      Await.result(response, 1 minute)
    }
  }

  it should "handle no service account for a drs object" in {
    doReturn(Future.successful(DrsHubMinimalResponse(Option(ServiceAccountPayload(None)))))
      .when(mockDrsHubResolver)
      .executeRequestWithToken(any[OAuth2BearerToken])(any[HttpRequest])(any())
    val response = mockDrsHubResolver.drsServiceAccountEmail("drs://drs-provider.com/v1_foo_bar", mockUserInfo)
    assertResult(None) {
      Await.result(response, 1 minute)
    }
  }
}
