package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import org.scalatestplus.mockito.MockitoSugar.mock
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, spy, when, RETURNS_SMART_NULLS}
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

  //TODO update result to be a url
  it should "get the signed url for a drs object" in {
    doReturn(
      Future.successful(
        DrsHubMinimalResponse(Option(("https://signed-url.com/file?key=123")))
      )
    )
      .when(mockDrsHubResolver)
      .executeRequestWithToken(any[OAuth2BearerToken])(any[HttpRequest])(any())
    val response = mockDrsHubResolver.drsSignedUrl("drs://drs-provider.com/v1_foo_bar", mockUserInfo)
    assertResult(Option("https://signed-url.com/file?key=123")) {
      Await.result(response, 1 minute)
    }
  }

  it should "handle no signed url for a drs object" in {
    doReturn(Future.successful(DrsHubMinimalResponse(None)))
      .when(mockDrsHubResolver)
      .executeRequestWithToken(any[OAuth2BearerToken])(any[HttpRequest])(any())
    val response = mockDrsHubResolver.drsSignedUrl("drs://drs-provider.com/v1_foo_bar", mockUserInfo)
    assertResult(None) {
      Await.result(response, 1 minute)
    }
  }
}
