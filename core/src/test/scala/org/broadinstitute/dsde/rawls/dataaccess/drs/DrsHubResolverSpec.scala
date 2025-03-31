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
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class DrsHubResolverSpec extends TestKit(ActorSystem("DrsHubResolverSpec")) with AnyFlatSpecLike {
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  val mockDrsHubResolver = spy(new DrsHubResolver("foo@bar.com"))
  val mockUserInfo = mock[UserInfo](RETURNS_SMART_NULLS)

  when(mockUserInfo.accessToken).thenReturn(OAuth2BearerToken("access_token"))
  behavior of "DrsHubResolver"

  it should "get the signed url for a drs object" in {
    doReturn(
      Future.successful(
        DrsHubMinimalResponse(Some(DrsHubAccessUrl(Some("https://signed-url.com/file?key=123"), None)))
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

  "getProvider" should "get correct providers" in {
    DrsResolver.getProvider("drs://dg.anv0:f51fc329-b09e-4e16-b1a9-2f60ebc428ab") shouldBe Some("dg.anv0")
    DrsResolver.getProvider("drs://drs.example.org/ga4gh/drs/v1/objects/314159") shouldBe Some("drs.example.org")
    DrsResolver.getProvider("https://storage.googleapis.com/v1_abc-123?key=54321") shouldBe Some(
      "storage.googleapis.com"
    )
    DrsResolver.getProvider("drs://jade.datarepo-dev.broadinstitute.org/v1_abcd-123-efg") shouldBe Some(
      "jade.datarepo-dev.broadinstitute.org"
    )
    DrsResolver.getProvider("invalid-url") shouldBe None
  }
}
