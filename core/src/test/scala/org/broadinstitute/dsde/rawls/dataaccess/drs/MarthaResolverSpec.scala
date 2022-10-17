package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.ActorMaterializer
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.mock.MockMarthaResolver
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class MarthaResolverSpec extends AnyFlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val mockActorSystem: ActorSystem = ActorSystem("MockMarthaResolver")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockMarthaResolver = new MockMarthaResolver(marthaUrl = "https://martha_v3_url")
  val mockUserInfo: UserInfo =
    UserInfo(RawlsUserEmail("mr_bean@gmail.com"), OAuth2BearerToken("foo"), 0, RawlsUserSubjectId("abc123"))

  behavior of "MarthaResolver"

  it should "return no client email for JDR uri" in {
    val actualResultFuture = mockMarthaResolver.drsServiceAccountEmail(
      drsUrl = MockMarthaResolver.jdrDevUrl,
      userInfo = mockUserInfo
    )

    assertResult(None) {
      Await.result(actualResultFuture, 1 minute)
    }
  }

  it should "return client email for non-JDR uri" in {
    val actualResultFuture = mockMarthaResolver.drsServiceAccountEmail(
      drsUrl = MockMarthaResolver.dgUrl,
      userInfo = mockUserInfo
    )

    assertResult(Option("mr_bean@gmail.com")) {
      Await.result(actualResultFuture, 1 minute)
    }
  }
}
