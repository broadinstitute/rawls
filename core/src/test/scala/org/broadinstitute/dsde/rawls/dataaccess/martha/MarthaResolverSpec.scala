package org.broadinstitute.dsde.rawls.dataaccess.martha

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.{ActorMaterializer, Materializer}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, UserInfo}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class MarthaResolverSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val mockActorSystem: ActorSystem = ActorSystem("MockMarthaDosResolver")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockMarthaResolver = new MockMarthaResolver(marthaUrl = "https://martha_v3_url")
  val mockUserInfo: UserInfo = UserInfo(
    userEmail = RawlsUserEmail("mr_bean@gmail.com"),
    accessToken = OAuth2BearerToken("foo"),
    accessTokenExpiresIn = 0,
    userSubjectId = RawlsUserSubjectId("abc123")
  )

  behavior of "Martha resolver"

  it should "return client email for non-JDR uri" in {
    val actualResultFuture = mockMarthaResolver.dosServiceAccountEmail(
      drsUrl = mockMarthaResolver.drsUrl,
      userInfo = mockUserInfo
    )

    assertResult(Option("mr_bean@gmail.com")) {
      Await.result(actualResultFuture, 1 minute)
    }
  }

  it should "return no client email for JDR uri" in {
    val actualResultFuture = mockMarthaResolver.dosServiceAccountEmail(
      drsUrl = mockMarthaResolver.jdrUrl,
      userInfo = mockUserInfo
    )

    assertResult(None) {
      Await.result(actualResultFuture, 1 minute)
    }
  }
}


class MockMarthaResolver(marthaUrl: String)
                        (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaResolver(marthaUrl) {

  val jdrUrl = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060"
  val drsUrl = "drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0"

  override def dosServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] = {
    drsUrl match {
      case `jdrUrl` => Future.successful(None)
      case _ => Future.successful(Option("mr_bean@gmail.com"))
    }
  }
}
