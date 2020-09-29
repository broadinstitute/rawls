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

  implicit val mockActorSystem: ActorSystem = ActorSystem("MockMarthaResolver")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockMarthaResolver = new MockMarthaResolver(marthaUrl = "https://martha_v2_url", excludeJDRDomain = true)
  val mockUserInfo: UserInfo = UserInfo(RawlsUserEmail("mr_bean@gmail.com"), OAuth2BearerToken("foo"), 0, RawlsUserSubjectId("abc123"))

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

object MockMarthaResolver {
  val jdrDevUrl = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4"
  val dgUrl = "drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0"

  val mockEmail: ServiceAccountEmail = ServiceAccountEmail("mr_bean@gmail.com")
  val mockSAPayload: ServiceAccountPayload = ServiceAccountPayload(Option(mockEmail))
  val exampleGoogleSA: MarthaMinimalResponse = MarthaMinimalResponse(Option(mockSAPayload))
}

class MockMarthaResolver(marthaUrl: String, excludeJDRDomain: Boolean)
                           (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaResolver(marthaUrl) {

  override def resolveDrsThroughMartha(drsUrl: String, userInfo: UserInfo): Future[MarthaMinimalResponse] = {
    Future.successful{
      drsUrl match {
        case MockMarthaResolver.jdrDevUrl => MarthaMinimalResponse(None)
        case _ => MockMarthaResolver.exampleGoogleSA
      }
    }
  }
}
