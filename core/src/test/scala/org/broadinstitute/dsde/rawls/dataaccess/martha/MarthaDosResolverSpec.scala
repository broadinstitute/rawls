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

class MarthaDosResolverSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  implicit val mockActorSystem: ActorSystem = ActorSystem("MockMarthaDosResolver")
  implicit val mockMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val mockExecutionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockMarthaDosResolver = new MockMarthaDosResolver(marthaUrl = "https://martha_v2_url", excludeJDRDomain = true)
  val mockUserInfo = UserInfo(RawlsUserEmail("mr_bean@gmail.com"), OAuth2BearerToken("foo"), 0, RawlsUserSubjectId("abc123"))

  behavior of "Martha DOS resolver"

  it should "return true for correct DEV JDR uri in isJDRDomain()" in {
    MarthaDosResolver.isJDRDomain("drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4") shouldBe true
  }

  it should "return false for look alike JDR uri in isJDRDomain()" in {
    MarthaDosResolver.isJDRDomain("drs://jade-datarepo.dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4") shouldBe false
  }

  it should "return true for correct PROD JDR uri in isJDRDomain()" in {
    MarthaDosResolver.isJDRDomain("drs://jade-terra.datarepo-prod.broadinstitute.org/v1_anything") shouldBe true
  }

  it should "return false for non-JDR uri in isJDRDomain()" in {
    MarthaDosResolver.isJDRDomain("drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0") shouldBe false
  }

  it should "not explode on URI parse fail" in {
    MarthaDosResolver.isJDRDomain("zardoz") shouldBe false
  }

  it should "not contact Martha for JDR uri" in {
    val actualResultFuture = mockMarthaDosResolver.dosServiceAccountEmail(
      dos = "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4",
      userInfo = mockUserInfo
    )

    assertResult(None) {
      Await.result(actualResultFuture, 1 minute)
    }
  }

  it should "return client email for non-JDR uri" in {
    val actualResultFuture = mockMarthaDosResolver.dosServiceAccountEmail(
      dos = "drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0",
      userInfo = mockUserInfo
    )

    assertResult(Option("mr_bean@gmail.com")) {
      Await.result(actualResultFuture, 1 minute)
    }
  }
}


class MockMarthaDosResolver(marthaUrl: String, excludeJDRDomain: Boolean)
                           (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext)
  extends MarthaDosResolver(marthaUrl, excludeJDRDomain) {

  override def getClientEmailFromMartha(dos: String, userInfo: UserInfo): Future[Option[String]] = {
    Future.successful(Option("mr_bean@gmail.com"))
  }
}
