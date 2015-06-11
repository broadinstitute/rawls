package scala.org.broadinstitute.dsde.rawls.openam

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.IntegrationTestConfig
import OpenAmClientService.{OpenAmResponse, OpenAmAuthRequest}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

class OpenAmClientSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with IntegrationTestConfig {

  implicit val timeout = Timeout(5, TimeUnit.SECONDS) // Required for actor sendReceive
  val duration: Duration = new FiniteDuration(5, TimeUnit.SECONDS)

  "The OpenAmClientService Actor" should {
    "return a valid token id" in {
      val actor = TestActorRef[OpenAmClientService]
      val future = actor ? OpenAmAuthRequest(openAmTestUser, openAmTestUserPassword)
      val result = Await.result(future, duration)
      result shouldNot be (None)
      result.asInstanceOf[OpenAmResponse].tokenId shouldNot be (null)
      result.asInstanceOf[OpenAmResponse].successUrl shouldNot be (null)
    }
  }

}
