package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.service.Sam.convertScalaFuture
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpecLike, Matchers}
import scala.concurrent.ExecutionContext.Implicits.global

class MarthaDosResolverSpec extends TestKit(ActorSystem("MarthaDosResolverSpec")) with FlatSpecLike with Eventually with Matchers {

  implicit val materializer = ActorMaterializer()
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val marthaDosResolver = new MarthaDosResolver("https://us-central1-broad-dsde-staging.cloudfunctions.net/martha_v1")

  // Currently fails with "A needed class was not found. This could be due to an error in your runpath. Missing class: com/fasterxml/jackson/module/scala/DefaultScalaModule$"
  "MarthaDAO" should "resolve a good DOS URI" ignore {
    val result = marthaDosResolver.dosToGs("dos://spbnq0bc10.execute-api.us-west-2.amazonaws.com/ed703a5d-4705-49a8-9429-5169d9225bbd").futureValue
    result shouldEqual "gs://commons-dss-commons/blobs/64573c6a0c75993c16e313f819fa71b8571b86de75b7523ae8677a92172ea2ba.9976538e92c4f12aebfea277ecaef9fc5b54c732.594f5f1a316e9ccfb38d02a345c86597-293.41a4b033"
  }
}
