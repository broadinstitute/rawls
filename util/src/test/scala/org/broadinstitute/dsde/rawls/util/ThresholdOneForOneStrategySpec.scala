package org.broadinstitute.dsde.rawls.util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class ThresholdOneForOneStrategySpec extends TestKit(ActorSystem("MySpec")) with FlatSpecLike with BeforeAndAfterAll with Matchers with MockitoSugar with ScalaFutures {
  //TODO: uhhh

  /* test that:
     - thresholdFunc executes after thresholdLimit is set
     - maxNrOfRetries works as expected still
   */
}
