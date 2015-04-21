package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.TestClass._
import org.scalatest.{Matchers, FlatSpec}
import spray.json._

/**
 * Created by abaumann on 4/21/15.
 */
class EqualityTest extends FlatSpec with Matchers {
  val tc = TestClass(33)
  val tc2 = TestClass(33)
  val tc3 = TestClass(11)

  "Two classes with the same seed" should "be equal" in {
    tc should be (tc2)
  }

  "Two classes with different seeds" should "not be equal" in {
    tc should not be (tc3)
  }
}

class SerializeTest extends FlatSpec with Matchers {
  val tc = TestClass(33)
  
  val tcAsJson = tc.toJson
  val tcAsJsonString = tcAsJson.compactPrint

  val tcBack = tcAsJsonString.parseJson.convertTo[TestClass]

  "JSON serialized classes " should "deserialize to the same object" in {
    tc should be (tcBack)
  }
}









