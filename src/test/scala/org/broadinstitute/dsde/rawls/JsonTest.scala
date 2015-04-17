package org.broadinstitute.dsde.rawls

import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest._

class JsonTestSpec extends FlatSpec with Matchers {
  "A JsonParser" should "parse JSON" in {
    val json = """{"numbers":[1,2,3,4]}"""
    val parsed = parse(json)
    compact(render(parsed)) should be (json)
  }
}
