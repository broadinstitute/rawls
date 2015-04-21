package org.broadinstitute.dsde.rawls

import spray.json._
import DefaultJsonProtocol._
import org.scalatest._

class JsonTestSpec extends FlatSpec with Matchers {
  "A JsonParser" should "parse JSON" in {
    val json = """{"numbers":[1,2,3,4]}"""
    val parsed = json.parseJson
    parsed.compactPrint should be (json)
  }
}
