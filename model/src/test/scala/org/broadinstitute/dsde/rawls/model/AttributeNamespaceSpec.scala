package org.broadinstitute.dsde.rawls.model

import org.scalatest.{FlatSpec, Matchers}

class AttributeNamespaceSpec extends FlatSpec with Matchers {
  "AttributeNamespace" should "parse delimited names" in {
    val fromExpectations = Map(
      "simple"              -> AttributeName("default", "simple"),
      "default:superfluous" -> AttributeName("default","superfluous"),
      "library:book"        -> AttributeName("library", "book")
    )

    fromExpectations.foreach { case (delimitedStr, name) =>
      assertResult(name) {
        AttributeName.fromDelimitedName(delimitedStr)
      }
    }

    val toExpectations = Map(
      AttributeName("default", "simple") -> "simple",
      AttributeName("default", "superfluous") -> "superfluous",
      AttributeName("library", "book") -> "library:book"
    )

    toExpectations.foreach { case (name, delimitedStr) =>
      assertResult(delimitedStr) {
        AttributeName.toDelimitedName(name)
      }
    }
  }
}
