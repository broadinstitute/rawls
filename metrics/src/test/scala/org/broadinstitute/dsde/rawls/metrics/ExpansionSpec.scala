package org.broadinstitute.dsde.rawls.metrics

import akka.http.scaladsl.model._
import org.broadinstitute.dsde.rawls.metrics.Expansion._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/**
  * Created by rtitle on 7/16/17.
  */
class ExpansionSpec extends AnyFlatSpec with Matchers {

  "the Expansion typeclass" should "expand UUIDs" in {
    val test = UUID.randomUUID
    assertResult(test.toString) {
      implicitly[Expansion[UUID]].makeName(test)
    }
  }

  it should "expand HttpMethods" in {
    val test = HttpMethods.PATCH
    assertResult("patch") {
      implicitly[Expansion[HttpMethod]].makeName(test)
    }
  }

  it should "expand StatusCodes" in {
    val test = StatusCodes.Forbidden
    assertResult("403") {
      implicitly[Expansion[StatusCode]].makeName(test)
    }
  }

  it should "expand Uris" in {
    val test = Uri("/workspace/broad-dsde-dev/myspace")
    assertResult("workspace.broad-dsde-dev.myspace") {
      implicitly[Expansion[Uri]].makeName(test)
    }
  }

  it should "expand primitives" in {
    val str = "A String"
    val int = 42
    assertResult(str) {
      implicitly[Expansion[String]].makeName(str)
    }
    assertResult("42") {
      implicitly[Expansion[Int]].makeName(int)
    }
  }
}
