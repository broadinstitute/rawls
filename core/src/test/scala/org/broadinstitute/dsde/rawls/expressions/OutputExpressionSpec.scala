package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class OutputExpressionSpec extends FlatSpec with Matchers {
  val testAttr = AttributeString("this is a test")

  "OutputExpressions" should "parse" in {
    OutputExpression.build("this.gvcf", testAttr) shouldEqual Success(BoundOutputExpression(ThisEntityTarget, AttributeName("default", "gvcf"), testAttr))
    OutputExpression.build("workspace.gvcf", testAttr) shouldEqual Success(BoundOutputExpression(WorkspaceTarget, AttributeName("default", "gvcf"), testAttr))
    OutputExpression.build("workspace.library:cohort", testAttr) shouldEqual Success(BoundOutputExpression(WorkspaceTarget, AttributeName("library", "cohort"), testAttr))
    OutputExpression.build("this.library:cohort", testAttr) shouldEqual Success(BoundOutputExpression(ThisEntityTarget, AttributeName("library", "cohort"), testAttr))

    // does not enforce Attribute Namespace constraints
    OutputExpression.build("this.arbitrary:whatever", testAttr) shouldEqual Success(BoundOutputExpression(ThisEntityTarget, AttributeName("arbitrary", "whatever"), testAttr))
    OutputExpression.build("workspace.arbitrary:whatever", testAttr) shouldEqual Success(BoundOutputExpression(WorkspaceTarget, AttributeName("arbitrary", "whatever"), testAttr))

    // empty output expressions are allowed: don't bind the outputs back to the data model
    OutputExpression.build("", testAttr) shouldEqual Success(UnboundOutputExpression)
  }

  it should "reject invalid output expressions" in {
    OutputExpression.build("this.", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("this.bad|character", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("this.case_sample.attribute", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("workspace.", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("workspace........", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("workspace.nope.nope.nope", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("where_does_this_even_go", testAttr) shouldBe a [Failure[_]]
    OutputExpression.build("*", testAttr) shouldBe a [Failure[_]]
  }
}
