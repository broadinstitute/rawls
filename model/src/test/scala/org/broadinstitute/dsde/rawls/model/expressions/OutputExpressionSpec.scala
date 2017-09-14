package org.broadinstitute.dsde.rawls.model.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class OutputExpressionSpec extends FlatSpec with Matchers with ExpressionFixture {
  "OutputExpressions" should "parse" in {
    OutputExpression.build("this.gvcf") shouldEqual Success(TargetedOutputExpression(EntityTarget, AttributeName("default", "gvcf")))
    OutputExpression.build("workspace.gvcf") shouldEqual Success(TargetedOutputExpression(WorkspaceTarget, AttributeName("default", "gvcf")))
    OutputExpression.build("workspace.library:cohort") shouldEqual Success(TargetedOutputExpression(WorkspaceTarget, AttributeName("library", "cohort")))
    OutputExpression.build("this.library:cohort") shouldEqual Success(TargetedOutputExpression(EntityTarget, AttributeName("library", "cohort")))

    // does not enforce Attribute Namespace constraints
    OutputExpression.build("this.arbitrary:whatever") shouldEqual Success(TargetedOutputExpression(EntityTarget, AttributeName("arbitrary", "whatever")))
    OutputExpression.build("workspace.arbitrary:whatever") shouldEqual Success(TargetedOutputExpression(WorkspaceTarget, AttributeName("arbitrary", "whatever")))

    // empty output expressions are allowed
    OutputExpression.build("") shouldEqual Success(BlankOutputExpression)
  }

  it should "bind" in {
    val testAttr = AttributeString("this is a test")

    OutputExpression.bind("this.gvcf", testAttr) shouldEqual Success(BoundOutputExpression(EntityTarget, AttributeName("default", "gvcf"), testAttr))
    OutputExpression.bind("workspace.gvcf", testAttr) shouldEqual Success(BoundOutputExpression(WorkspaceTarget, AttributeName("default", "gvcf"), testAttr))
    OutputExpression.bind("workspace.library:cohort", testAttr) shouldEqual Success(BoundOutputExpression(WorkspaceTarget, AttributeName("library", "cohort"), testAttr))
    OutputExpression.bind("this.library:cohort", testAttr) shouldEqual Success(BoundOutputExpression(EntityTarget, AttributeName("library", "cohort"), testAttr))

    // does not enforce Attribute Namespace constraints
    OutputExpression.bind("this.arbitrary:whatever", testAttr) shouldEqual Success(BoundOutputExpression(EntityTarget, AttributeName("arbitrary", "whatever"), testAttr))
    OutputExpression.bind("workspace.arbitrary:whatever", testAttr) shouldEqual Success(BoundOutputExpression(WorkspaceTarget, AttributeName("arbitrary", "whatever"), testAttr))

    // empty output expressions are allowed: don't bind the outputs back to the data model
    OutputExpression.bind("", testAttr) shouldEqual Success(UnboundOutputExpression)
  }

  it should "round-trip correctly" in {
    validOutputExpressions foreach { expr =>
      OutputExpression.build(expr).map(_.toString) shouldEqual Success(expr)
    }
  }

  it should "reject invalid output expressions" in {
    invalidOutputExpressions foreach { expr =>
      OutputExpression.build(expr) shouldBe a [Failure[_]]
    }
  }
}
