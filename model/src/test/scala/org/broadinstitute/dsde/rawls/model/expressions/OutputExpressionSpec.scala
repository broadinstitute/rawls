package org.broadinstitute.dsde.rawls.model.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class OutputExpressionSpec extends FlatSpec with Matchers {
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
    val strings = Seq(
      "this.gvcf",
      "workspace.gvcf",
      "workspace.library:cohort",
      "this.library:cohort",
      "this.arbitrary:whatever",
      "workspace.arbitrary:whatever",
      ""
    )

    strings foreach { s =>
      OutputExpression.build(s).map(_.toString) shouldEqual Success(s)
    }
  }

  it should "reject invalid output expressions" in {
    OutputExpression.build("this.") shouldBe a [Failure[_]]
    OutputExpression.build("this.bad|character") shouldBe a [Failure[_]]
    OutputExpression.build("this.case_sample.attribute") shouldBe a [Failure[_]]
    OutputExpression.build("workspace.") shouldBe a [Failure[_]]
    OutputExpression.build("workspace........") shouldBe a [Failure[_]]
    OutputExpression.build("workspace.nope.nope.nope") shouldBe a [Failure[_]]
    OutputExpression.build("where_does_this_even_go") shouldBe a [Failure[_]]
    OutputExpression.build("*") shouldBe a [Failure[_]]
  }
}
