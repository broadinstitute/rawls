package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString}
import org.scalatest.{FlatSpec, Matchers}

class OutputExpressionSpec extends FlatSpec with Matchers {
  val testAttr = AttributeString("this is a test")

  "OutputExpressions" should "parse" in {
    OutputExpression("this.gvcf", testAttr) shouldEqual BoundOutputExpression(ThisEntityTarget, AttributeName("default", "gvcf"), testAttr)
    OutputExpression("workspace.gvcf", testAttr) shouldEqual BoundOutputExpression(WorkspaceTarget, AttributeName("default", "gvcf"), testAttr)
    OutputExpression("workspace.library:cohort", testAttr) shouldEqual BoundOutputExpression(WorkspaceTarget, AttributeName("library", "cohort"), testAttr)
    OutputExpression("this.library:cohort", testAttr) shouldEqual BoundOutputExpression(ThisEntityTarget, AttributeName("library", "cohort"), testAttr)

    // does not enforce Attribute Namespace constraints
    OutputExpression("this.arbitrary:whatever", testAttr) shouldEqual BoundOutputExpression(ThisEntityTarget, AttributeName("arbitrary", "whatever"), testAttr)
    OutputExpression("workspace.arbitrary:whatever", testAttr) shouldEqual BoundOutputExpression(WorkspaceTarget, AttributeName("arbitrary", "whatever"), testAttr)

    // empty output expressions are allowed: don't bind the outputs back to the data model
    OutputExpression("", testAttr) shouldEqual UnboundOutputExpression
  }

  it should "reject invalid output expressions" in {
    intercept[RawlsException] { OutputExpression("this.", testAttr) }
    intercept[RawlsException] { OutputExpression("this.bad|character", testAttr) }
    intercept[RawlsException] { OutputExpression("this.case_sample.attribute", testAttr) }
    intercept[RawlsException] { OutputExpression("workspace.", testAttr) }
    intercept[RawlsException] { OutputExpression("workspace........", testAttr) }
    intercept[RawlsException] { OutputExpression("workspace.nope.nope.nope", testAttr) }
    intercept[RawlsException] { OutputExpression("where_does_this_even_go", testAttr) }
    intercept[RawlsException] { OutputExpression("*", testAttr) }
  }
}
