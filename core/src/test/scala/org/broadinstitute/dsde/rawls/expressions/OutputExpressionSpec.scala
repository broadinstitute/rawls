package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.AttributeName
import org.scalatest.{FlatSpec, Matchers}

class OutputExpressionSpec extends FlatSpec with Matchers {

  "OutputExpressions" should "parse" in {
    OutputExpression("this.gvcf") shouldEqual OutputExpression(ThisEntityTarget, AttributeName("default", "gvcf"))
    OutputExpression("workspace.gvcf") shouldEqual OutputExpression(WorkspaceTarget, AttributeName("default", "gvcf"))
    OutputExpression("workspace.library:cohort") shouldEqual OutputExpression(WorkspaceTarget, AttributeName("library", "cohort"))
    OutputExpression("this.library:cohort") shouldEqual OutputExpression(ThisEntityTarget, AttributeName("library", "cohort"))

    // does not enforce Attribute Namespace constraints
    OutputExpression("this.arbitrary:whatever") shouldEqual OutputExpression(ThisEntityTarget, AttributeName("arbitrary", "whatever"))
    OutputExpression("workspace.arbitrary:whatever") shouldEqual OutputExpression(WorkspaceTarget, AttributeName("arbitrary", "whatever"))
  }

  it should "reject invalid output expressions" in {
    intercept[RawlsException] { OutputExpression("this.") }
    intercept[RawlsException] { OutputExpression("this.bad|character") }
    intercept[RawlsException] { OutputExpression("this.case_sample.attribute") }
    intercept[RawlsException] { OutputExpression("workspace.") }
    intercept[RawlsException] { OutputExpression("workspace........") }
    intercept[RawlsException] { OutputExpression("workspace.nope.nope.nope") }
    intercept[RawlsException] { OutputExpression("where_does_this_even_go") }
    intercept[RawlsException] { OutputExpression("") }
    intercept[RawlsException] { OutputExpression("*") }
  }
}
