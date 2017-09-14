package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.scalatest.FlatSpec

class ExpressionParserSpec extends FlatSpec with TestDriverComponent {

  it should "parse method config expressions" in {
    val shouldBeValid = ExpressionParser.parseMCExpressions(testData.methodConfigValidExprs, this)
    assertResult(6) { shouldBeValid.validInputs.size }
    assertResult(4) { shouldBeValid.validOutputs.size }
    assertResult(0) { shouldBeValid.invalidInputs.size }
    assertResult(0) { shouldBeValid.invalidOutputs.size }

    val shouldBeInvalid = ExpressionParser.parseMCExpressions(testData.methodConfigInvalidExprs, this)
    assertResult(1) { shouldBeInvalid.validInputs.size }
    assertResult(0) { shouldBeInvalid.validOutputs.size }
    assertResult(1) { shouldBeInvalid.invalidInputs.size }
    assertResult(4) { shouldBeInvalid.invalidOutputs.size }
  }

}
