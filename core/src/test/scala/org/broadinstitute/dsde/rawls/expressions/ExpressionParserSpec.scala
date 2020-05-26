package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{AttributeString}
import org.scalatest.FlatSpec

class ExpressionParserSpec extends FlatSpec with TestDriverComponent with ExpressionFixture with RawlsTestUtils  {

  // assumes that expressions have already been validated: see ExpressionValidatorSpec for that step

  it should "parse method config expressions" in {
    def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
      expressions.map { expr => expr.toString -> AttributeString(expr) }.toMap

    val actualParseable = ExpressionParser.parseMCExpressions(toExpressionMap(parseableInputExpressions), toExpressionMap(parseableOutputExpressions), allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualParseable.validInputs)
    assertSameElements(parseableOutputExpressions, actualParseable.validOutputs)
    actualParseable.invalidInputs shouldBe 'empty
    actualParseable.invalidOutputs shouldBe 'empty

    val actualParseableWithNoRoot = ExpressionParser.parseMCExpressions(toExpressionMap(parseableInputExpressionsWithNoRoot), toExpressionMap(parseableOutputExpressionsWithNoRoot), allowRootEntity = false, this)
    assertSameElements(parseableInputExpressionsWithNoRoot, actualParseableWithNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualParseableWithNoRoot.validOutputs)
    actualParseableWithNoRoot.invalidInputs shouldBe 'empty
    actualParseableWithNoRoot.invalidOutputs shouldBe 'empty

    val actualUnparseable = ExpressionParser.parseMCExpressions(toExpressionMap(unparseableInputExpressions), toExpressionMap(unparseableOutputExpressions), allowRootEntity = true, this)
    actualUnparseable.validInputs shouldBe 'empty
    actualUnparseable.validOutputs shouldBe 'empty
    actualUnparseable.invalidInputs should have size unparseableInputExpressions.size
    actualUnparseable.invalidOutputs should have size unparseableOutputExpressions.size

    val actualUnparseableWithNoRoot = ExpressionParser.parseMCExpressions(toExpressionMap(unparseableInputExpressionsWithNoRoot), toExpressionMap(unparseableOutputExpressionsWithNoRoot), allowRootEntity = false, this)
    actualUnparseableWithNoRoot.validInputs shouldBe 'empty
    actualUnparseableWithNoRoot.validOutputs shouldBe 'empty
    actualUnparseableWithNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualUnparseableWithNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size
  }
}
