package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.expressions.ExpressionFixture
import org.broadinstitute.dsde.rawls.model.AttributeString
import org.scalatest.FlatSpec

class LocalEntityExpressionParserSpec extends FlatSpec with TestDriverComponent with ExpressionFixture with RawlsTestUtils  {

  // assumes that expressions have already been validated: see ExpressionValidatorSpec for that step

  it should "parse method config expressions" in {
    def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
      expressions.map { expr => expr -> AttributeString(expr) }.toMap

    val actualParseable = parseMCExpressions(
      inputs = toExpressionMap(parseableInputExpressions),
      outputs = toExpressionMap(parseableOutputExpressions),
      allowRootEntity = true,
      rootEntityTypeOption = None
    )
    assertSameElements(parseableInputExpressions, actualParseable.validInputs)
    assertSameElements(parseableOutputExpressions, actualParseable.validOutputs)
    actualParseable.invalidInputs shouldBe 'empty
    actualParseable.invalidOutputs shouldBe 'empty

    val actualParseableWithNoRoot = parseMCExpressions(
      inputs = toExpressionMap(parseableInputExpressionsWithNoRoot),
      outputs = toExpressionMap(parseableOutputExpressionsWithNoRoot),
      allowRootEntity = false,
      rootEntityTypeOption = None
    )
    assertSameElements(parseableInputExpressionsWithNoRoot, actualParseableWithNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualParseableWithNoRoot.validOutputs)
    actualParseableWithNoRoot.invalidInputs shouldBe 'empty
    actualParseableWithNoRoot.invalidOutputs shouldBe 'empty

    val actualUnparseable = parseMCExpressions(
      inputs = toExpressionMap(unparseableInputExpressions),
      outputs = toExpressionMap(unparseableOutputExpressions),
      allowRootEntity = true,
      rootEntityTypeOption = None
    )
    actualUnparseable.validInputs shouldBe 'empty
    actualUnparseable.validOutputs shouldBe 'empty
    actualUnparseable.invalidInputs should have size unparseableInputExpressions.size
    actualUnparseable.invalidOutputs should have size unparseableOutputExpressions.size

    val actualUnparseableWithNoRoot = parseMCExpressions(
      inputs = toExpressionMap(unparseableInputExpressionsWithNoRoot),
      outputs = toExpressionMap(unparseableOutputExpressionsWithNoRoot),
      allowRootEntity = false,
      rootEntityTypeOption = None
    )
    actualUnparseableWithNoRoot.validInputs shouldBe 'empty
    actualUnparseableWithNoRoot.validOutputs shouldBe 'empty
    actualUnparseableWithNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualUnparseableWithNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size
  }
}
