package org.broadinstitute.dsde.rawls.entities.datarepo

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.base.{ExpressionTypes, ParsedExpression}
import org.broadinstitute.dsde.rawls.expressions.DataRepoExpressionFixture
import org.broadinstitute.dsde.rawls.model.AttributeString
import org.scalatest.FlatSpec

class DataRepoExpressionParserSpec extends FlatSpec with TestDriverComponent with DataRepoExpressionFixture with RawlsTestUtils {

  // assumes that expressions have already been validated: see ExpressionValidatorSpec for that step

  it should "parse method config expressions using the old parseMCExpressions" in {
    def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
      expressions.map { expr => expr.toString -> AttributeString(expr) }.toMap

    val actualParseable = parseMCExpressions(toExpressionMap(parseableInputExpressions), toExpressionMap(parseableOutputExpressions), allowRootEntity = true)
    assertSameElements(parseableInputExpressions, actualParseable.validInputs)
    assertSameElements(parseableOutputExpressions, actualParseable.validOutputs)
    actualParseable.invalidInputs shouldBe 'empty
    actualParseable.invalidOutputs shouldBe 'empty

    val actualParseableWithNoRoot = parseMCExpressions(toExpressionMap(parseableInputExpressionsWithNoRoot), toExpressionMap(parseableOutputExpressionsWithNoRoot), allowRootEntity = false)
    assertSameElements(parseableInputExpressionsWithNoRoot, actualParseableWithNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualParseableWithNoRoot.validOutputs)
    actualParseableWithNoRoot.invalidInputs shouldBe 'empty
    actualParseableWithNoRoot.invalidOutputs shouldBe 'empty

    val actualUnparseable = parseMCExpressions(toExpressionMap(unparseableInputExpressions), toExpressionMap(unparseableOutputExpressions), allowRootEntity = true)
    actualUnparseable.validInputs shouldBe 'empty
    actualUnparseable.validOutputs shouldBe 'empty
    actualUnparseable.invalidInputs should have size unparseableInputExpressions.size
    actualUnparseable.invalidOutputs should have size unparseableOutputExpressions.size

    val actualUnparseableWithNoRoot = parseMCExpressions(toExpressionMap(unparseableInputExpressionsWithNoRoot), toExpressionMap(unparseableOutputExpressionsWithNoRoot), allowRootEntity = false)
    actualUnparseableWithNoRoot.validInputs shouldBe 'empty
    actualUnparseableWithNoRoot.validOutputs shouldBe 'empty
    actualUnparseableWithNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualUnparseableWithNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size
  }

  def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
    expressions.map { expr => expr.toString -> AttributeString(expr) }.toMap

  it should "parse workspace input expressions using the new parseMCExpressions" in {
    val parsedExpressions = parseMCExpressions(toExpressionMap(parseableInputWorkspaceExpressions))
    val successfulParsedExpressions = parsedExpressions.collect {
      case (_, expression@ParsedExpression(_, ExpressionTypes.Workspace, _)) => expression
    }
    successfulParsedExpressions should have size parseableInputWorkspaceExpressions.size
  }

  it should "parse entity input expressions using the new parseMCExpressions" in {
    val parsedExpressions = parseMCExpressions(toExpressionMap(parseableInputEntityExpressions))
    val successfulParsedExpressions = parsedExpressions.collect {
      case (_, expression@ParsedExpression(_, ExpressionTypes.Entity, _)) => expression
    }
    successfulParsedExpressions should have size parseableInputEntityExpressions.size
  }

  // todo: distinguish between unparseable and invalid inputs. some of these 'unparseable' input expressions are
  // actually easily parsed, but invalid from the ExpressionValidator standpoint ('this.bad|character'). we may need
  // some additional checks for some of these that are parsing fine but shouldn't be ('this.')
  ignore should "parse invalid input expressions using the new parseMCExpressions" in {
    val parsedExpressions = parseMCExpressions(toExpressionMap(unparseableInputExpressions))
    val failedParsedExpressions = parsedExpressions.collect {
      case (_, expression@ParsedExpression(_, ExpressionTypes.Invalid, _)) => expression
    }
    failedParsedExpressions should have size unparseableInputExpressions.size
  }


  //    val actualParseableWithNoRoot = parseMCExpressions(toExpressionMap(parseableInputExpressionsWithNoRoot), toExpressionMap(parseableOutputExpressionsWithNoRoot), allowRootEntity = false)
//    assertSameElements(parseableInputExpressionsWithNoRoot, actualParseableWithNoRoot.validInputs)
//    assertSameElements(parseableOutputExpressionsWithNoRoot, actualParseableWithNoRoot.validOutputs)
//    actualParseableWithNoRoot.invalidInputs shouldBe 'empty
//    actualParseableWithNoRoot.invalidOutputs shouldBe 'empty
//
//    val actualUnparseable = parseMCExpressions(toExpressionMap(unparseableInputExpressions), toExpressionMap(unparseableOutputExpressions), allowRootEntity = true)
//    actualUnparseable.validInputs shouldBe 'empty
//    actualUnparseable.validOutputs shouldBe 'empty
//    actualUnparseable.invalidInputs should have size unparseableInputExpressions.size
//    actualUnparseable.invalidOutputs should have size unparseableOutputExpressions.size
//
//    val actualUnparseableWithNoRoot = parseMCExpressions(toExpressionMap(unparseableInputExpressionsWithNoRoot), toExpressionMap(unparseableOutputExpressionsWithNoRoot), allowRootEntity = false)
//    actualUnparseableWithNoRoot.validInputs shouldBe 'empty
//    actualUnparseableWithNoRoot.validOutputs shouldBe 'empty
//    actualUnparseableWithNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
//    actualUnparseableWithNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size
}
