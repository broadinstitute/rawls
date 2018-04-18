package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, AttributeString, MethodConfiguration}
import org.scalatest.FlatSpec
import wom.callable.Callable.{InputDefinition, InputDefinitionWithDefault, OptionalInputDefinition, RequiredInputDefinition}
import wom.types.WomStringType

class ExpressionValidatorSpec extends FlatSpec with TestDriverComponent with ExpressionFixture with RawlsTestUtils  {

  def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
    expressions.map { expr => expr.toString -> AttributeString(expr) }.toMap

  def toMethodInput(tuple: (String, AttributeString)): MethodInput = tuple match {
    case (name, expr) => MethodInput(RequiredInputDefinition(name, WomStringType), expr.value)
  }

  def toMethodInputs(methodConfiguration: MethodConfiguration) : Seq[MethodInput] = {
    methodConfiguration.inputs.map(toMethodInput).toSeq
  }

  val allValid = MethodConfiguration("dsde", "methodConfigValidExprs", Some("Sample"), prerequisites=Map.empty,
    inputs = toExpressionMap(parseableInputExpressions),
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  val allInvalid = MethodConfiguration("dsde", "methodConfigInvalidExprs", Some("Sample"), prerequisites=Map.empty,
    inputs = toExpressionMap(unparseableInputExpressions),
    outputs = toExpressionMap(unparseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  val emptyExpr = "this.empty" -> AttributeString("")

  val oneEmpty = MethodConfiguration("dsde", "methodConfigEmptyExpr", Some("Sample"), prerequisites=Map.empty,
    inputs = toExpressionMap(parseableInputExpressions) + emptyExpr,
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  "ExpressionValidator" should "validateAndParse" in {

    val actualValid = ExpressionValidator.validateAndParse(allValid, allValid.inputs, allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualInvalid = ExpressionValidator.validateAndParse(allInvalid, allInvalid.inputs, allowRootEntity = true, this)
    actualInvalid.validInputs shouldBe 'empty
    actualInvalid.validOutputs shouldBe 'empty
    actualInvalid.invalidInputs should have size unparseableInputExpressions.size
    actualInvalid.invalidOutputs should have size unparseableOutputExpressions.size

    val actualOneEmpty = ExpressionValidator.validateAndParse(oneEmpty, oneEmpty.inputs, allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualOneEmpty.validInputs)
    assertSameElements(parseableOutputExpressions, actualOneEmpty.validOutputs)
    assertSameElements(Seq("this.empty"), actualOneEmpty.invalidInputs.keys)
    actualOneEmpty.invalidOutputs shouldBe 'empty

    //FIXME: allowRootEntity = false tests
  }

  it should "validateAndParseMCExpressions" in {

    val actualValid = ExpressionValidator.validateAndParseMCExpressions(allValid,  allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualInvalid = ExpressionValidator.validateAndParseMCExpressions(allInvalid, allowRootEntity = true, this)
    actualInvalid.validInputs shouldBe 'empty
    actualInvalid.validOutputs shouldBe 'empty
    actualInvalid.invalidInputs should have size unparseableInputExpressions.size
    actualInvalid.invalidOutputs should have size unparseableOutputExpressions.size

    val actualOneEmpty = ExpressionValidator.validateAndParseMCExpressions(oneEmpty, allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualOneEmpty.validInputs)
    assertSameElements(parseableOutputExpressions, actualOneEmpty.validOutputs)
    assertSameElements(Seq("this.empty"), actualOneEmpty.invalidInputs.keys)
    actualOneEmpty.invalidOutputs shouldBe 'empty
  }

  it should "validateExpressionsForSubmission" in {

    val actualValid = ExpressionValidator.validateExpressionsForSubmission(allValid, toMethodInputs(allValid), Seq.empty, allowRootEntity = true, this).get
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    // fail submission when given an empty non-optional input

    val actualOneEmpty = ExpressionValidator.validateExpressionsForSubmission(oneEmpty, toMethodInputs(oneEmpty), Seq.empty, allowRootEntity = true, this)
    actualOneEmpty shouldBe a [scala.util.Failure[_]]

    // succeed if the empty input is optional

    val emptyOptionalInput = toMethodInput(emptyExpr)
    val inputsToProcess = toMethodInputs(oneEmpty).toSet - emptyOptionalInput

    val actualOptionalEmpty = ExpressionValidator.validateExpressionsForSubmission(oneEmpty, inputsToProcess.toSeq, Seq(emptyOptionalInput), allowRootEntity = true, this).get
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty

    //FIXME: allowRootEntity = false
  }

}
