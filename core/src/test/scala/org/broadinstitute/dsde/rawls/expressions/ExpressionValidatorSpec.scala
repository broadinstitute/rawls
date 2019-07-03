package org.broadinstitute.dsde.rawls.expressions

import cromwell.client.model.{ToolInputParameter, ValueType}
import cromwell.client.model.ValueType.TypeNameEnum
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, AttributeString, MethodConfiguration}
import org.scalatest.FlatSpec

class ExpressionValidatorSpec extends FlatSpec with TestDriverComponent with ExpressionFixture with RawlsTestUtils  {

  def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
    expressions.map { expr => expr.toString -> AttributeString(expr) }.toMap

  def toMethodInput(tuple: (String, AttributeString)): MethodInput = tuple match {
    case (name, expr) => {
      val valueType = new ValueType()
      valueType.setTypeName(TypeNameEnum.STRING)
      val input = new ToolInputParameter()
      input.setName(name)
      input.setValueType(valueType)
      MethodInput(input, expr.value)
    }
  }

  def toMethodInputs(methodConfiguration: MethodConfiguration) : Seq[MethodInput] = {
    methodConfiguration.inputs.map(toMethodInput).toSeq
  }

  def toGatherInputs(exprs: Map[String, AttributeString]): GatherInputsResult = {
    val methodInputs = exprs.map(toMethodInput)
    GatherInputsResult(methodInputs.toSet, Set(), Set(), Set())
  }

  val allValid = MethodConfiguration("dsde", "methodConfigValidExprs", Some("Sample"), prerequisites=None,
    inputs = toExpressionMap(parseableInputExpressions),
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  val allValidNoRootMC = allValid.copy(inputs = toExpressionMap(parseableInputExpressionsWithNoRoot), outputs = toExpressionMap(parseableOutputExpressionsWithNoRoot), rootEntityType = None)

  val allInvalid = MethodConfiguration("dsde", "methodConfigInvalidExprs", Some("Sample"), prerequisites=None,
    inputs = toExpressionMap(unparseableInputExpressions),
    outputs = toExpressionMap(unparseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  val allInvalidNoRootMC = allInvalid.copy(inputs = toExpressionMap(unparseableInputExpressionsWithNoRoot), outputs = toExpressionMap(unparseableOutputExpressionsWithNoRoot), rootEntityType = None)

  val emptyExpr = "this.empty" -> AttributeString("")

  val oneEmpty = MethodConfiguration("dsde", "methodConfigEmptyExpr", Some("Sample"), prerequisites=None,
    inputs = toExpressionMap(parseableInputExpressions) + emptyExpr,
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  it should "validateAndParseMCExpressions" in {

    val actualValid = ExpressionValidator.validateAndParseMCExpressions(allValid, toGatherInputs(allValid.inputs), allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = ExpressionValidator.validateAndParseMCExpressions(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs), allowRootEntity = false, this)
    assertSameElements(parseableInputExpressionsWithNoRoot, actualValidNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualValidNoRoot.validOutputs)
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    val actualInvalid = ExpressionValidator.validateAndParseMCExpressions(allInvalid, toGatherInputs(allInvalid.inputs), allowRootEntity = true, this)
    actualInvalid.validInputs shouldBe 'empty
    actualInvalid.validOutputs shouldBe 'empty
    actualInvalid.invalidInputs should have size unparseableInputExpressions.size
    actualInvalid.invalidOutputs should have size unparseableOutputExpressions.size

    val actualInvalidNoRoot = ExpressionValidator.validateAndParseMCExpressions(allInvalidNoRootMC, toGatherInputs(allInvalidNoRootMC.inputs), allowRootEntity = false, this)
    actualInvalidNoRoot.validInputs shouldBe 'empty
    actualInvalidNoRoot.validOutputs shouldBe 'empty
    actualInvalidNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualInvalidNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size

    val actualOneEmpty = ExpressionValidator.validateAndParseMCExpressions(oneEmpty, toGatherInputs(oneEmpty.inputs), allowRootEntity = true, this)
    assertSameElements(parseableInputExpressions, actualOneEmpty.validInputs)
    assertSameElements(parseableOutputExpressions, actualOneEmpty.validOutputs)
    assertSameElements(Seq("this.empty"), actualOneEmpty.invalidInputs.keys)
    actualOneEmpty.invalidOutputs shouldBe 'empty
    actualOneEmpty.invalidInputs.size shouldBe 1

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs = GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty = ExpressionValidator.validateAndParseMCExpressions(oneEmpty, optionalGatherInputs, allowRootEntity = true, this)
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

  it should "validateExpressionsForSubmission" in {

    val actualValid = ExpressionValidator.validateExpressionsForSubmission(allValid, toGatherInputs(allValid.inputs), allowRootEntity = true, this).get
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = ExpressionValidator.validateExpressionsForSubmission(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs), allowRootEntity = false, this).get
    assertSameElements(parseableInputExpressionsWithNoRoot, actualValidNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualValidNoRoot.validOutputs)
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    // fail submission when given an empty non-optional input

    val actualOneEmpty = ExpressionValidator.validateExpressionsForSubmission(oneEmpty, toGatherInputs(oneEmpty.inputs), allowRootEntity = true, this)
    actualOneEmpty shouldBe a [scala.util.Failure[_]]

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs = GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty = ExpressionValidator.validateExpressionsForSubmission(oneEmpty, optionalGatherInputs, allowRootEntity = true, this).get
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

}
