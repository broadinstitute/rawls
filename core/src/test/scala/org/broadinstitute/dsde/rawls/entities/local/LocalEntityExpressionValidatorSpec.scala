package org.broadinstitute.dsde.rawls.entities.local

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.expressions.ExpressionFixture
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, AttributeString, MethodConfiguration}
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures

class LocalEntityExpressionValidatorSpec extends FlatSpec with TestDriverComponent with ExpressionFixture with RawlsTestUtils with ScalaFutures  {

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

  val expressionValidator = new LocalEntityExpressionValidator(slickDataSource)

  it should "validateAndParseMCExpressions" in {

    val actualValid = expressionValidator.validateAndParseMCExpressions(allValid, toGatherInputs(allValid.inputs), allowRootEntity = true).futureValue
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = expressionValidator.validateAndParseMCExpressions(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs), allowRootEntity = false).futureValue
    assertSameElements(parseableInputExpressionsWithNoRoot, actualValidNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualValidNoRoot.validOutputs)
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    val actualInvalid = expressionValidator.validateAndParseMCExpressions(allInvalid, toGatherInputs(allInvalid.inputs), allowRootEntity = true).futureValue
    actualInvalid.validInputs shouldBe 'empty
    actualInvalid.validOutputs shouldBe 'empty
    actualInvalid.invalidInputs should have size unparseableInputExpressions.size
    actualInvalid.invalidOutputs should have size unparseableOutputExpressions.size

    val actualInvalidNoRoot = expressionValidator.validateAndParseMCExpressions(allInvalidNoRootMC, toGatherInputs(allInvalidNoRootMC.inputs), allowRootEntity = false).futureValue
    actualInvalidNoRoot.validInputs shouldBe 'empty
    actualInvalidNoRoot.validOutputs shouldBe 'empty
    actualInvalidNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualInvalidNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size

    val actualOneEmpty = expressionValidator.validateAndParseMCExpressions(oneEmpty, toGatherInputs(oneEmpty.inputs), allowRootEntity = true).futureValue
    assertSameElements(parseableInputExpressions, actualOneEmpty.validInputs)
    assertSameElements(parseableOutputExpressions, actualOneEmpty.validOutputs)
    assertSameElements(Seq("this.empty"), actualOneEmpty.invalidInputs.keys)
    actualOneEmpty.invalidOutputs shouldBe 'empty
    actualOneEmpty.invalidInputs.size shouldBe 1

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs = GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty = expressionValidator.validateAndParseMCExpressions(oneEmpty, optionalGatherInputs, allowRootEntity = true).futureValue
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

  it should "validateExpressionsForSubmission" in {

    val actualValid = expressionValidator.validateExpressionsForSubmission(allValid, toGatherInputs(allValid.inputs), allowRootEntity = true).futureValue.get
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = expressionValidator.validateExpressionsForSubmission(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs), allowRootEntity = false).futureValue.get
    assertSameElements(parseableInputExpressionsWithNoRoot, actualValidNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualValidNoRoot.validOutputs)
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    // fail submission when given an empty non-optional input

    val actualOneEmpty = expressionValidator.validateExpressionsForSubmission(oneEmpty, toGatherInputs(oneEmpty.inputs), allowRootEntity = true).futureValue
    actualOneEmpty shouldBe a [scala.util.Failure[_]]

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs = GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty = expressionValidator.validateExpressionsForSubmission(oneEmpty, optionalGatherInputs, allowRootEntity = true).futureValue.get
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

}
