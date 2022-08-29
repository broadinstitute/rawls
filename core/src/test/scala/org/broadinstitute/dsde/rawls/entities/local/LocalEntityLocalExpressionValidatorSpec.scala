package org.broadinstitute.dsde.rawls.entities.local

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.expressions.LocalExpressionFixture
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, Attributable, AttributeString, MethodConfiguration}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Seconds, Span}

class LocalEntityLocalExpressionValidatorSpec
    extends AnyFlatSpec
    with TestDriverComponent
    with LocalExpressionFixture
    with RawlsTestUtils
    with ScalaFutures {

  def toExpressionMap(expressions: Seq[String]): Map[String, AttributeString] =
    expressions.map(expr => expr.toString -> AttributeString(expr)).toMap

  def toMethodInput(tuple: (String, AttributeString)): MethodInput = tuple match {
    case (name, expr) =>
      val valueType = new ValueType()
      valueType.setTypeName(TypeNameEnum.STRING)
      val input = new ToolInputParameter()
      input.setName(name)
      input.setValueType(valueType)
      MethodInput(input, expr.value)
  }

  def toMethodInputs(methodConfiguration: MethodConfiguration): Seq[MethodInput] =
    methodConfiguration.inputs.map(toMethodInput).toSeq

  def toGatherInputs(exprs: Map[String, AttributeString]): GatherInputsResult = {
    val methodInputs = exprs.map(toMethodInput)
    GatherInputsResult(methodInputs.toSet, Set(), Set(), Set())
  }

  val allValid = MethodConfiguration(
    "dsde",
    "methodConfigValidExprs",
    Some("Sample"),
    prerequisites = None,
    inputs = toExpressionMap(parseableInputExpressions),
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1)
  )

  val allValidNoRootMC = allValid.copy(inputs = toExpressionMap(parseableInputExpressionsWithNoRoot),
                                       outputs = toExpressionMap(parseableOutputExpressionsWithNoRoot),
                                       rootEntityType = None
  )

  val allInvalid = MethodConfiguration(
    "dsde",
    "methodConfigInvalidExprs",
    Some("Sample"),
    prerequisites = None,
    inputs = toExpressionMap(unparseableInputExpressions),
    outputs = toExpressionMap(unparseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1)
  )

  val allInvalidNoRootMC = allInvalid.copy(inputs = toExpressionMap(unparseableInputExpressionsWithNoRoot),
                                           outputs = toExpressionMap(unparseableOutputExpressionsWithNoRoot),
                                           rootEntityType = None
  )

  val emptyExpr = "this.empty" -> AttributeString("")

  val oneEmpty = MethodConfiguration(
    "dsde",
    "methodConfigEmptyExpr",
    Some("Sample"),
    prerequisites = None,
    inputs = toExpressionMap(parseableInputExpressions) + emptyExpr,
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1)
  )

  val expressionValidator: ExpressionValidator = new LocalEntityExpressionValidator()

  it should "validateAndParseMCExpressions" in {
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))
    val actualValid = expressionValidator.validateMCExpressions(allValid, toGatherInputs(allValid.inputs)).futureValue
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot =
      expressionValidator.validateMCExpressions(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs)).futureValue
    assertSameElements(parseableInputExpressionsWithNoRoot, actualValidNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualValidNoRoot.validOutputs)
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    val actualInvalid =
      expressionValidator.validateMCExpressions(allInvalid, toGatherInputs(allInvalid.inputs)).futureValue
    actualInvalid.validInputs shouldBe 'empty
    actualInvalid.validOutputs shouldBe 'empty
    actualInvalid.invalidInputs should have size unparseableInputExpressions.size
    actualInvalid.invalidOutputs should have size unparseableOutputExpressions.size

    val actualInvalidNoRoot = expressionValidator
      .validateMCExpressions(allInvalidNoRootMC, toGatherInputs(allInvalidNoRootMC.inputs))
      .futureValue
    actualInvalidNoRoot.validInputs shouldBe 'empty
    actualInvalidNoRoot.validOutputs shouldBe 'empty
    actualInvalidNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualInvalidNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size

    val actualOneEmpty =
      expressionValidator.validateMCExpressions(oneEmpty, toGatherInputs(oneEmpty.inputs)).futureValue
    assertSameElements(parseableInputExpressions, actualOneEmpty.validInputs)
    assertSameElements(parseableOutputExpressions, actualOneEmpty.validOutputs)
    assertSameElements(Seq("this.empty"), actualOneEmpty.invalidInputs.keys)
    actualOneEmpty.invalidOutputs shouldBe 'empty
    actualOneEmpty.invalidInputs.size shouldBe 1

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs =
      GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty = expressionValidator.validateMCExpressions(oneEmpty, optionalGatherInputs).futureValue
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

  it should "validateExpressionsForSubmission" in {

    val actualValid =
      expressionValidator.validateExpressionsForSubmission(allValid, toGatherInputs(allValid.inputs)).futureValue.get
    assertSameElements(parseableInputExpressions, actualValid.validInputs)
    assertSameElements(parseableOutputExpressions, actualValid.validOutputs)
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = expressionValidator
      .validateExpressionsForSubmission(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs))
      .futureValue
      .get
    assertSameElements(parseableInputExpressionsWithNoRoot, actualValidNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualValidNoRoot.validOutputs)
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    // fail submission when given an empty non-optional input

    val actualOneEmpty =
      expressionValidator.validateExpressionsForSubmission(oneEmpty, toGatherInputs(oneEmpty.inputs)).futureValue
    actualOneEmpty shouldBe a[scala.util.Failure[_]]

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs =
      GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty =
      expressionValidator.validateExpressionsForSubmission(oneEmpty, optionalGatherInputs).futureValue.get
    assertSameElements(oneEmpty.inputs.keys, actualOptionalEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, actualOptionalEmpty.validOutputs)
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

  it should "parse method config expressions" in {
    val actualParseable = expressionValidator.validateMCExpressionsInternal(toExpressionMap(parseableInputExpressions),
                                                                            toExpressionMap(parseableOutputExpressions),
                                                                            defaultRootEntity
    )
    assertSameElements(parseableInputExpressions, actualParseable.validInputs)
    assertSameElements(parseableOutputExpressions, actualParseable.validOutputs)
    actualParseable.invalidInputs shouldBe 'empty
    actualParseable.invalidOutputs shouldBe 'empty

    val actualParseableWithNoRoot =
      expressionValidator.validateMCExpressionsInternal(toExpressionMap(parseableInputExpressionsWithNoRoot),
                                                        toExpressionMap(parseableOutputExpressionsWithNoRoot),
                                                        None
      )
    assertSameElements(parseableInputExpressionsWithNoRoot, actualParseableWithNoRoot.validInputs)
    assertSameElements(parseableOutputExpressionsWithNoRoot, actualParseableWithNoRoot.validOutputs)
    actualParseableWithNoRoot.invalidInputs shouldBe 'empty
    actualParseableWithNoRoot.invalidOutputs shouldBe 'empty

    val actualUnparseable =
      expressionValidator.validateMCExpressionsInternal(toExpressionMap(unparseableInputExpressions),
                                                        toExpressionMap(unparseableOutputExpressions),
                                                        defaultRootEntity
      )
    actualUnparseable.validInputs shouldBe 'empty
    actualUnparseable.validOutputs shouldBe 'empty
    actualUnparseable.invalidInputs should have size unparseableInputExpressions.size
    actualUnparseable.invalidOutputs should have size unparseableOutputExpressions.size

    val actualUnparseableWithNoRoot =
      expressionValidator.validateMCExpressionsInternal(toExpressionMap(unparseableInputExpressionsWithNoRoot),
                                                        toExpressionMap(unparseableOutputExpressionsWithNoRoot),
                                                        None
      )
    actualUnparseableWithNoRoot.validInputs shouldBe 'empty
    actualUnparseableWithNoRoot.validOutputs shouldBe 'empty
    actualUnparseableWithNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualUnparseableWithNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size
  }

  private val defaultRootEntity: Option[String] = Option("Sample")

  it should "validate output expressions" in {
    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.attribute").isSuccess,
           "this.attribute should parse correctly"
    )
    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this..attribute").isFailure,
           "this..attribute should not parse correctly"
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.chained.expression").isFailure,
      "this.chained.expression should not parse correctly"
    )

    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "workspace.attribute").isSuccess,
           "workspace.attribute should parse correctly"
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "workspace..attribute").isFailure,
      "workspace..attribute should not parse correctly"
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "workspace.chained.expression").isFailure,
      "workspace.chained.expression should not parse correctly"
    )

    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "bonk.attribute").isFailure,
           "bonk.attribute should not parse correctly"
    )

    assert(
      expressionValidator
        .validateOutputExpr(defaultRootEntity)(expression =
          s"this.${defaultRootEntity.get}${Attributable.entityIdAttributeSuffix}"
        )
        .isFailure,
      "this.sample_id should reject reserved attribute name"
    )
    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.name").isFailure,
           "this.name should reject reserved attribute name"
    )

    assert(expressionValidator.validateOutputExpr(None)(expression = "this.attribute").isFailure,
           "this.attribute should fail if no root entity"
    )
    assert(
      expressionValidator.validateOutputExpr(None)(expression = "workspace.attribute").isSuccess,
      "workspace.attribute should succeed even if root entity does not exist"
    )
  }

  it should "validate library output expressions" in {
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.library:attribute").isSuccess,
      "this.library:attribute should parse correctly"
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this..library:attribute").isFailure,
      "this..library:attribute should not parse correctly"
    )
    assert(
      expressionValidator
        .validateOutputExpr(defaultRootEntity)(expression = "this.library:chained.expression")
        .isFailure,
      "this.library:chained.expression should not parse correctly"
    )
    assert(
      expressionValidator
        .validateOutputExpr(defaultRootEntity)(expression = "this.chained.library:expression")
        .isFailure,
      "this.chained.library:expression should not parse correctly"
    )

    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "workspace.library:attribute").isSuccess,
      "workspace.library:attribute should parse correctly"
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "workspace..library:attribute").isFailure,
      "workspace..library:attribute should not parse correctly"
    )
    assert(
      expressionValidator
        .validateOutputExpr(defaultRootEntity)(expression = "workspace.library:chained.expression")
        .isFailure,
      "workspace.library:chained.expression should not parse correctly"
    )
    assert(
      expressionValidator
        .validateOutputExpr(defaultRootEntity)(expression = "workspace.chained.library:expression")
        .isFailure,
      "workspace.chained.library:expression should not parse correctly"
    )

    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "bonk.library:attribute").isFailure,
      "bonk.library:attribute should not parse correctly"
    )

    assert(expressionValidator.validateOutputExpr(None)(expression = "this.library:attribute").isFailure,
           "this.library:attribute should fail if no root entity"
    )
    assert(
      expressionValidator.validateOutputExpr(None)(expression = "workspace.library:attribute").isSuccess,
      "workspace.library:attribute should succeed even if root entity does not exist"
    )
  }
}
