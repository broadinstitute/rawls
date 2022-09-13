package org.broadinstitute.dsde.rawls.entities.datarepo

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.expressions.DataRepoExpressionFixture
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, Attributable, AttributeString, MethodConfiguration}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Seconds, Span}

class DataRepoEntityExpressionValidatorSpec
    extends AnyFlatSpec
    with TestDriverComponent
    with RawlsTestUtils
    with ScalaFutures
    with DataRepoEntityProviderSpecSupport
    with DataRepoExpressionFixture {

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

  val provider: DataRepoEntityProvider = createTestProvider(snapshotModel = createSnapshotModel(defaultFixtureTables))
  val expressionValidator: ExpressionValidator = provider.expressionValidator

  val allValid = MethodConfiguration(
    "dsde",
    "methodConfigValidExprs",
    Some(defaultFixtureRootTableName),
    prerequisites = None,
    inputs = toExpressionMap(validInputExpressions),
    outputs = toExpressionMap(validOutputExpressions),
    AgoraMethod("dsde", "three_step", 1)
  )

  val allValidNoRootMC = allValid.copy(inputs = toExpressionMap(validInputExpressionsWithNoRoot),
                                       outputs = toExpressionMap(validWorkspaceOutputExpressions),
                                       rootEntityType = None
  )

  val allInvalid = MethodConfiguration(
    "dsde",
    "methodConfigInvalidExprs",
    Some(defaultFixtureRootTableName),
    prerequisites = None,
    inputs = toExpressionMap(badInputExpressionsWithRoot),
    outputs = toExpressionMap(invalidOutputExpressions),
    AgoraMethod("dsde", "three_step", 1)
  )

  val allInvalidNoRootMC = allInvalid.copy(inputs = toExpressionMap(badInputExpressionsWithNoRoot),
                                           outputs = toExpressionMap(invalidOutputExpressions),
                                           rootEntityType = None
  )

  val emptyExpr = "this.empty" -> AttributeString("")

  val oneEmpty = MethodConfiguration(
    "dsde",
    "methodConfigEmptyExpr",
    Some(defaultFixtureRootTableName),
    prerequisites = None,
    inputs = toExpressionMap(validInputExpressions) + emptyExpr,
    outputs = toExpressionMap(validOutputExpressions),
    AgoraMethod("dsde", "three_step", 1)
  )

  /** Set up for Relationships */
  val providerWithMultipleTables: DataRepoEntityProvider =
    createTestProvider(snapshotModel = createSnapshotModel(relationshipTables, relationships))
  val expressionValidatorWithMultipleTables: ExpressionValidator = providerWithMultipleTables.expressionValidator

  val allValidWithRelationships = MethodConfiguration(
    "dsde",
    "methodConfigValidExprs",
    Some(bookTableName),
    prerequisites = None,
    inputs = toExpressionMap(validInputExpressionsWithRelationships),
    outputs = toExpressionMap(validOutputExpressions), // output is always saved in workspace attributes
    AgoraMethod("dsde", "three_step", 1)
  )

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  "validateMCExpressions" should "validate expressions in a MethodConfiguration with a root entity" in {
    val validationResults =
      expressionValidator.validateMCExpressions(allValid, toGatherInputs(allValid.inputs)).futureValue
    assertSameElements(validInputExpressions, validationResults.validInputs)
    assertSameElements(validOutputExpressions, validationResults.validOutputs)
    validationResults.invalidInputs shouldBe 'empty
    validationResults.invalidOutputs shouldBe 'empty
  }

  it should "validate expressions in a MethodConfiguration without a root entity" in {
    val validationResultsNoRoot =
      expressionValidator.validateMCExpressions(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs)).futureValue
    assertSameElements(validInputExpressionsWithNoRoot, validationResultsNoRoot.validInputs)
    assertSameElements(validWorkspaceOutputExpressions, validationResultsNoRoot.validOutputs)
    validationResultsNoRoot.invalidInputs shouldBe 'empty
    validationResultsNoRoot.invalidOutputs shouldBe 'empty
  }

  it should "detect invalid expressions in a MethodConfiguration with a root entity" in {
    val validationResults =
      expressionValidator.validateMCExpressions(allInvalid, toGatherInputs(allInvalid.inputs)).futureValue
    validationResults.validInputs shouldBe 'empty
    validationResults.validOutputs shouldBe 'empty
    validationResults.invalidInputs should have size badInputExpressionsWithRoot.size
    validationResults.invalidOutputs should have size invalidOutputExpressions.size
  }

  it should "detect invalid expressions in a MethodConfiguration without a root entity" in {
    val validationResultsNoRoot = expressionValidator
      .validateMCExpressions(allInvalidNoRootMC, toGatherInputs(allInvalidNoRootMC.inputs))
      .futureValue
    validationResultsNoRoot.validInputs shouldBe 'empty
    validationResultsNoRoot.validOutputs shouldBe 'empty
    validationResultsNoRoot.invalidInputs should have size badInputExpressionsWithNoRoot.size
    validationResultsNoRoot.invalidOutputs should have size invalidOutputExpressions.size
  }

  it should "handle optional inputs" in {
    // fail if empty input is required
    val validationResultsOneEmpty =
      expressionValidator.validateMCExpressions(oneEmpty, toGatherInputs(oneEmpty.inputs)).futureValue
    assertSameElements(validInputExpressions, validationResultsOneEmpty.validInputs)
    assertSameElements(validOutputExpressions, validationResultsOneEmpty.validOutputs)
    assertSameElements(Seq("this.empty"), validationResultsOneEmpty.invalidInputs.keys)
    validationResultsOneEmpty.invalidOutputs shouldBe 'empty
    validationResultsOneEmpty.invalidInputs.size shouldBe 1

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs =
      GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val validationResultsEmpty = expressionValidator.validateMCExpressions(oneEmpty, optionalGatherInputs).futureValue
    assertSameElements(oneEmpty.inputs.keys, validationResultsEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, validationResultsEmpty.validOutputs)
    validationResultsEmpty.invalidInputs shouldBe 'empty
    validationResultsEmpty.invalidOutputs shouldBe 'empty
  }

  it should "validate the happy path for relationship traversals" in {
    val validationResults = expressionValidatorWithMultipleTables
      .validateMCExpressions(allValidWithRelationships, toGatherInputs(allValidWithRelationships.inputs))
      .futureValue
    assertSameElements(validInputExpressionsWithRelationships, validationResults.validInputs)
    assertSameElements(validOutputExpressions, validationResults.validOutputs)
    validationResults.invalidInputs shouldBe 'empty
    validationResults.invalidOutputs shouldBe 'empty
  }

  // should never get here if TDR is not buggy
  it should "fail if the linked table does not exist for relationship traversals" in {
    val providerWithMultipleTables =
      createTestProvider(snapshotModel = createSnapshotModel(defaultFixtureTables, relationships))
    val expressionValidatorWithMultipleTables: ExpressionValidator = providerWithMultipleTables.expressionValidator

    val validationResults = expressionValidatorWithMultipleTables
      .validateMCExpressions(allValidWithRelationships, toGatherInputs(allValidWithRelationships.inputs))
      .futureValue
    validationResults.invalidInputs.size shouldBe validEntityInputExpressionsWithRelationships.length
    validationResults.invalidOutputs shouldBe 'empty
  }

  it should "fail if the relationship does not exist for relationship traversals" in {
    val providerWithMultipleTables =
      createTestProvider(snapshotModel = createSnapshotModel(relationshipTables, List.empty))
    val expressionValidatorWithMultipleTables: ExpressionValidator = providerWithMultipleTables.expressionValidator

    val validationResults = expressionValidatorWithMultipleTables
      .validateMCExpressions(allValidWithRelationships, toGatherInputs(allValidWithRelationships.inputs))
      .futureValue
    validationResults.invalidInputs.size shouldBe validEntityInputExpressionsWithRelationships.length
    validationResults.invalidOutputs shouldBe 'empty
  }

  "validateExpressionsForSubmission" should "succeed for valid expressions in a MethodConfiguration with a root entity" in {
    val validationResults =
      expressionValidator.validateExpressionsForSubmission(allValid, toGatherInputs(allValid.inputs)).futureValue.get
    assertSameElements(validInputExpressions, validationResults.validInputs)
    assertSameElements(validOutputExpressions, validationResults.validOutputs)
    validationResults.invalidInputs shouldBe 'empty
    validationResults.invalidOutputs shouldBe 'empty
  }

  it should "succeed for valid expressions in a MethodConfiguration without a root entity" in {
    val validationResultsNoRoot = expressionValidator
      .validateExpressionsForSubmission(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs))
      .futureValue
      .get
    assertSameElements(validInputExpressionsWithNoRoot, validationResultsNoRoot.validInputs)
    assertSameElements(validWorkspaceOutputExpressions, validationResultsNoRoot.validOutputs)
    validationResultsNoRoot.invalidInputs shouldBe 'empty
    validationResultsNoRoot.invalidOutputs shouldBe 'empty
  }

  it should "succeed for empty optional input expressions in a MethodConfiguration" in {
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs =
      GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val validationResultsEmpty =
      expressionValidator.validateExpressionsForSubmission(oneEmpty, optionalGatherInputs).futureValue.get
    assertSameElements(oneEmpty.inputs.keys, validationResultsEmpty.validInputs)
    assertSameElements(oneEmpty.outputs.keys, validationResultsEmpty.validOutputs)
    validationResultsEmpty.invalidInputs shouldBe 'empty
    validationResultsEmpty.invalidOutputs shouldBe 'empty
  }

  it should "succeed for relationship traversals" in {
    val validationResults = expressionValidatorWithMultipleTables
      .validateExpressionsForSubmission(allValidWithRelationships, toGatherInputs(allValidWithRelationships.inputs))
      .futureValue
      .get
    assertSameElements(validInputExpressionsWithRelationships, validationResults.validInputs)
    assertSameElements(validOutputExpressions, validationResults.validOutputs)
    validationResults.invalidInputs shouldBe 'empty
    validationResults.invalidOutputs shouldBe 'empty
  }

  it should "fail for invalid expressions in a MethodConfiguration with a root entity" in {
    val validationResults =
      expressionValidator.validateExpressionsForSubmission(allInvalid, toGatherInputs(allInvalid.inputs)).futureValue
    validationResults shouldBe a[scala.util.Failure[_]]
  }

  it should "fail for invalid expressions in a MethodConfiguration without a root entity" in {
    val validationResultsNoRoot = expressionValidator
      .validateExpressionsForSubmission(allInvalidNoRootMC, toGatherInputs(allInvalidNoRootMC.inputs))
      .futureValue
    validationResultsNoRoot shouldBe a[scala.util.Failure[_]]
  }

  it should "fail for empty required input expressions in a MethodConfiguration" in {
    val validationResultsOneEmpty =
      expressionValidator.validateExpressionsForSubmission(oneEmpty, toGatherInputs(oneEmpty.inputs)).futureValue
    validationResultsOneEmpty shouldBe a[scala.util.Failure[_]]
  }

  // should never get here if TDR is not buggy
  it should "fail if the linked table does not exist for relationship traversals" in {
    val providerWithMultipleTables =
      createTestProvider(snapshotModel = createSnapshotModel(defaultFixtureTables, relationships))
    val expressionValidatorWithMultipleTables: ExpressionValidator = providerWithMultipleTables.expressionValidator

    val validationResults = expressionValidatorWithMultipleTables
      .validateExpressionsForSubmission(allValidWithRelationships, toGatherInputs(allValidWithRelationships.inputs))
      .futureValue
    validationResults shouldBe a[scala.util.Failure[_]]
  }

  it should "fail if the relationship does not exist for relationship traversals" in {
    val providerWithMultipleTables =
      createTestProvider(snapshotModel = createSnapshotModel(relationshipTables, List.empty))
    val expressionValidatorWithMultipleTables: ExpressionValidator = providerWithMultipleTables.expressionValidator

    val validationResults = expressionValidatorWithMultipleTables
      .validateExpressionsForSubmission(allValidWithRelationships, toGatherInputs(allValidWithRelationships.inputs))
      .futureValue
    validationResults shouldBe a[scala.util.Failure[_]]
  }

  private val defaultRootEntity: Option[String] = Option("Sample")

  "validateOutputExpr" should "fail for entity output expressions" in {
    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.attribute").isFailure,
           "output entity expressions not permitted with TDR"
    )
    assert(expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this..attribute").isFailure,
           "this..attribute should not parse correctly"
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.chained.expression").isFailure,
      "this.chained.expression should not parse correctly"
    )
    assert(expressionValidator.validateOutputExpr(None)(expression = "this.attribute").isFailure,
           "this.attribute should fail if no root entity"
    )
  }

  it should "validate workspace entity output expressions" in {
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
    assert(
      expressionValidator.validateOutputExpr(None)(expression = "workspace.attribute").isSuccess,
      "workspace.attribute should succeed even if root entity does not exist"
    )
  }

  it should "handle other invalid output expressions" in {
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "bonk.attribute").isFailure,
      "expressions must start with \"this\" or \"workspace\""
    )
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "bonk.library:attribute").isFailure,
      "bonk.library:attribute should not parse correctly"
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
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.invalid.name").isFailure,
      "this.invalid.name should not find invalid relationship"
    )
  }

  it should "fail for library entity output expressions" in {
    assert(
      expressionValidator.validateOutputExpr(defaultRootEntity)(expression = "this.library:attribute").isFailure,
      "output entity expressions not permitted with TDR"
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
    assert(expressionValidator.validateOutputExpr(None)(expression = "this.library:attribute").isFailure,
           "this.library:attribute should fail if no root entity"
    )
  }

  it should "validate library workspace entity output expressions" in {
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
      expressionValidator.validateOutputExpr(None)(expression = "workspace.library:attribute").isSuccess,
      "workspace.library:attribute should succeed even if root entity does not exist"
    )
  }
}
