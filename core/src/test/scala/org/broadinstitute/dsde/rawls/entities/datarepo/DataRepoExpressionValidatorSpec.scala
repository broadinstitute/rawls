package org.broadinstitute.dsde.rawls.entities.datarepo

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.expressions.DataRepoExpressionFixture
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AgoraMethod, AttributeString, MethodConfiguration}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

class DataRepoExpressionValidatorSpec extends FlatSpec with DataRepoExpressionFixture with ScalaFutures with DataRepoEntityProviderSpecSupport with Matchers {

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
  val tableName = "rootEntityTable"
  val testSchema = List((tableName, List("gvcf", "library:cohort", "library:cohort1", "arbitrary:whatever", "underscores_are_ok", "_")))
  val snapshotModel = createSnapshotModel(testSchema)
  val expressionValidator = new DataRepoExpressionValidator(new DataRepoExpressionParser(), snapshotModel)
  implicit val executionContext = TestExecutionContext.testExecutionContext

  val allValid = MethodConfiguration("dsde", "methodConfigValidExprs", Some(tableName), prerequisites=None,
    inputs = toExpressionMap(parseableInputExpressions),
    outputs = toExpressionMap(parseableOutputExpressions),
    AgoraMethod("dsde", "three_step", 1))

  val allValidNoRootMC = allValid.copy(inputs = toExpressionMap(parseableInputExpressionsWithNoRoot), outputs = toExpressionMap(parseableOutputExpressionsWithNoRoot), rootEntityType = None)

  val allInvalid = MethodConfiguration("dsde", "methodConfigInvalidExprs", Some(tableName), prerequisites=None,
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

    val actualValid = expressionValidator.validateMCExpressions(allValid, toGatherInputs(allValid.inputs), allowRootEntity = true).futureValue
    println(s"actualValidIN: ${actualValid.validInputs}")
    println(s"actualValidOUT: ${actualValid.validOutputs}")
    println(s"actualInValidIN: ${actualValid.invalidInputs}")
    println(s"actualInValidOUT: ${actualValid.invalidOutputs}")
    parseableInputExpressions should contain theSameElementsAs actualValid.validInputs
    parseableOutputExpressions should contain theSameElementsAs actualValid.validOutputs
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = expressionValidator.validateMCExpressions(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs), allowRootEntity = false).futureValue
    parseableInputExpressionsWithNoRoot should contain theSameElementsAs actualValidNoRoot.validInputs
    parseableOutputExpressionsWithNoRoot should contain theSameElementsAs actualValidNoRoot.validOutputs
    actualValidNoRoot.invalidInputs shouldBe 'empty
    actualValidNoRoot.invalidOutputs shouldBe 'empty

    val actualInvalid = expressionValidator.validateMCExpressions(allInvalid, toGatherInputs(allInvalid.inputs), allowRootEntity = true).futureValue
    actualInvalid.validInputs shouldBe 'empty
    actualInvalid.validOutputs shouldBe 'empty
    actualInvalid.invalidInputs should have size unparseableInputExpressions.size
    actualInvalid.invalidOutputs should have size unparseableOutputExpressions.size

    val actualInvalidNoRoot = expressionValidator.validateMCExpressions(allInvalidNoRootMC, toGatherInputs(allInvalidNoRootMC.inputs), allowRootEntity = false).futureValue
    actualInvalidNoRoot.validInputs shouldBe 'empty
    actualInvalidNoRoot.validOutputs shouldBe 'empty
    actualInvalidNoRoot.invalidInputs should have size unparseableInputExpressionsWithNoRoot.size
    actualInvalidNoRoot.invalidOutputs should have size unparseableOutputExpressionsWithNoRoot.size

    val actualOneEmpty = expressionValidator.validateMCExpressions(oneEmpty, toGatherInputs(oneEmpty.inputs), allowRootEntity = true).futureValue
    parseableInputExpressions should contain theSameElementsAs actualOneEmpty.validInputs
    parseableOutputExpressions should contain theSameElementsAs actualOneEmpty.validOutputs
    Seq("this.empty") should contain theSameElementsAs actualOneEmpty.invalidInputs.keys
    actualOneEmpty.invalidOutputs shouldBe 'empty
    actualOneEmpty.invalidInputs.size shouldBe 1

    // succeed if the empty input is optional
    val methodInputs = oneEmpty.inputs.map(toMethodInput)
    val emptyOptionalInput = Set(toMethodInput(emptyExpr))
    val optionalGatherInputs = GatherInputsResult(methodInputs.toSet diff emptyOptionalInput, emptyOptionalInput, Set(), Set())

    val actualOptionalEmpty = expressionValidator.validateMCExpressions(oneEmpty, optionalGatherInputs, allowRootEntity = true).futureValue
    oneEmpty.inputs.keys should contain theSameElementsAs actualOptionalEmpty.validInputs
    oneEmpty.outputs.keys should contain theSameElementsAs actualOptionalEmpty.validOutputs
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

  it should "validateExpressionsForSubmission" in {

    val actualValid = expressionValidator.validateExpressionsForSubmission(allValid, toGatherInputs(allValid.inputs), allowRootEntity = true).futureValue.get
    parseableInputExpressions should contain theSameElementsAs actualValid.validInputs
    parseableOutputExpressions should contain theSameElementsAs actualValid.validOutputs
    actualValid.invalidInputs shouldBe 'empty
    actualValid.invalidOutputs shouldBe 'empty

    val actualValidNoRoot = expressionValidator.validateExpressionsForSubmission(allValidNoRootMC, toGatherInputs(allValidNoRootMC.inputs), allowRootEntity = false).futureValue.get
    parseableInputExpressionsWithNoRoot should contain theSameElementsAs actualValidNoRoot.validInputs
    parseableOutputExpressionsWithNoRoot should contain theSameElementsAs actualValidNoRoot.validOutputs
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
    oneEmpty.inputs.keys should contain theSameElementsAs actualOptionalEmpty.validInputs
    oneEmpty.outputs.keys should contain theSameElementsAs actualOptionalEmpty.validOutputs
    actualOptionalEmpty.invalidInputs shouldBe 'empty
    actualOptionalEmpty.invalidOutputs shouldBe 'empty
  }

}
