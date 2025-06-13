package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityQuery, CompactEntityRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.compact.{CompactEntityRepository, CompactEntitySerialization}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.ExpressionLookup
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{
  AttributeNameContext,
  RelationContext
}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueEmptyList,
  AttributeValueList,
  AttributeValueRawJson,
  Entity,
  MethodConfiguration,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue,
  WdlSource
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.concurrent.ScalaFutures
import slick.dbio.DBIO

import scala.concurrent.duration._
import scala.util.{Random, Success}

class CompactExpressionEvaluatorSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with TableDrivenPropertyChecks
    with TestDriverComponent
    with MethodConfigTestSupport {

  // TODO this is just for debugging, remove or reduce
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 300.seconds, interval = 100.millis)

  val compactEntityRepository = mock[CompactEntityRepository]
  val mockQueries = mock[CompactEntityQuery]
  when(compactEntityRepository.queries).thenReturn(mockQueries)
  when(compactEntityRepository.dataSource).thenReturn(slickDataSource)

  val compactExpressionEvaluator = new CompactExpressionEvaluator(compactEntityRepository)

  // I didn't put this method on the Entity itself because that doesn't have workspace id or id or anything
  def toCompactEntityRecord(entity: Entity): CompactEntityRecord = CompactEntityRecord(
    Random.nextLong(),
    entity.name,
    entity.entityType,
    workspace.workspaceIdAsUUID,
    1L,
    false,
    Option(CompactEntitySerialization.toSql(entity.attributes).compactPrint)
  )

  val sampleGoodAsCER = toCompactEntityRecord(sampleGood)
  val sampleGood2AsCER = toCompactEntityRecord(sampleGood2)
  val sampleMissingValueAsCER = toCompactEntityRecord(sampleMissingValue)
  val sampleWithSingleElementArrayAsCER = toCompactEntityRecord(sampleWithSingleElementArray)
  val sampleSet2AsCER = toCompactEntityRecord(sampleSet2)
  val sampleForWdlStructAsCER = toCompactEntityRecord(sampleForWdlStruct)

  def evalInputs(
    context: ExpressionEvaluationContext,
    config: MethodConfiguration,
    wdl: WdlSource
  ): LazyList[SubmissionValidationEntityInputs] = {
    val gatherInputsResult =
      methodConfigResolver.gatherInputs(userInfo, config, wdl).get // throws if Failure
    compactExpressionEvaluator
      .evaluateExpressions(workspace.workspaceIdAsUUID, context, gatherInputsResult)
      .futureValue // blocks and returns result or throws
  }

  // Note: this is also essentially a test for CompactEvaluateVisitor
  "parseLookups" should "generate correct lookups" in {
    val straightForwardTests =
      Table(
        ("input", "result"),
        (
          "this.type",
          List(
            ExpressionLookup(
              expression = "this.type",
              relations = List(),
              attributeName = Some("type"),
              values = Seq.empty
            )
          )
        ),
        ("blah", List()),
        ("\"blah\"",
         List(
         )
        ),
        (
          "workspace.string",
          List(
            ExpressionLookup(
              expression = "workspace.string",
              relations = List(),
              attributeName = Some("string"),
              values = Seq.empty
            )
          )
        )
      )

    forAll(straightForwardTests) { (input, result) =>
      compactExpressionEvaluator.parseLookups(input) shouldBe result
    }

    val relationTests =
      Table(
        ("input", "getText", "attributeName"),
        ("this.samples.type", "samples.", Some("type")),
        (
          "[[10,11,12],this.samples.blah]",
          "samples.",
          Some("blah")
        )

//        ("workspace.sample1ref.type", "sample1ref.", "type") // TODO do i need to implement workspace entities?
      )

    forAll(relationTests) { (input, getText, attributeName) =>
      val result: Seq[ExpressionLookup] = compactExpressionEvaluator.parseLookups(input)
      result.size shouldBe 1
      result(0).relations.size shouldBe 1
      result(0).relations(0).getText shouldBe getText
      result(0).attributeName shouldBe attributeName
    }

    // TODO expand assertions
    val chainedResult: Seq[ExpressionLookup] = compactExpressionEvaluator.parseLookups("this.samples.participant.id")
    chainedResult.size shouldBe 1

    val complexResult: Seq[ExpressionLookup] =
      compactExpressionEvaluator.parseLookups("{\"id\": this.bar, \"this.samples\": this.samples.blah}")
    complexResult.size shouldBe 2

    val complexResult2: Seq[ExpressionLookup] =
      compactExpressionEvaluator.parseLookups("{\"foo\": this.foo, \"bar\": this.bar}")
    complexResult2.size shouldBe 2

  }

  // Test cases are taken from LocalEntityProviderSpec
  behavior of "evaluateExpressions"

  it should "resolve method config inputs for a single entity" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleGood.name),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER))
        )
      )

    val context =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, Some(sampleGood.entityType))
    val result = evalInputs(context, configGood, littleWdl)
    result should contain(
      SubmissionValidationEntityInputs(
        sampleGood.name,
        Set(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName))
      )
    )

    val context2 =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, Some(sampleGood.entityType))
    val result2 = evalInputs(context2, configEvenBetter, littleWdl)
    result2 should contain(
      SubmissionValidationEntityInputs(
        sampleGood.name,
        Set(
          SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName),
          SubmissionValidationValue(Some(AttributeNumber(1)), None, intOptNameWithWfName)
        )
      )
    )

  }

  it should "resolve method config inputs for a set entity" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), org.mockito.ArgumentMatchers.eq("daSampleSet"), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER),
              sampleMissingValueAsCER.name -> Seq(sampleMissingValueAsCER)
          )
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), org.mockito.ArgumentMatchers.eq("daSampleSet2"), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), org.mockito.ArgumentMatchers.eq("daSampleSet4"), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleWithSingleElementArrayAsCER.name -> Seq(sampleWithSingleElementArrayAsCER))
        )
      )

    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleSet.entityType), Some(sampleSet.name), None, Some(sampleSet.entityType))
    val result = evalInputs(expressionEvaluationContext, configSampleSet, arrayWdl)
    result should contain(
      SubmissionValidationEntityInputs(
        sampleSet.name,
        Set(
          SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1)))), None, intArrayNameWithWfName)
        )
      )
    )

    val expressionEvaluationContext2 =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet.entityType))
    val result2 = evalInputs(expressionEvaluationContext2, configSampleSet, arrayWdl)
    result2 should contain(
      SubmissionValidationEntityInputs(
        sampleSet2.name,
        Set(
          SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2)))),
                                    None,
                                    intArrayNameWithWfName
          )
        )
      )
    )

    val expressionEvaluationContext3 =
      ExpressionEvaluationContext(Some(sampleSet4.entityType), Some(sampleSet4.name), None, Some(sampleSet.entityType))
    val result3 = evalInputs(expressionEvaluationContext3, configSampleSet, arrayWdl)
    // attribute reference with 1 element array should resolve as AttributeValueList
    result3 should contain(
      SubmissionValidationEntityInputs(
        sampleSet4.name,
        Set(
          SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(101)))), None, intArrayNameWithWfName)
        )
      )
    )

  }

  it should "resolve method config inputs for a set entity with root entity single type" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), org.mockito.ArgumentMatchers.eq(sampleSet2.name), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )

    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleSet2.entityType),
                                  Some(sampleSet2.name),
                                  Some("this.samples"),
                                  Some(sampleGood.entityType)
      )
    val result = evalInputs(expressionEvaluationContext, configGood, littleWdl)
    result should contain theSameElementsAs Seq(
      SubmissionValidationEntityInputs(
        sampleGood.name,
        Set(
          SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName)
        )
      ),
      SubmissionValidationEntityInputs(
        sampleGood2.name,
        Set(
          SubmissionValidationValue(Some(AttributeNumber(2)), None, intArgNameWithWfName)
        )
      )
    )
  }

  it should "return error on missing values" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleMissingValue.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleMissingValue.name),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleMissingValue.name -> Seq(sampleMissingValueAsCER))
        )
      )

    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleMissingValue.entityType),
                                  Some(sampleMissingValue.name),
                                  None,
                                  Some(sampleMissingValue.entityType)
      )
    val result = evalInputs(expressionEvaluationContext, configGood, littleWdl)
    result
      .find(_.entityName == "sampleMissingValue")
      .exists(_.inputResolutions.exists(v => v.inputName == intArgNameWithWfName && v.error.isDefined)) shouldBe true
  }

  // TODO maybe also test error on missing entitytype and name?
  it should "error on missing input definitions" in withConfigData {
    // We don't use evalInputs here in order to catch and inspect the exception
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, None)
    val gatherInputsResult =
      methodConfigResolver.gatherInputs(userInfo, configMissingExpr, littleWdl).get

    val future = compactExpressionEvaluator
      .evaluateExpressions(workspace.workspaceIdAsUUID, expressionEvaluationContext, gatherInputsResult)

    val ex = future.failed.futureValue
    ex shouldBe a[RawlsExceptionWithErrorReport]
    ex.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.message should include("Missing rootEntityType")
  }

  it should "error on root entity type/expression evaluation mismatch" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), org.mockito.ArgumentMatchers.eq(sampleSet2.name), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )

    // root entity type:set, no entity expression, input expression: this.samples.something
    // SVV with error Expected single value for workflow input, but evaluated result set had multiple values
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))

    val result = evalInputs(expressionEvaluationContext, configSampleSetSingleInput, stringWdl)

    val errorResult = result
      .find(_.entityName == sampleSet2.name)
      .flatMap(_.inputResolutions.find(v => v.inputName == stringArgNameWithWfName && v.error.isDefined))

    errorResult shouldBe defined
    val errorMessage = errorResult.get.error.get
    errorMessage should include("Expected single value")

    // root entity type: set, entity expression: this.samples, input expression: this.samples.something
    // "The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type sample_set.)
    val expressionEvaluationContext2 =
      ExpressionEvaluationContext(Some(sampleSet2.entityType),
                                  Some(sampleSet2.name),
                                  Some("this.samples"),
                                  Some(sampleSet2.entityType)
      )
    val gatherInputsResult2 =
      methodConfigResolver.gatherInputs(userInfo, configSampleSet, arrayWdl).get

    val future2 = compactExpressionEvaluator
      .evaluateExpressions(workspace.workspaceIdAsUUID, expressionEvaluationContext2, gatherInputsResult2)

    val ex2 = future2.failed.futureValue
    ex2 shouldBe a[RawlsExceptionWithErrorReport]
    ex2.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.message should include(
      "matched only entities of the wrong type"
    )

  }

  it should "error on root entity type/input entity mismatch" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), org.mockito.ArgumentMatchers.eq("daSampleSet"), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER),
              sampleMissingValueAsCER.name -> Seq(sampleMissingValueAsCER)
          )
        )
      )

    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleSet.entityType), Some(sampleSet.name), None, Some(sampleGood.entityType))
    val gatherInputsResult =
      methodConfigResolver.gatherInputs(userInfo, configEvenBetter, littleWdl).get

    val future = compactExpressionEvaluator
      .evaluateExpressions(workspace.workspaceIdAsUUID, expressionEvaluationContext, gatherInputsResult)

    val ex = future.failed.futureValue
    ex shouldBe a[RawlsExceptionWithErrorReport]
    ex.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.message should include("expects an entity of type")

  }

  it should "resolve empty lists into AttributeEmptyLists" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.name),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleSet2.name -> Seq(sampleSet2AsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
    val result = evalInputs(context, configEmptyArray, arrayWdl)
    result should contain(
      SubmissionValidationEntityInputs(
        sampleSet2.name,
        Set(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayNameWithWfName))
      )
    )
  }

  it should "resolve empty lists into empty Array in nested WDL Struct" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.name),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleForWdlStruct.name -> Seq(sampleForWdlStructAsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct.entityType),
                                  Some(sampleForWdlStruct.name),
                                  None,
                                  Some(sampleForWdlStruct.entityType)
      )
    val result = evalInputs(context, configNestedWdlStructWithEmptyList, wdlStructInputWdlWithNestedStruct)

    val methodProps = result
      .find(_.entityName == sampleForWdlStruct.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)
    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
    wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[]},"id":101,"sample":"sample1","samples":[]}}"""
  }

  it should "unpack AttributeValueRawJson into WDL-arrays" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.name),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleSet2.name -> Seq(sampleSet2AsCER))
        )
      )

    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
    val result = evalInputs(context, configRawJsonDoubleArray, doubleArrayWdl)

    val methodProps = result
      .find(_.entityName == sampleSet2.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
  }

  it should "unpack array input expression with attribute reference into WDL-arrays" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.name),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleGood2.name -> Seq(sampleGood2AsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
    val result = evalInputs(context, configArrayWithAttrRef, doubleArrayWdl)

    val methodProps = result
      .find(_.entityName == sampleSet2.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"w1.aint_array":[[10,11,12],[1,2]]}"""
  }

  // Expression: """{"id":this.participant_id,"sample":"sample1","samples":this.samples.blah}"""
  it should "correctly unpack wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.name),
        org.mockito.ArgumentMatchers.eq(List("samples"))
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleWithSingleElementArray.name -> Seq(sampleWithSingleElementArrayAsCER))
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.name),
        org.mockito.ArgumentMatchers.eq(List())
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleForWdlStruct2.name -> Seq(toCompactEntityRecord(sampleForWdlStruct2)))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct2.entityType),
                                  Some(sampleForWdlStruct2.name),
                                  None,
                                  Some(sampleForWdlStruct2.entityType)
      )
    val result = evalInputs(context, configWdlStruct, wdlStructInputWdl)

    val methodProps = result
      .find(_.entityName == sampleForWdlStruct2.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
    wdlInputs shouldBe """{"wdlStructWf.obj":{"id":123,"sample":"sample1","samples":[101]}}"""
  }

  // Expression: """{"id":this.participant_id,"sample":"sample1","samples":this.samples.blah,"foo":{"bar":this.samples.blah}}"""
  it should "correctly unpack nested wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.name),
        org.mockito.ArgumentMatchers.eq(List("samples"))
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleWithSingleElementArray.name -> Seq(sampleWithSingleElementArrayAsCER))
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.name),
        org.mockito.ArgumentMatchers.eq(List())
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleForWdlStruct2.name -> Seq(toCompactEntityRecord(sampleForWdlStruct2)))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct2.entityType),
                                  Some(sampleForWdlStruct2.name),
                                  None,
                                  Some(sampleForWdlStruct2.entityType)
      )
    val result = evalInputs(context, configNestedWdlStruct, wdlStructInputWdlWithNestedStruct)

    val methodProps = result
      .find(_.entityName == sampleForWdlStruct2.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[101]},"id":123,"sample":"sample1","samples":[101]}}"""
  }

  // Expression: """{"id":this.participant_id,"sample":"sample1","samples":this.samples.blah}"""
  it should "unpack wdl struct expression with attribute references into WDL Struct input" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.entityType),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.name),
        org.mockito.ArgumentMatchers.eq(List("samples"))
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleGood2.name -> Seq(sampleGood2AsCER))
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.entityType),
        org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.name),
        org.mockito.ArgumentMatchers.eq(List())
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleForWdlStruct.name -> Seq(toCompactEntityRecord(sampleForWdlStruct)))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct.entityType),
                                  Some(sampleForWdlStruct.name),
                                  None,
                                  Some(sampleForWdlStruct.entityType)
      )
    val result = evalInputs(context, configWdlStruct, wdlStructInputWdl)

    val methodProps = result
      .find(_.entityName == sampleForWdlStruct.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"wdlStructWf.obj":{"id":101,"sample":"sample1","samples":[1,2]}}"""
  }

  it should "unpack AttributeValueRawJson into optional WDL-arrays" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleSet2.name),
        any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleSet2.name -> Seq(toCompactEntityRecord(sampleSet2)))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
    val result = evalInputs(context, configRawJsonDoubleArray, optionalDoubleArrayWdl)

    val methodProps = result
      .find(_.entityName == sampleSet2.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
  }

  it should "unpack nested Array into WDL Struct" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleForWdlStruct))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct.entityType),
                                  Some(sampleForWdlStruct.name),
                                  None,
                                  Some(sampleForWdlStruct.entityType)
      )
    val result = evalInputs(context, configNestedArrayWdlStruct, wdlStructInputWdlWithNestedArray)

    val methodProps = result
      .find(_.entityName == sampleForWdlStruct.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[[0,1,2],[3,4,5]]},"id":101,"sample":"sample1","samples":[[0,1,2],[3,4,5]]}}"""
  }

  it should "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleSet2.name),
        org.mockito.ArgumentMatchers.eq(List("samples"))
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleGood2.name -> Seq(sampleGood2AsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
    val result = evalInputs(context, configRawJsonTripleArray, tripleArrayWdl)

    val methodProps = result
      .find(_.entityName == sampleSet2.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)
    println(result)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}"""
  }

  it should "unpack triple Array into WDL Struct" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleForWdlStruct))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct.entityType),
                                  Some(sampleForWdlStruct.name),
                                  None,
                                  Some(sampleForWdlStruct.entityType)
      )
    val result = evalInputs(context, configTripleArrayWdlStruct, wdlStructInputWdlWithTripleArray)

    val methodProps = result
      .find(_.entityName == sampleForWdlStruct.name)
      .map(_.inputResolutions.map { svv =>
        svv.inputName -> svv.value.get
      })
      .getOrElse(Seq.empty)

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]},"id":101,"sample":"sample1","samples":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}}"""
  }

  it should "cast attribute numbers into strings for string inputs" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
        org.mockito.ArgumentMatchers.eq(sampleGood.name),
        any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGood.name -> Seq(sampleGoodAsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, Some(sampleGood.entityType))
    val result = evalInputs(context, configStringArgFromNumberAttribute, stringWdl)
    result
      .find(_.entityName == sampleGood.name)
      .exists(_.inputResolutions.exists(v => v.inputName == stringArgNameWithWfName)) shouldBe true

    result should contain(
      SubmissionValidationEntityInputs(
        sampleGood.name,
        Set(
          SubmissionValidationValue(Some(AttributeString("1")), None, stringArgNameWithWfName)
        )
      )
    )
  }

  it should "cast attribute numbers into strings for string inputs via a set" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
        org.mockito.ArgumentMatchers.eq(sampleSet2.name),
        org.mockito.ArgumentMatchers.eq(List("samples"))
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleGood2.name -> Seq(sampleGood2AsCER))
        )
      )

    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
    val result = evalInputs(context, configStringArgFromNumberAttributeViaSampleSet, arrayStringWdl)

    result should contain(
      SubmissionValidationEntityInputs(
        sampleSet2.name,
        Set(
          SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeString("1"), AttributeString("2")))),
                                    None,
                                    strArrayNameWithWfName
          )
        )
      )
    )
  }

  behavior of "evaluateExpression"

  it should "return attribute values for a simple attribute" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
        org.mockito.ArgumentMatchers.eq(sampleGood.name),
        any()
      )
    ).thenReturn(
      DBIO.successful(
        Map(sampleGood.name -> Seq(sampleGoodAsCER))
      )
    )

    val resultFut = compactExpressionEvaluator.evaluateExpression(
      workspace.workspaceIdAsUUID,
      "this.blah",
      sampleGood.entityType,
      sampleGood.name
    )

    val result = resultFut.futureValue
    result should contain only AttributeNumber(1)
  }

  it should "return all attribute values for an attribute reference in a set" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleSet.entityType),
        org.mockito.ArgumentMatchers.eq(sampleSet.name),
        any()
      )
    ).thenReturn(
      DBIO.successful(
        Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleGood2.name -> Seq(sampleGood2AsCER))
      )
    )

    val resultFut = compactExpressionEvaluator.evaluateExpression(
      workspace.workspaceIdAsUUID,
      "this.samples.blah",
      sampleSet.entityType,
      sampleSet.name
    )

    val result = resultFut.futureValue
    result should contain theSameElementsAs Seq(AttributeNumber(1), AttributeNumber(2))
  }

  // TODO is this an empty Seq or an error?
  it should "return an empty Seq if no attributes are found" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
        org.mockito.ArgumentMatchers.eq(sampleGood.name),
        any()
      )
    ).thenReturn(
      DBIO.successful(
        Map(sampleGood.name -> Seq(sampleGoodAsCER))
      )
    )

    val resultFut = compactExpressionEvaluator.evaluateExpression(
      workspace.workspaceIdAsUUID,
      "this.nonexistent",
      sampleGood.entityType,
      sampleGood.name
    )

    val result = resultFut.futureValue
    result shouldBe empty
  }

  it should "return all values from AttributeValueList attributes" in withConfigData {
    val entityWithList = sampleGood.copy(attributes =
      Map(AttributeName("default", "foo") -> AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2))))
    )
    val entityWithListCER = toCompactEntityRecord(entityWithList)

    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(entityWithList.entityType),
        org.mockito.ArgumentMatchers.eq(entityWithList.name),
        any()
      )
    ).thenReturn(
      DBIO.successful(
        Map(entityWithList.name -> Seq(entityWithListCER))
      )
    )

    val resultFut = compactExpressionEvaluator.evaluateExpression(
      workspace.workspaceIdAsUUID,
      "this.foo",
      entityWithList.entityType,
      entityWithList.name
    )

    val result = resultFut.futureValue
    result should contain theSameElementsAs Seq(AttributeNumber(1), AttributeNumber(2))
  }

  it should "return values from a mixed expression" in withConfigData {
    // TODO figure out what relations are passed in and refine both this and the actual call to the query
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleSet.entityType),
        org.mockito.ArgumentMatchers.eq(sampleSet.name),
        any()
      )
    ).thenReturn(
      DBIO.successful(
        Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleMissingValue.name -> Seq(sampleMissingValueAsCER))
      )
    )

    val resultFut = compactExpressionEvaluator.evaluateExpression(
      workspace.workspaceIdAsUUID,
      "[[10,11,12],this.samples.blah]",
      sampleSet.entityType,
      sampleSet.name
    )

    val result = resultFut.futureValue
    // [[10,11,12],[1]]
    result should contain only AttributeValueRawJson("[[10,11,12],1]")
  }

  behavior of "buildQueryPlans"

  it should "return a queryplan for each relation level" in {
    val mockSampleAttributeContext = Mockito.mock(classOf[AttributeNameContext])
    Mockito.when(mockSampleAttributeContext.getText).thenReturn("samples")
    val mockSampleRelationContext = Mockito.mock(classOf[RelationContext])
    Mockito.when(mockSampleRelationContext.attributeName()).thenReturn(mockSampleAttributeContext)
    val mockParticipantAttributeContext = Mockito.mock(classOf[AttributeNameContext])
    Mockito.when(mockParticipantAttributeContext.getText).thenReturn("participants")
    val mockParticipantRelationContext = Mockito.mock(classOf[RelationContext])
    Mockito.when(mockParticipantRelationContext.attributeName()).thenReturn(mockParticipantAttributeContext)
    val relationExpression = "this.samples.type"
    val relationExpressionLookup =
      ExpressionLookup(relationExpression, List(mockSampleRelationContext), Some("type"), Seq.empty)
    val plainExpression = "this.foo"
    val plainExpressionLookup = ExpressionLookup(plainExpression, List(), Some("foo"), Seq.empty)
    val chainedExpression = "this.samples.participants.id"
    val chainedExpressionLookup = ExpressionLookup(chainedExpression,
                                                   List(mockSampleRelationContext, mockParticipantRelationContext),
                                                   Some("id"),
                                                   Seq.empty
    )
    val complexExpression = "{\"id\": this.bar, \"this.samples\": this.samples.blah}"
    val complexExpressionLookup1 =
      ExpressionLookup(complexExpression, List(), Some("bar"), Seq.empty)
    val complexExpressionLookup2 =
      ExpressionLookup(complexExpression, List(mockSampleRelationContext), Some("blah"), Seq.empty)

    // The expressions above all together should result in 3 queries: one for the base entity, one for base -> samples, and one for base -> samples -> participants
    val result = compactExpressionEvaluator.buildQueryPlans(
      Seq(relationExpressionLookup,
          plainExpressionLookup,
          chainedExpressionLookup,
          complexExpressionLookup1,
          complexExpressionLookup2
      )
    )
    result.size shouldBe 3
    result should contain theSameElementsAs (Seq(
      QueryPlan(List(), Map(plainExpression -> Set("foo"), complexExpression -> Set("bar"))),
      QueryPlan(List("samples"), Map(relationExpression -> Set("type"), complexExpression -> Set("blah"))),
      QueryPlan(List("samples", "participants"), Map(chainedExpression -> Set("id")))
    ))

  }

  behavior of "executeQueryPlan"

  it should "get the attributes from the entities" in withConfigData {
    val expression = "this.samples.blah"
    val queryPlan = QueryPlan(List("samples"), Map(expression -> Set("blah")))

    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), any(), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )
    val result = compactExpressionEvaluator
      .executeQueryPlan(workspace.workspaceIdAsUUID, "sampleset", "sampleset1", "sampleset", queryPlan)
      .futureValue
    result.size shouldBe 1
    //  type ExpressionAndResult = (LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])
    result should contain theSameElementsAs (Seq(
      (expression, Map("sampleset1" -> Success(Seq(AttributeNumber(1), AttributeNumber(2)))))
    ))

  }

  it should "get multiple attributes from multiple expressions" in withConfigData {
    val expression1 = "this.rawJsonDoubleArray"
    val expression2 = "this.blah"
    val queryPlan =
      QueryPlan(List("samples"), Map(expression1 -> Set("rawJsonDoubleArray"), expression2 -> Set("blah")))

    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), any(), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )
    val result = compactExpressionEvaluator
      .executeQueryPlan(workspace.workspaceIdAsUUID, "sampleset", "sampleset1", "sampleset", queryPlan)
      .futureValue
    result.size shouldBe 2
    //  type ExpressionAndResult = (LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])
    result should contain theSameElementsAs (Seq(
      (expression2, Map("sampleset1" -> Success(Seq(AttributeNumber(1), AttributeNumber(2))))),
      (expression1,
       Map(
         "sampleset1" -> Success(
           Seq(AttributeValueRawJson("[[0,1,2],[3,4,5]]"), AttributeValueRawJson("[[3,4,5],[6,7,8]]"))
         )
       )
      )
    ))
  }

  it should "create separate results if entity type does not match root entity type" in withConfigData {
    val expression = "this.samples.blah"
    val queryPlan = QueryPlan(List("samples"), Map(expression -> Set("blah")))

    when(
      mockQueries.queryRelatedRecordsWithArray(any(), any(), any(), any())
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )
    val result = compactExpressionEvaluator
      .executeQueryPlan(workspace.workspaceIdAsUUID, "sampleset", "sampleset1", "Sample", queryPlan)
      .futureValue
    result.size shouldBe 1
    //  type ExpressionAndResult = (LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])
    result should contain theSameElementsAs (Seq(
      (expression,
       Map(sampleGood.name -> Success(Seq(AttributeNumber(1))), sampleGood2.name -> Success(Seq(AttributeNumber(2))))
      )
    ))
  }

}
