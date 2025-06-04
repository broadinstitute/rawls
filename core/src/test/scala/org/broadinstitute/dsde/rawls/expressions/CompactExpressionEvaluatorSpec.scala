package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityQuery, CompactEntityRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.compact.{CompactEntityRepository, CompactEntitySerialization}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueList,
  Entity,
  MethodConfiguration,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue,
  WdlSource
}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.concurrent.ScalaFutures
import slick.dbio.DBIO

import scala.util.Random

class CompactExpressionEvaluatorSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with TableDrivenPropertyChecks
    with TestDriverComponent
    with MethodConfigTestSupport {

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
        ("this.type", List(AttributeLookup(List(), "type"))),
        ("blah", List()),
        ("\"blah\"", List()),
        ("workspace.string", List(AttributeLookup(List(), "string")))
      )

    forAll(straightForwardTests) { (input, result) =>
      compactExpressionEvaluator.parseLookups(input) shouldBe result
    }

    val relationTests =
      Table(
        ("input", "getText", "attributeName"),
        ("this.samples.type", "samples.", "type")
//        ("workspace.sample1ref.type", "sample1ref.", "type") // TODO do i need to implement workspace entities?
      )

    forAll(relationTests) { (input, getText, attributeName) =>
      val result: Seq[AttributeLookup] = compactExpressionEvaluator.parseLookups(input)
      result.size shouldBe 1
      result(0).relations.size shouldBe 1
      result(0).relations(0).getText shouldBe getText
      result(0).attributeName shouldBe attributeName
    }
  }

  // Copying from LocalEntityProviderSpec
  "evaluateExpressions" should "resolve method config inputs for a single entity" in withConfigData {
    // TODO probably check that it calls the query with the correct values
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleGood.name)
      )
    )
      .thenReturn(DBIO.successful(Some(sampleGoodAsCER)))

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
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               any(),
                                               org.mockito.ArgumentMatchers.eq("daSampleSet"),
                                               any(),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER),
              sampleMissingValueAsCER.name -> Seq(sampleMissingValueAsCER)
          )
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               any(),
                                               org.mockito.ArgumentMatchers.eq("daSampleSet2"),
                                               any(),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGoodAsCER.name -> Seq(sampleGoodAsCER), sampleGood2AsCER.name -> Seq(sampleGood2AsCER))
        )
      )

    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               any(),
                                               org.mockito.ArgumentMatchers.eq("daSampleSet4"),
                                               any(),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleWithSingleElementArrayAsCER.name -> Seq(sampleWithSingleElementArrayAsCER))
        )
      )

    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleSet.entityType),
                                  Some(sampleSet.name),
                                  Some("this.samples"),
                                  Some(sampleSet.entityType)
      )
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
      ExpressionEvaluationContext(Some(sampleSet2.entityType),
                                  Some(sampleSet2.name),
                                  Some("this.samples"),
                                  Some(sampleSet.entityType)
      )
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
      ExpressionEvaluationContext(Some(sampleSet4.entityType),
                                  Some(sampleSet4.name),
                                  Some("this.samples"),
                                  Some(sampleSet.entityType)
      )
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

  it should "return error on missing values" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleMissingValue.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleMissingValue.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(sampleMissingValueAsCER)
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

  // TODO why is this an empty list instead of an error
//  it should "resolve empty lists into AttributeEmptyLists" in withConfigData {
//    when(
//      mockQueries.getEntity(any(),
//                            org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
//                            org.mockito.ArgumentMatchers.eq(sampleSet2.name)
//      )
//    )
//      .thenReturn(
//        DBIO.successful(
//          Some(sampleSet2AsCER)
//        )
//      )
//    val context =
//      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), None, Some(sampleSet2.entityType))
//    val result = evalInputs(context, configEmptyArray, arrayWdl)
//    result should contain(
//      SubmissionValidationEntityInputs(
//        sampleSet2.name,
//        Set(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayNameWithWfName))
//      )
//    )
//
////    runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
////      Map(
////        sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayNameWithWfName))
////      )
//  }

//  it should "resolve empty lists into empty Array in nested WDL Struct" in withConfigData {
//    when(
//      mockQueries.getEntity(any(),
//                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.entityType),
//                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct.name)
//      )
//    )
//      .thenReturn(
//        DBIO.successful(
//          Some(sampleForWdlStructAsCER)
//        )
//      )
//    val context =
//      ExpressionEvaluationContext(Some(sampleForWdlStruct.entityType),
//                                  Some(sampleForWdlStruct.name),
//                                  None,
//                                  Some(sampleForWdlStruct.entityType)
//      )
//    val result = evalInputs(context, configNestedWdlStructWithEmptyList, wdlStructInputWdlWithNestedStruct)
//
//    val methodProps = result
//      .find(_.entityName == sampleForWdlStruct.name)
//      .map(_.inputResolutions.map { svv =>
//        svv.inputName -> svv.value.get
//      })
//      .getOrElse(Seq.empty)
//    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
//    wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[]},"id":101,"sample":"sample1","samples":[]}}"""
//  }

  it should "unpack AttributeValueRawJson into WDL-arrays" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(sampleSet2AsCER)
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

  // TODO fix "{"w1.aint_array":[[1,2]]}" was not equal to "{"w1.aint_array":[[[10,11,12],[1,2]]]}"
  it should "unpack array input expression with attribute reference into WDL-arrays" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleSet2.name),
                                               any(),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleGood.name -> Seq(sampleGoodAsCER), sampleGood2.name -> Seq(sampleGood2AsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleSet2.entityType),
                                  Some(sampleSet2.name),
                                  Some("this.samples"),
                                  Some(sampleSet2.entityType)
      )
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

  // TODO fix
  it should "correctly unpack wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(any(),
                                               org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.entityType),
                                               org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.name),
                                               any(),
                                               any()
      )
    )
      .thenReturn(
        DBIO.successful(
          Map(sampleWithSingleElementArray.name -> Seq(sampleWithSingleElementArrayAsCER))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleForWdlStruct2.entityType),
                                  Some(sampleForWdlStruct2.name),
                                  Some("this.samples"),
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

  // TODO fix None.get
  it should "correctly unpack nested wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleForWdlStruct2.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleForWdlStruct2))
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

  // TODO fix None.get
  it should "unpack wdl struct expression with attribute references into WDL Struct input" in withConfigData {
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
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleSet2))
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

  // TODO fix
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

  // TODO fix
  it should "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleSet2))
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

    val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

    wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}"""
  }

  // TODO fix
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

  // TODO fix
  it should "cast attribute numbers into strings for string inputs" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleGood.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleGood))
        )
      )
    val context =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, Some(sampleGood.entityType))
    val result = evalInputs(context, configStringArgFromNumberAttribute, stringWdl)
    result
      .find(_.entityName == sampleGood.name)
      .exists(_.inputResolutions.exists(v => v.inputName == intArgNameWithWfName && v.error.isDefined)) shouldBe true

    result should contain(
      SubmissionValidationEntityInputs(
        sampleGood.name,
        Set(
          SubmissionValidationValue(Some(AttributeString("1")), None, stringArgNameWithWfName)
        )
      )
    )
  }

  // TODO fix
  it should "cast attribute numbers into strings for string inputs via a set" in withConfigData {
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleSet2.name)
      )
    )
      .thenReturn(
        DBIO.successful(
          Some(toCompactEntityRecord(sampleSet2))
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

  "evaluateExpression" should "return attribute values for a simple attribute" in withConfigData {
    when(
      mockQueries.queryRelatedRecordsWithArray(
        any(),
        org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
        org.mockito.ArgumentMatchers.eq(sampleGood.name),
        any(),
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
        any(),
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
        any(),
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
        any(),
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
        any(),
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
    result should contain theSameElementsAs Seq(Seq(AttributeNumber(10), AttributeNumber(12), AttributeNumber(12)),
                                                Seq(AttributeNumber(1))
    )
  }

}
