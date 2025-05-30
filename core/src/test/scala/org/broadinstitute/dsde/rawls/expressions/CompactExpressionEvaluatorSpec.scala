package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  CompactEntityQuery,
  CompactEntityRecord,
  ReadAction,
  TestDriverComponent
}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.compact.{CompactEntityRepository, CompactEntitySerialization}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.model.{
  AttributeNull,
  AttributeNumber,
  AttributeValue,
  AttributeValueList,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue
}
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar.mock

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.scalatest.concurrent.ScalaFutures
import slick.dbio.DBIO

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

    // TODO is there a better way
    val sampleGoodAsCER = CompactEntityRecord(
      1L,
      sampleGood.name,
      sampleGood.entityType,
      workspace.workspaceIdAsUUID,
      1L,
      false,
      Option(CompactEntitySerialization.toSql(sampleGood.attributes).compactPrint)
    )
    when(
      mockQueries.getEntity(any(),
                            org.mockito.ArgumentMatchers.eq(sampleGood.entityType),
                            org.mockito.ArgumentMatchers.eq(sampleGood.name)
      )
    )
      .thenReturn(DBIO.successful(Some(sampleGoodAsCER)))

    // TODO make a helper method to reduce repetition
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, None)
    val futureResult = Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configGood, littleWdl)) flatMap {
      gatherInputsResult =>
        compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                       expressionEvaluationContext,
                                                       gatherInputsResult
        )
    }
    // TODO there must be a better way to check the result?
    whenReady(futureResult) { lazyList =>
      lazyList should contain(
        SubmissionValidationEntityInputs(
          sampleGood.name,
          Set(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName))
        )
      )
    }

    val expressionEvaluationContext2 =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, None)
    val futureResult2 =
      Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configEvenBetter, littleWdl)) flatMap {
        gatherInputsResult =>
          compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                         expressionEvaluationContext2,
                                                         gatherInputsResult
          )
      }
    whenReady(futureResult2) { lazyList =>
      lazyList should contain(
        SubmissionValidationEntityInputs(
          sampleGood.name,
          Set(
            SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName),
            SubmissionValidationValue(Some(AttributeNumber(1)), None, intOptNameWithWfName)
          )
        )
      )
    }

  }

  it should "resolve method config inputs for a set entity" in withConfigData {
    val sampleGoodAsCER = CompactEntityRecord(
      1L,
      sampleGood.name,
      sampleGood.entityType,
      workspace.workspaceIdAsUUID,
      1L,
      false,
      Option(CompactEntitySerialization.toSql(sampleGood.attributes).compactPrint)
    )

    val sampleGood2AsCER = CompactEntityRecord(
      3L,
      sampleGood2.name,
      sampleGood2.entityType,
      workspace.workspaceIdAsUUID,
      1L,
      false,
      Option(CompactEntitySerialization.toSql(sampleGood2.attributes).compactPrint)
    )

    val sampleMissingValueAsCER = CompactEntityRecord(
      2L,
      sampleMissingValue.name,
      sampleMissingValue.entityType,
      workspace.workspaceIdAsUUID,
      1L,
      false,
      Option(CompactEntitySerialization.toSql(sampleMissingValue.attributes).compactPrint)
    )

    val sampleWithSingleElementArrayAsCER = CompactEntityRecord(
      4L,
      sampleWithSingleElementArray.name,
      sampleWithSingleElementArray.entityType,
      workspace.workspaceIdAsUUID,
      1L,
      false,
      Option(CompactEntitySerialization.toSql(sampleWithSingleElementArray.attributes).compactPrint)
    )
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
      ExpressionEvaluationContext(Some(sampleSet.entityType), Some(sampleSet.name), Some("this.samples"), None)
    val futureResult = Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configSampleSet, arrayWdl)) flatMap {
      gatherInputsResult =>
        compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                       expressionEvaluationContext,
                                                       gatherInputsResult
        )
    }
    whenReady(futureResult) { lazyList =>
      lazyList should contain(
        SubmissionValidationEntityInputs(
          sampleSet.name,
          Set(
            SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1)))), None, intArrayNameWithWfName)
          )
        )
      )
    }

    // TODO is changing the entity in the exevcxt the correct way to set this up?
    val expressionEvaluationContext2 =
      ExpressionEvaluationContext(Some(sampleSet2.entityType), Some(sampleSet2.name), Some("this.samples"), None)
    val futureResult2 = Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configSampleSet, arrayWdl)) flatMap {
      gatherInputsResult =>
        compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                       expressionEvaluationContext2,
                                                       gatherInputsResult
        )
    }

    whenReady(futureResult2) { lazyList =>
      lazyList should contain(
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
    }

    val expressionEvaluationContext3 =
      ExpressionEvaluationContext(Some(sampleSet4.entityType), Some(sampleSet4.name), Some("this.samples"), None)
    val futureResult3 = Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configSampleSet, arrayWdl)) flatMap {
      gatherInputsResult =>
        compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                       expressionEvaluationContext3,
                                                       gatherInputsResult
        )
    }
    // attribute reference with 1 element array should resolve as AttributeValueList
    whenReady(futureResult3) { lazyList =>
      lazyList should contain(
        SubmissionValidationEntityInputs(
          sampleSet4.name,
          Set(
            SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(101)))), None, intArrayNameWithWfName)
          )
        )
      )
    }

  }

  it should "return error on missing values" in withConfigData {
    // TODO what would this actually return, and then how would CEE deal with it
    when(
      mockQueries.queryEntityForAttribute(any(), any(), any(), org.mockito.ArgumentMatchers.eq("sampleMissingValue"))
    )
      .thenReturn(DBIO.successful(AttributeNull))
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleMissingValue.entityType), Some(sampleMissingValue.name), None, None)
    val futureResult = Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configGood, littleWdl)) flatMap {
      gatherInputsResult =>
        compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                       expressionEvaluationContext,
                                                       gatherInputsResult
        )
    }
    whenReady(futureResult) { lazyList =>
      val result = lazyList.find(_.entityName == "sampleMissingValue").flatMap { entityInputs =>
        entityInputs.inputResolutions.find {
          case SubmissionValidationValue(None, Some(_), intArg) if intArg == intArgNameWithWfName => true
          case _                                                                                  => false
        }
      }
      result shouldBe true
    }
    /*
          assertResult(true, "Missing values should return an error") {
        runAndWait(testResolveInputs(context, configGood, sampleMissingValue, littleWdl, this))
          .get("sampleMissingValue")
          .get match {
          case Seq(SubmissionValidationValue(None, Some(_), intArg)) if intArg == intArgNameWithWfName => true
        }
      }
     */
  }

  it should "error on missing input definitions" in withConfigData {
    val expressionEvaluationContext =
      ExpressionEvaluationContext(Some(sampleGood.entityType), Some(sampleGood.name), None, None)
    val futureResult =
      Future.fromTry(methodConfigResolver.gatherInputs(userInfo, configMissingExpr, littleWdl)) flatMap {
        gatherInputsResult =>
          compactExpressionEvaluator.evaluateExpressions(workspace.workspaceIdAsUUID,
                                                         expressionEvaluationContext,
                                                         gatherInputsResult
          )
      }
    /*
          // MethodConfiguration config_namespace/configMissingExpr is missing definitions for these inputs: w1.t1.int_arg
      intercept[RawlsException] {
        runAndWait(testResolveInputs(context, configMissingExpr, sampleGood, littleWdl, this))
      }

     */
  }

}
