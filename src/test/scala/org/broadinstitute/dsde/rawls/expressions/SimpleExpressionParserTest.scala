package org.broadinstitute.dsde.rawls.expressions

import com.tinkerpop.blueprints.Graph
import org.broadinstitute.dsde.rawls.dataaccess.{RawlsTransaction, WorkspaceContext}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.util.Success

/**
 * Created by abaumann on 5/21/15.
 */
class SimpleExpressionParserTest extends FunSuite with OrientDbTestFixture {
  def withTestWorkspace(testCode: (WorkspaceContext, RawlsTransaction) => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      dataSource.inTransaction(writeLocks=Set(testData.workspace.toWorkspaceName)) { txn =>
        withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
          testCode(workspaceContext, txn)
        }
      }
    }
  }

  test("simple attribute expression") {
    withTestWorkspace { (workspaceContext, txn) =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)
      assertResult(Success(ArrayBuffer(AttributeString("normal")))) {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.type")
      }

      assertResult(Success(ArrayBuffer(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor")))) {
        evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.type")
      }

      assertResult(Success(ArrayBuffer())) {
        evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset_empty", "this.samples")
      }

      assertResult(Success(ArrayBuffer(AttributeString("a"), AttributeBoolean(true)))) {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.thingies")
      }
    }
  }

  test("reserved attribute expression") {
    withTestWorkspace { (workspaceContext, txn) =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)
      assertResult(Success(ArrayBuffer(AttributeString("sample1")))) {
        evaluator.evalFinalAttribute(workspaceContext, "Sample", "sample1", "this.name")
      }

      assertResult(Success(ArrayBuffer(AttributeString("Sample"), AttributeString("Sample"), AttributeString("Sample")))) {
        evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "this.samples.entityType")
      }

      assertResult(Success(ArrayBuffer(AttributeString(workspaceContext.workspace.name)))) {
        evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.name")
      }

      assertResult(Success(ArrayBuffer(AttributeNull))) {
        evaluator.evalFinalAttribute(workspaceContext, "SampleSet", "sset1", "workspace.entityType")
      }
    }
  }

  test("workspace attribute expression") {
    withTestWorkspace { (workspaceContext, txn) =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)

      assertResult(Success(ArrayBuffer(testData.wsAttrs.get("string").get))) {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.string")
      }

      assertResult(Success(ArrayBuffer(testData.wsAttrs.get("number").get))) {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.number")
      }

      assertResult(Success(ArrayBuffer(testData.sample1.attributes.get("type").get))) {
        val attributesPlusReference = testData.workspace.attributes + ("sample1ref" -> AttributeEntityReference("Sample", "sample1"))
        workspaceDAO.save(testData.workspace.copy(attributes = attributesPlusReference), txn)

        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.sample1ref.type")
      }

      assertResult(Success(ArrayBuffer())) {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.empty")
      }

      assertResult(Success(ArrayBuffer(AttributeString("another string"), AttributeBoolean(true)))) {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.values")
      }

      assertResult(Success(ArrayBuffer())) {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.this_attribute_is_not_present")
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace").isFailure
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.").isFailure
      }

      assert {
        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.missing.also_missing").isFailure
      }

      assert {
        val attributesPlusReference = testData.workspace.attributes + ("sample1ref" -> AttributeEntityReference("Sample", "sample1"))
        workspaceDAO.save(testData.workspace.copy(attributes = attributesPlusReference), txn)

        evaluator.evalFinalAttribute(workspaceContext, "dummy text", "dummy text", "workspace.sample1ref.").isFailure
      }
    }
  }

  test("simple entity expression") {
    withTestWorkspace { (workspaceContext, txn) =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)

      assertResult(Success(ArrayBuffer(testData.sample2))) {
        evaluator.evalFinalEntity(workspaceContext, "Pair", "pair1", "this.case")
      }

      assertResult(Success(ArrayBuffer(testData.sample1, testData.sample2, testData.sample3))) {
        evaluator.evalFinalEntity(workspaceContext, "Individual", "indiv1", "this.sset.samples")
      }

      assertResult(Success(ArrayBuffer(testData.pair1))) {
        evaluator.evalFinalEntity(workspaceContext, "Pair", "pair1", "this")
      }
    }
  }

  test("workspace entity expression") {
    withTestWorkspace { (workspaceContext, txn) =>
      val evaluator = new ExpressionEvaluator(new ExpressionParser)

      assertResult(Success(ArrayBuffer(testData.sample1))) {
        val attributesPlusReference = testData.workspace.attributes + ("sample1ref" -> AttributeEntityReference("Sample", "sample1"))
        workspaceDAO.save(testData.workspace.copy(attributes = attributesPlusReference), txn)

        evaluator.evalFinalEntity(workspaceContext, "dummy text", "dummy text", "workspace.sample1ref")
      }

      assertResult(Success(ArrayBuffer(testData.sample2))) {
        val reflist = AttributeEntityReferenceList(Seq(AttributeEntityReference("Sample", "sample2")))
        val attributesPlusReference = testData.workspace.attributes + ("samplerefs" -> reflist)
        workspaceDAO.save(testData.workspace.copy(attributes = attributesPlusReference), txn)

        evaluator.evalFinalEntity(workspaceContext, "dummy text", "dummy text", "workspace.samplerefs")
      }
    }
  }

  test("output expressions") {
    withTestWorkspace { (workspaceContext, txn) =>
      val parser = new ExpressionParser
      assert(parser.parseOutputExpr("this.attribute").isSuccess, "this.attribute should parse correctly" )
      assert(parser.parseOutputExpr("this..attribute").isFailure, "this..attribute should not parse correctly" )
      assert(parser.parseOutputExpr("this.chained.expression").isFailure, "this.chained.expression should not parse correctly" )

      assert(parser.parseOutputExpr("workspace.attribute").isSuccess, "workspace.attribute should parse correctly" )
      assert(parser.parseOutputExpr("workspace..attribute").isFailure, "workspace..attribute should not parse correctly" )
      assert(parser.parseOutputExpr("workspace.chained.expression").isFailure, "workspace.chained.expression should not parse correctly" )
      
      assert(parser.parseOutputExpr("bonk.attribute").isFailure, "bonk.attribute should not parse correctly" )
    }
  }
}
