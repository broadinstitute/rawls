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
  def withTestData(testCode:Graph => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      dataSource.inTransaction { txn =>
        txn.withGraph { graph =>
          testCode(graph);
        }
      }
    }
  }

  def withTestWorkspace(testCode: (WorkspaceContext, RawlsTransaction) => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      dataSource inTransaction { txn =>
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
}
