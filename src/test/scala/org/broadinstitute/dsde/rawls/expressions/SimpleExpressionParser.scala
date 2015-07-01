package org.broadinstitute.dsde.rawls.expressions

import com.tinkerpop.blueprints.Graph
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.AttributeString
import org.scalatest.FunSuite

import scala.collection.immutable.HashMap
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

  test("simple attribute expression") {
    withTestData { graph =>
      val evaluator = new ExpressionEvaluator(graph, new ExpressionParser)
      assertResult(Success(ArrayBuffer(AttributeString("normal")))) {
        evaluator.evalFinalAttribute(testData.wsName.namespace, testData.wsName.name, "Sample", "sample1", "this.type")
      }

      assertResult(Success(ArrayBuffer(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor")))) {
        evaluator.evalFinalAttribute(testData.wsName.namespace, testData.wsName.name, "SampleSet", "sset1", "this.samples.type")
      }
    }
  }

  test("simple entity expression") {
    withTestData { graph =>
      val evaluator = new ExpressionEvaluator(graph, new ExpressionParser)

      assertResult(Success(ArrayBuffer(testData.sample2))) {
        evaluator.evalFinalEntity(testData.wsName.namespace, testData.wsName.name, "Pair", "pair1", "this.case")
      }

      assertResult(Success(ArrayBuffer(testData.sample1, testData.sample2, testData.sample3))) {
        evaluator.evalFinalEntity(testData.wsName.namespace, testData.wsName.name, "Individual", "indiv1", "this.sset.samples")
      }
    }
  }
}
