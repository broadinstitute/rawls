package org.broadinstitute.dsde.rawls.expressions

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
  override val testDbName = "ExpressionParserTest"

  test("simple attribute expression") {
    initializeTestGraph()
    val evaluator = new ExpressionEvaluator(graph, new ExpressionParser)

    assertResult( Success(ArrayBuffer(AttributeString("normal"))) ) {
      evaluator.evalFinalAttribute("workspaces", "test_workspace", "Sample", "sample1", "this.type")
    }

    assertResult( Success(ArrayBuffer(AttributeString("normal"), AttributeString("tumor"), AttributeString("tumor"))) ) {
      evaluator.evalFinalAttribute("workspaces", "test_workspace", "SampleSet", "sset1", "this.samples.type")
    }
  }

  test("simple entity expression") {
    initializeTestGraph()
    val evaluator = new ExpressionEvaluator(graph, new ExpressionParser)

    assertResult( Success( ArrayBuffer(tg_sample2) ) ) {
      evaluator.evalFinalEntity("workspaces", "test_workspace", "Pair", "pair1", "this.case")
    }

    assertResult( Success(ArrayBuffer(tg_sample1, tg_sample2, tg_sample3)) ) {
      evaluator.evalFinalEntity("workspaces", "test_workspace", "Individual", "indiv1", "this.sset.samples")
    }
  }
}
