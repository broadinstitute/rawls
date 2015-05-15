package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.scalatest.FunSuite

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Success

/**
 * Created by abaumann on 5/21/15.
 */
class SimpleExpressionParserTest extends FunSuite with OrientDbTestFixture {
  override val testDbName = "ExpressionParserTest"

  test("simple expression") {
    initializeTestGraph()
    val evaluator = new ExpressionEvaluator(graph, new ExpressionParser)

    assertResult( Success(ArrayBuffer("normal", "tumor", "tumor")) ) {
      evaluator.evaluate("workspaces", "test_workspace", "SampleSet", "sset1", "this.samples.type")
    }

    assertResult( Success(ArrayBuffer("normal")) ) {
      evaluator.evaluate("workspaces", "test_workspace", "Sample", "sample1", "this.type")
    }
  }
}
