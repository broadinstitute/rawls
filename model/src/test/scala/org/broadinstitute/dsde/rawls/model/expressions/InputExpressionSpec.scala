package org.broadinstitute.dsde.rawls.model.expressions

import org.broadinstitute.dsde.rawls.model.AttributeName
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsTrue}

import scala.util.{Failure, Success}

class InputExpressionSpec extends FlatSpec with Matchers with ExpressionFixture {
  "InputExpressions" should "parse targeted expressions" in {
    InputExpression.build("this.gvcf") shouldEqual Success(TargetedInputExpression(EntityTarget, Seq(AttributeName("default", "gvcf"))))
    InputExpression.build("workspace.gvcf") shouldEqual Success(TargetedInputExpression(WorkspaceTarget, Seq(AttributeName("default", "gvcf"))))
    InputExpression.build("workspace.library:cohort") shouldEqual Success(TargetedInputExpression(WorkspaceTarget, Seq(AttributeName("library", "cohort"))))
    InputExpression.build("this.library:cohort") shouldEqual Success(TargetedInputExpression(EntityTarget, Seq(AttributeName("library", "cohort"))))

    // does not enforce Attribute Namespace constraints
    InputExpression.build("this.arbitrary:whatever") shouldEqual Success(TargetedInputExpression(EntityTarget, Seq(AttributeName("arbitrary", "whatever"))))
    InputExpression.build("workspace.arbitrary:whatever") shouldEqual Success(TargetedInputExpression(WorkspaceTarget, Seq(AttributeName("arbitrary", "whatever"))))

    // multiple attributes in the path is OK

    val entExpected = Seq(
      AttributeName("default", "case_sample"),
      AttributeName("foo", "ref"),
      AttributeName("bar", "attribute"))
    InputExpression.build("this.case_sample.foo:ref.bar:attribute") shouldEqual Success(TargetedInputExpression(EntityTarget, entExpected))

    val wsExpected = Seq(
      AttributeName("default", "yes"),
      AttributeName("default", "we"),
      AttributeName("default", "can"))
    InputExpression.build("workspace.yes.we.can") shouldEqual Success(TargetedInputExpression(WorkspaceTarget, wsExpected))
  }

  it should "parse JSON" in {
    InputExpression.build(""" "a string literal" """) shouldEqual Success(JSONInputExpression(JsString("a string literal")))
    InputExpression.build("9000") shouldEqual Success(JSONInputExpression(JsNumber(9000)))
    InputExpression.build("-3.77") shouldEqual Success(JSONInputExpression(JsNumber(-3.77)))
    InputExpression.build("true") shouldEqual Success(JSONInputExpression(JsTrue))
    InputExpression.build("""[ "foo", "bar", "horsefish" ]""") shouldEqual Success(JSONInputExpression(JsArray(Vector(JsString("foo"), JsString("bar"), JsString("horsefish")))))
    InputExpression.build("""{ "key" : "value" }""") shouldEqual Success(JSONInputExpression(JsObject(Map("key" -> JsString("value")))))

    val expected = JSONInputExpression(JsArray(Vector(JsString("a"), JsObject(Map("more" -> JsObject(Map("elaborate" -> JsString("example"))))))))
    InputExpression.build("""["a",{"more":{"elaborate":"example"}}]""") shouldEqual Success(expected)
  }

  it should "round-trip correctly" in {
    validInputExpressions foreach { expr =>
      InputExpression.build(expr).map(_.toString) shouldEqual Success(expr)
    }
  }

  it should "reject invalid input expressions" in {
    invalidInputExpressions foreach { expr =>
      InputExpression.build(expr) shouldBe a [Failure[_]]
    }
  }
}
