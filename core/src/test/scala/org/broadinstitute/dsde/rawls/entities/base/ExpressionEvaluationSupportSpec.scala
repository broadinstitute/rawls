package org.broadinstitute.dsde.rawls.entities.base

import cromwell.client.model.{ToolInputParameter, ValueType}
import cromwell.client.model.ValueType.TypeNameEnum
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeEntityReference,
  AttributeEntityReferenceEmptyList,
  AttributeEntityReferenceList,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  AttributeValueEmptyList,
  AttributeValueList,
  AttributeValueRawJson
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsArray, JsNumber, JsObject, JsString}

import scala.jdk.CollectionConverters._

class ExpressionEvaluationSupportSpec extends AnyFlatSpec with Matchers with ExpressionEvaluationSupport {

  behavior of "ExpressionEvaluationSupport.isStringInputType"

  // helper method to create a MethodInput for use in unit tests
  def methodInput(typeName: TypeNameEnum, isOptional: Boolean = false, isArray: Boolean = false): MethodInput = {
    val input = new ToolInputParameter()

    val valueType = new ValueType()
    if (isOptional && isArray) {
      val arrType = new ValueType()
      arrType.setTypeName(typeName)
      valueType.setArrayType(arrType)
      val optType = new ValueType()
      optType.setTypeName(typeName)
      valueType.setTypeName(TypeNameEnum.OPTIONAL)
      valueType.setOptionalType(optType)
      input.setOptional(isOptional)
    } else if (isOptional) {
      val optType = new ValueType()
      optType.setTypeName(typeName)
      valueType.setOptionalType(optType)
      valueType.setTypeName(TypeNameEnum.OPTIONAL)
      input.setOptional(isOptional)
    } else if (isArray) {
      val arrType = new ValueType()
      arrType.setTypeName(typeName)
      valueType.setTypeName(TypeNameEnum.ARRAY)
      valueType.setArrayType(arrType)
    } else {
      valueType.setTypeName(typeName)
    }

    input.setName("inputName") // doesn't matter for these tests
    input.setValueType(valueType)
    MethodInput(input, "this.something") // expression value doesn't matter for these tests
  }

  // helper method to create a unique name for each test
  def testDescriptor(typeName: TypeNameEnum, isOptional: Boolean, isArray: Boolean) = Seq(
    if (isOptional) Some("optional") else None,
    if (isArray) Some("array of") else None,
    Some(typeName.getValue)
  ).flatten.mkString(" ")

  // all input types that are not String, nor are top-level optional or array, since those have sub-types
  val nonStringTypes: Set[TypeNameEnum] =
    TypeNameEnum.values().toSet diff Set(TypeNameEnum.STRING, TypeNameEnum.OPTIONAL, TypeNameEnum.ARRAY)

  // programmatically create unit tests for optional=true/false, array=true/false, and each type
  List(true, false) foreach { isOptional =>
    List(true, false) foreach { isArray =>
      // these types should return true, i.e. isStringInputType detects a string input
      List(TypeNameEnum.STRING) foreach { valueType =>
        it should s"detect string inputs for ${testDescriptor(valueType, isOptional, isArray)}" in {
          val input = methodInput(valueType, isOptional = isOptional, isArray = isArray)
          isStringInputType(input) shouldBe true
        }
      }
      // these types should return false, i.e. isStringInputType does not detect a string input
      nonStringTypes.foreach { valueType =>
        it should s"not detect a string input for ${testDescriptor(valueType, isOptional, isArray)}" in {
          val input = methodInput(valueType, isOptional = isOptional, isArray = isArray)
          isStringInputType(input) shouldBe false
        }
      }
    }
  }

  behavior of "ExpressionEvaluationSupport.maybeConvertToString"

  // define a map of (input attribute value) -> (expected result from castToString)
  val stringConversions = Map(
    AttributeNumber(123) -> AttributeString("123"),
    AttributeNumber(123.4567) -> AttributeString("123.4567"),
    AttributeNumber(Double.MaxValue) -> AttributeString(
      "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    ),
    AttributeNumber(Double.MinValue) -> AttributeString(
      "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    ),
    AttributeNumber(Double.MinPositiveValue) -> AttributeString(
      "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000049"
    ),
    AttributeBoolean(true) -> AttributeString("true"),
    AttributeBoolean(false) -> AttributeString("false"),
    AttributeValueList(Seq(AttributeNumber(123), AttributeNumber(456))) -> AttributeValueList(
      Seq(AttributeString("123"), AttributeString("456"))
    ),
    AttributeValueList(Seq(AttributeBoolean(false), AttributeBoolean(true))) -> AttributeValueList(
      Seq(AttributeString("false"), AttributeString("true"))
    )
  )

  stringConversions foreach { case (input, expected) =>
    it should s"cast $input to $expected" in {
      maybeConvertToString(input) shouldBe expected
    }
  }

  // all of these should not be converted; input and expected should be exactly the same
  val shouldNotConvert = Seq(
    AttributeString("hello world"),
    AttributeString(""),
    AttributeNull,
    AttributeValueRawJson(new JsObject(Map.empty)),
    AttributeValueRawJson(new JsObject(Map("num" -> JsNumber(123)))),
    AttributeValueRawJson(new JsArray(Vector(JsString("foo"), JsString("bar")))),
    AttributeEntityReference("foo", "bar"),
    AttributeValueEmptyList,
    AttributeValueList(Seq(AttributeString("foo"), AttributeString("bar"))),
    AttributeValueList(Seq(AttributeNull, AttributeNull)),
    AttributeValueList(
      Seq(AttributeValueRawJson(new JsObject(Map("num" -> JsNumber(123)))),
          AttributeValueRawJson(new JsArray(Vector(JsString("foo"), JsString("bar"))))
      )
    ),
    AttributeEntityReferenceEmptyList,
    AttributeEntityReferenceList(Seq(AttributeEntityReference("foo", "bar"), AttributeEntityReference("baz", "qux")))
  )

  shouldNotConvert foreach { input =>
    it should s"leave an input of $input unchanged and un-casted" in {
      maybeConvertToString(input) shouldBe input
    }
  }

}
