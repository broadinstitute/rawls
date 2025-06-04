package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.entities.exceptions.CompactEntityDeserializationException
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeEntityReferenceList, AttributeName, AttributeNumber, AttributeString, Entity}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsArray, JsNumber, JsObject}

class CompactEntitySerializationSpec extends AnyFlatSpec with Matchers with CompactEntitySerialization {

  private val emptyEntity = Entity("emptyEntity", "mytype", Map())
  private val simpleEntity = Entity(
    "simpleEntity",
    "mytype",
    Map(
      AttributeName.fromDelimitedName("hello") -> AttributeString("world"),
      AttributeName.fromDelimitedName("num") -> AttributeNumber(42)
    )
  )
  private val refsEntity = Entity(
    "refsEntity",
    "mytype",
    Map(
      AttributeName.fromDelimitedName("hello") -> AttributeString("world"),
      AttributeName.fromDelimitedName("singleref") -> AttributeEntityReference("targetType", "targetName1"),
      AttributeName.fromDelimitedName("reflist") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("targetType", "targetName2"),
          AttributeEntityReference("targetType", "targetName3")
        )
      )
    )
  )

  behavior of "Attribute serialization"

  List(emptyEntity, simpleEntity, refsEntity) foreach { entity =>
    it should s"include a version number for ${entity.name}" in {
      val serialized = toSql(entity.attributes)
      val actual = serialized.fields.get(VERSION_KEY)
      actual should contain(JsNumber(CURRENT_VERSION))
    }

    it should s"include the attributes as a sub-object for ${entity.name}" in {
      val serialized = toSql(entity.attributes)
      val actual = serialized.fields.get(ATTRS_KEY)
      actual should not be empty
      actual.get shouldBe a[JsArray]
      // this intentionally does not test the details of how the AttributeMap is serialized; that is done elsewhere.
      // this only tests that the serialized AttributeMap is a sub-object located in an "attrs" key
    }
  }

  behavior of "Attribute deserialization"

  private val expectedAttrs = Map(
    AttributeName.fromDelimitedName("hello") -> AttributeString("world"),
    AttributeName.fromDelimitedName("reflist") -> AttributeEntityReferenceList(
      Seq(
        AttributeEntityReference("targetType", "targetName1")
      )
    )
  )

  it should "deserialize version 0" in {
    val input = """{"hello": "world", "reflist": [{"entityName": "targetName1", "entityType": "targetType"}]}"""
    val actual = fromSql(Option(input))
    actual shouldBe expectedAttrs
  }

  it should "deserialize version 1" in {
    val input =
      """{ "v": 1, "attrs": {"hello": "world", "reflist": [{"entityName": "targetName1", "entityType": "targetType"}]} }"""
    val actual = fromSql(Option(input))
    actual shouldBe expectedAttrs
  }

  it should "return an empty map for input of None" in {
    val input = None
    val actual = fromSql(input)
    actual shouldBe Map()
  }

  it should "return an empty map for input of {}" in {
    val input = Some("{}")
    val actual = fromSql(input)
    actual shouldBe Map()
  }

  it should "throw if the version number is out of range" in {
    val input =
      """{ "v": 3, "attrs": {} }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

  it should "throw if the version number is an unexpected data type" in {
    val input =
      """{ "v": "v2.0.0", "attrs": {} }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

  it should "throw if the attributes sub-object is missing" in {
    val input =
      """{ "v": 1, "incorrect-key-for-attrs": {} }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

  it should "throw if the attributes sub-object is an unexpected data type" in {
    val input =
      """{ "v": 1, "attrs": false }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

}
