package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityAttributeListSerializer
import org.broadinstitute.dsde.rawls.entities.exceptions.CompactEntityDeserializationException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeFormat,
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueList,
  Entity
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsNumber, JsObject}

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
      actual.get shouldBe a[JsObject]
      // this intentionally does not test the details of how the AttributeMap is serialized; that is done elsewhere.
      // this only tests that the serialized AttributeMap is a sub-object located in an "attrs" key
    }
  }

  behavior of "v2-specific serialization"

  it should "serialize references into a normalized array with sortable values in attrs" in {
    // shorthand for attrs
    val singleref = AttributeName.withDefaultNS("singleref")
    val reflist = AttributeName.withDefaultNS("reflist")

    val serialized = toSql(refsEntity.attributes)
    val actual = serialized.convertTo[SqlEntityData]

    // check refs array
    actual.refs should contain theSameElementsInOrderAs Seq(
      SqlEntityReference(a = "singleref", t = "targetType", n = "targetName1", z = Some(true)),
      SqlEntityReference(a = "reflist", t = "targetType", n = "targetName2", z = None),
      SqlEntityReference(a = "reflist", t = "targetType", n = "targetName3", z = None)
    )

    actual.attrs.keys should contain(singleref)
    actual.attrs(singleref) shouldBe AttributeString("targetName1")
    actual.attrs.keys should contain(reflist)
    actual.attrs(reflist) shouldBe AttributeNumber(2)
  }

  behavior of "AttributeFormat serialization"

  val trailingZeroSerializationCases = Map(
    "1" -> "1",
    "1.0" -> "1",
    "1.1" -> "1.1",
    "1.10" -> "1.1",
    "10" -> "10",
    "50" -> "50",
    "99999.0000" -> "99999",
    "-123456.7890" -> "-123456.789"
  )

  // note this is serialization of an entity via `AttributeFormat`, not via `CompactEntitySerialization`
  trailingZeroSerializationCases foreach { case (input, expected) =>
    it should s"strip trailing zeros from $input" in {
      import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.AttributeNameFormat
      import spray.json.DefaultJsonProtocol._
      import spray.json._

      implicit val attributeFormat: AttributeFormat = new AttributeFormat with CompactEntityAttributeListSerializer

      val attrs: AttributeMap = Map(
        AttributeName.withDefaultNS("foo") -> AttributeNumber(BigDecimal(input))
      )
      val serialized = attrs.toJson.compactPrint
      serialized shouldBe s"""{"foo":$expected}"""
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

  behavior of "v2-specific deserialization"

  it should "deserialize with no references" in {
    val input =
      """{ "v": 2,
         "attrs": {"hello": "world", "mynum": 42, "somelist": [true, false] },
         "refs": []
       }"""

    val expected = Map(
      AttributeName.fromDelimitedName("hello") -> AttributeString("world"),
      AttributeName.fromDelimitedName("mynum") -> AttributeNumber(42),
      AttributeName.fromDelimitedName("somelist") -> AttributeValueList(
        Seq(AttributeBoolean(true), AttributeBoolean(false))
      )
    )

    val actual = fromSql(Option(input))
    actual shouldBe expected
  }

  it should "deserialize with references" in {
    val input =
      """{ "v": 2,
           "attrs": {"hello": "world", "reflist": 1, "refscalar": "targetName2"},
           "refs": [
             {"a": "reflist", "n": "targetName1", "t": "targetType"},
             {"a": "refscalar", "n": "targetName2", "t": "targetType", "z": true}
           ]
         }"""

    val expected = Map(
      AttributeName.fromDelimitedName("hello") -> AttributeString("world"),
      AttributeName.fromDelimitedName("reflist") -> AttributeEntityReferenceList(
        Seq(
          AttributeEntityReference("targetType", "targetName1")
        )
      ),
      AttributeName.fromDelimitedName("refscalar") -> AttributeEntityReference("targetType", "targetName2")
    )

    val actual = fromSql(Option(input))
    actual shouldBe expected
  }

  val trailingZeroDeserializationCases = Map(
    "1" -> "1",
    "1.0" -> "1",
    "1.1" -> "1.1",
    "1.10" -> "1.1",
    "10" -> "10",
    "50" -> "50",
    "5E+1" -> "50",
    "99999.0000" -> "99999",
    "-123456.7890" -> "-123456.789"
  )

  trailingZeroDeserializationCases foreach { case (inputVal, expectedVal) =>
    it should s"strip trailing zeros when deserializing $inputVal" in {
      val input =
        s"""{ "v": 2,
         "attrs": {"hello": "world", "mynum": $inputVal, "somelist": [true, false] },
         "refs": []
       }"""

      val expected = Map(
        AttributeName.fromDelimitedName("hello") -> AttributeString("world"),
        AttributeName.fromDelimitedName("mynum") -> AttributeNumber(
          BigDecimal(expectedVal).bigDecimal.stripTrailingZeros()
        ),
        AttributeName.fromDelimitedName("somelist") -> AttributeValueList(
          Seq(AttributeBoolean(true), AttributeBoolean(false))
        )
      )

      val actual = fromSql(Option(input))
      actual shouldBe expected
    }
  }

  it should "throw if the attributes sub-object is missing" in {
    val input =
      """{ "v": 2, "incorrect-key-for-attrs": {}, "refs": [] }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

  it should "throw if the attributes sub-object is an unexpected data type" in {
    val input =
      """{ "v": 2, "attrs": false, "refs": [] }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

  it should "throw if the references sub-object is missing" in {
    val input =
      """{ "v": 2, "attrs": {}, "incorrect-key-for-refs": [] }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

  it should "throw if the references sub-object is an unexpected data type" in {
    val input =
      """{ "v": 2, "attrs": {}, "refs": false }"""
    intercept[CompactEntityDeserializationException] {
      fromSql(Option(input))
    }
  }

}
