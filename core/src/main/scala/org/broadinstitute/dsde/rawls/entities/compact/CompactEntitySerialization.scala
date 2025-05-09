package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityAttributeListSerializer
import org.broadinstitute.dsde.rawls.entities.exceptions.CompactEntityDeserializationException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.{AttributeFormat, AttributeName}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._

// allow for static access to methods from classes that don't want to mix in the trait
object CompactEntitySerialization extends CompactEntitySerialization

/**
  * Methods to serialize attributes to/deserialize attributes from the database
  */
trait CompactEntitySerialization {

  // defines how list attributes are translated to/from JSON
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with CompactEntityAttributeListSerializer

  // json key for the version number
  val VERSION_KEY: String = "v"
  // json key for the attributes content. If this ever changes, make sure to also change the triggers
  // on the `ENTITY` database table
  val ATTRS_KEY: String = "attrs"

  // the current serialization version
  val CURRENT_VERSION: Int = 1

  // translate a AttributeMap into a JsValue suitable for persisting into the db
  def toSql(attributes: AttributeMap): JsObject =
    JsObject(
      Map(
        VERSION_KEY -> JsNumber(CURRENT_VERSION),
        ATTRS_KEY -> attributes.toJson
      )
    )

  // translate a SQL column value back into an AttributeMap
  def fromSql(attributes: Option[String]): AttributeMap =
    attributes.getOrElse("{}").parseJson match {
      case jso: JsObject => deserialize(jso)
      case otherJsValue =>
        throw new CompactEntityDeserializationException(
          s"wanted a JsObject; found a ${otherJsValue.getClass.getName}"
        )
    }

  def keepOnlyFields(attributes: Option[String], fields: Set[AttributeName]): Option[String] =
    attributes.map { attr =>
      val attributeMap = attr.parseJson match {
        case jso: JsObject => deserialize(jso)
        case otherJsValue =>
          throw new CompactEntityDeserializationException(
            s"wanted a JsObject; found a ${otherJsValue.getClass.getName}"
          )
      }
      val filtered = attributeMap.filter { case (k, _) =>
        fields.contains(k)
      }
      toSql(filtered).compactPrint
    }

  val slickAttrsPath: String = s"$$.${ATTRS_KEY}"
  def slickAttributePath(attributeName: String): String = s"${slickAttrsPath}.${attributeName}"
  def slickAttributePath(attributeName: AttributeName): String = slickAttributePath(toDelimitedName(attributeName))

  // retrieve the version number from the database's JSON
  private def getVersion(jso: JsObject): Int =
    jso.fields.get(VERSION_KEY) match {
      // when a version is embedded in the json, use it
      case Some(n: JsNumber) => n.value.intValue
      // when no version is embedded, assume version 0
      case None => 0
      // version could not be determined
      case Some(otherJsValue) =>
        throw new CompactEntityDeserializationException(
          s"wanted a version number; found a ${otherJsValue.getClass.getName}"
        )
    }

  // retrieve the attributes packet from the database's JSON
  private def getAttrs(jso: JsObject): JsObject =
    jso.fields.get(ATTRS_KEY) match {
      case Some(jso: JsObject) => jso
      case None =>
        throw new CompactEntityDeserializationException(
          s"wanted an attributes sub-object; found none"
        )
      // version could not be determined
      case Some(otherJsValue) =>
        throw new CompactEntityDeserializationException(
          s"wanted an attributes sub-object; found a ${otherJsValue.getClass.getName}"
        )
    }

  private def deserialize(jso: JsObject): AttributeMap =
    getVersion(jso) match {

      /*  version 0 stores the attribute map directly, with denormalized references:
          {
            "hello": "world",
            "refs": [
              {"entityName": "1", "entityType": "test"},
              {"entityName": "2", "entityType": "test"}
            ]
          }
       */
      case 0 => jso.convertTo[AttributeMap]

      /*  version 1 embeds the version number. References are denormalized:
          {
            "v": 1,
            "attrs": {
              "hello": "world",
              "refs": [
                {"entityName": "1", "entityType": "test"},
                {"entityName": "2", "entityType": "test"}
              ]
            }
          }
       */
      case 1 => getAttrs(jso).convertTo[AttributeMap]

      case x =>
        throw new CompactEntityDeserializationException(
          s"found unexpected version number: $x"
        )
    }

}
