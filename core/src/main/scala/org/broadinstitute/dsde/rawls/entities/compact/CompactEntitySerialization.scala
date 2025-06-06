package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityAttributeListSerializer
import org.broadinstitute.dsde.rawls.entities.exceptions.CompactEntityDeserializationException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{
  Attribute,
  AttributeBoolean,
  AttributeEntityReference,
  AttributeEntityReferenceEmptyList,
  AttributeEntityReferenceList,
  AttributeFormat,
  AttributeName,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  AttributeValue,
  AttributeValueEmptyList,
  AttributeValueList,
  AttributeValueRawJson
}
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
  // json key for the "query" values, used for sorting and filtering in entityQuery
  val REFS_KEY: String = "refs"

  // the current serialization version
  val CURRENT_VERSION: Int = 2

  // translate a AttributeMap into a JsValue suitable for persisting into the db
  def toSql(attributes: AttributeMap): JsObject = {
    // v1 serialization format:
    // val attrsJson = attributes.toJson

    // translate references and reference lists into sortable values;
    // the references themselves are stored in a separate "refs" key
    val jsonAttrs: AttributeMap = attributes.map { case (name, value) =>
      val normalizedValue = value match {
        case AttributeEntityReferenceList(l)         => AttributeNumber(l.size)
        case AttributeEntityReference(_, entityName) => AttributeString(entityName)
        case x                                       => x
      }
      name -> normalizedValue
    }

    // collect all references and translate them to SqlEntityReference objects
    val refObjects: Vector[SqlEntityReference] = attributes
      .collect {
        case (name, ref: AttributeEntityReference) =>
          Seq(
            SqlEntityReference(a = AttributeName.toDelimitedName(name),
                               n = ref.entityName,
                               t = ref.entityType,
                               s = Some(true)
            )
          )
        case (name, refList: AttributeEntityReferenceList) =>
          refList.list.map(r =>
            SqlEntityReference(a = AttributeName.toDelimitedName(name), n = r.entityName, t = r.entityType, s = None)
          )
      }
      .flatten
      .toVector

    JsObject(
      Map(
        VERSION_KEY -> JsNumber(CURRENT_VERSION),
        ATTRS_KEY -> jsonAttrs.toJson,
        REFS_KEY -> JsArray(refObjects.map(_.toJson))
      )
    )
  }

  // translate a SQL column value back into an AttributeMap
  def fromSql(attributes: Option[String]): AttributeMap =
    attributes.getOrElse("{}").parseJson match {
      case jso: JsObject => deserialize(jso)
      case otherJsValue =>
        throw new CompactEntityDeserializationException(
          s"wanted a JsObject; found a ${otherJsValue.getClass.getName}"
        )
    }

  val slickAttrsPath: String = s"$$.$ATTRS_KEY"
  def slickAttributePath(attributeName: String): String = s"""${slickAttrsPath}."${attributeName}""""
  def slickAttributePath(attributeName: AttributeName): String = slickAttributePath(toDelimitedName(attributeName))

  val slickRefsPath: String = s"$$.$REFS_KEY"

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
          s"wanted an attributes object; found none"
        )
      // version could not be determined
      case Some(otherJsValue) =>
        throw new CompactEntityDeserializationException(
          s"wanted an attributes object; found a ${otherJsValue.getClass.getName}"
        )
    }

  // retrieve the references packet from the database's JSON
  private def getReferences(jso: JsObject): JsArray =
    jso.fields.get(REFS_KEY) match {
      case Some(jsa: JsArray) => jsa
      case None =>
        throw new CompactEntityDeserializationException(
          s"wanted a references array; found none"
        )
      // version could not be determined
      case Some(otherJsValue) =>
        throw new CompactEntityDeserializationException(
          s"wanted a references array; found a ${otherJsValue.getClass.getName}"
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

      /*  version 2 stores references normalized and replaced with sortable/filterable values in the main
            "attrs" object.
          {
            "v": 2,
            "attrs": {
              "attrName1": "Hello world",
              "attrName2": "targetName1", // used for sorting/filtering
              "attrName3": 2,             // used for sorting/filtering
              "attrName4": 123,
            },
            "refs": [
              {"a": "attrName2", "t": "targetType", "n": "targetName1", "s": true},
              {"a": "attrName3", "t": "targetType", "n": "targetName1"},
              {"a": "attrName3", "t": "targetType", "n": "targetName2"}
            ]
          }
       */
      case 2 =>
        // extract the base attributes from the JSON
        val baseAttrs = getAttrs(jso).convertTo[AttributeMap]

        // extract the references from the JSON
        val refs = getReferences(jso).convertTo[Seq[SqlEntityReference]]
        val groupedRefs: Map[String, Seq[SqlEntityReference]] = refs.groupMap(_.a)(identity)
        // discard any references that do not have a matching base attribute
        val filteredGroupedRefs: Map[String, Seq[SqlEntityReference]] = groupedRefs.filter { case (attributeName, _) =>
          baseAttrs.contains(AttributeName.fromDelimitedName(attributeName))
        }
        val refAttrs: AttributeMap = filteredGroupedRefs.map { case (attributeName, refSeq) =>
          val aname = AttributeName.fromDelimitedName(attributeName)
          val attr: Attribute = if (refSeq.isEmpty) {
            AttributeValueEmptyList // no references, so this is an empty list
          } else if (refSeq.head.s.contains(true)) {
            refSeq.head.toAttributeEntityReference // single scalar reference
          } else {
            AttributeEntityReferenceList(refSeq.map(_.toAttributeEntityReference)) // multiple references
          }
          aname -> attr
        }

        // layer the references on top of the base attribute map
        baseAttrs ++ refAttrs

      case x =>
        throw new CompactEntityDeserializationException(
          s"found unexpected version number: $x"
        )
    }

  // SQL representation of an entity reference. We use single-character field names to save space in the database.
  case class SqlEntityReference(
    a: String, // delimited attribute name (namespace:)name
    n: String, // target entity name
    t: String, // target entity type
    s: Option[Boolean] // true if this reference is a scalar; false or None if it is a list
  ) {
    def toAttributeEntityReference: AttributeEntityReference =
      AttributeEntityReference(entityType = t, entityName = n)
  }

  implicit val sqlEntityReferenceFormat: RootJsonFormat[SqlEntityReference] = jsonFormat4(SqlEntityReference)

}
