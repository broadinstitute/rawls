package org.broadinstitute.dsde.rawls.entities.opensearch

import akka.http.scaladsl.model.Uri
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.model.AttributeFormat.{ENTITY_NAME_KEY, ENTITY_OBJECT_KEYS, ENTITY_TYPE_KEY}
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeBoolean, AttributeEntityReference, AttributeFormat, AttributeList, AttributeName, AttributeNull, AttributeNumber, AttributeString, AttributeValue, AttributeValueList, AttributeValueRawJson, Entity, PlainArrayAttributeListSerializer, Workspace, WorkspaceJsonSupport}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RestHighLevelClient
import org.opensearch.common.xcontent.XContentType
import org.opensearch.search.SearchHit
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait OpenSearchSupport extends LazyLogging {

  // must be overridden by implementing classes
  val client: RestHighLevelClient
  val requestArguments: EntityRequestArguments

  // constants used when talking to OpenSearch
  final val ID_DELIMITER = "/"
  final val FIELD_DELIMITER = "/"
  final val TYPE_FIELD = "sys_entity_type"
  final val NAME_FIELD = "sys_entity_name"
  final val REF_SCHEMA = "terra-data-entity"

  // explicit json de/serializer for attributes, overriding the entity-reference handling in AttributeFormat

  class OpenSearchAttributeFormat extends AttributeFormat with PlainArrayAttributeListSerializer {
    override def writeAttribute(obj: Attribute): JsValue = obj match {
      //ref
      case ref:AttributeEntityReference =>
        JsString(referenceToUri(ref).toString)
      case _ => super.writeAttribute(obj)
    }
    override def readAttribute(json: JsValue): Attribute = json match {
      case JsString(s) if s.toLowerCase.startsWith(s"$REF_SCHEMA://") => uriToReference(s)
      case _ => super.readAttribute(json)
    }
  }

  // list attributes should serialize as ["foo", "bar"], not as {itemsType: "AttributeValue", items: ["45", "46", "47", "48"]}
  implicit val attributeFormat: AttributeFormat = new OpenSearchAttributeFormat

  // OpenSearch index name for this workspace
  // TODO: should we use an alias so we can reindex easily?
  val workspaceIndexAlias = indexName(requestArguments.workspace)

  // ================================================================================================
  // utilities for translating from Terra to OpenSearch

  /** generate the OpenSearch document id for a given Terra entity */
  def documentId(entity: Entity): String = documentId(entity.entityType, entity.name)
  def documentId(entityType: String, entityName: String): String = s"$entityType$ID_DELIMITER$entityName"

  /** generate the OpenSearch field name from a Terra attribute name */
  def fieldName(entityType: String, attrName: AttributeName): String =
    s"$entityType$FIELD_DELIMITER${AttributeName.toDelimitedName(attrName)}"

  /** generate the OpenSearch request to index a single Terra entity */
  def indexableDocument(entity: Entity): IndexRequest = {
    val request = new IndexRequest(workspaceIndexAlias) // Use the workspace-specific index
    request.id(documentId(entity)) // Unique id for this document in the index

    // prepend the entityType to each attribute name, to ensure that same-named
    // attributes within different entity types can have different OpenSearch datatypes
    val entityAttrs = entity.attributes.map {
      case (k, v) => (fieldName(entity.entityType, k), v)
    }

    // append the NAME_FIELD to hold the id, so we don't rely on the built-in "_id" field
    // append the TYPE_FIELD attribute so OpenSearch can distinguish between types
    // the first-class "_type" still exists in OpenSearch but it is deprecated:
    // "Types are in the process of being removed"
    val indexableAttrs = entityAttrs ++ Map(
      TYPE_FIELD -> AttributeString(entity.entityType),
      NAME_FIELD -> AttributeString(entity.name)
    )

    // add the attributes to the document's source
    // TODO: are we content to rely on JSON-ification here? Will this have problems with entity references?
    // TODO: handle entity references!
    // TODO: disable date detection for strings?
    // TODO: ensure numeric detection is disabled (which is the default)

    // when converting from entity to OpenSearch document, use the json serialization that turns an EntityReference
    // into a URI:
    implicit val uriAttributeFormat: AttributeFormat = new OpenSearchAttributeFormat
    request.source(indexableAttrs.toJson(mapFormat(StringJsonFormat, uriAttributeFormat)).prettyPrint, XContentType.JSON)

    request
  }

  def referenceToUri(ref: AttributeEntityReference): Uri = {
    val t = URLEncoder.encode(ref.entityType, StandardCharsets.UTF_8)
    val n = URLEncoder.encode(ref.entityName, StandardCharsets.UTF_8)
    val ws = requestArguments.workspace.workspaceId
    Uri(s"$REF_SCHEMA://$ws/$t/$n")
  }

  /** generate the OpenSearch index name for a given Terra workspace */
  def indexName(workspace: Workspace): String = s"workspace-${workspace.workspaceId}"

  // ================================================================================================
  // utilities for translating from OpenSearch to Terra

  /** generate a Terra entityName from an OpenSearch document id */
  def entityName(documentId: String): String = documentId.split(ID_DELIMITER).tail.mkString

  /** generate a Terra AttributeName from an OpenSearch field name */
  def attributeName(entityType: String, fieldName: String) =
    AttributeName.fromDelimitedName(fieldName.substring(fieldName.indexOf('/')+1))
    // AttributeName.fromDelimitedName(fieldName.replaceFirst(s"$entityType$FIELD_DELIMITER", ""))

  /** generate a Terra entity from an OpenSearch search hit */
  def entity(hit: SearchHit): Entity = {
    val fields = hit.getSourceAsMap.asScala.toMap
    entity(hit.getId, fields)
  }

  /** generate a Terra entity from a OpenSearch collection of fields */
  def entity(id: String, fields: Map[String, AnyRef]): Entity = {
    val name = entityName(id)

    if (!fields.contains(TYPE_FIELD)) {
      logger.error(s"[document $id]: $TYPE_FIELD not found in ${fields.keys.toList.sorted}")
    }

    val entityType = fields(TYPE_FIELD).toString

    // parse all document fields - except _type - into an AttributeMap
    val attributeMap: Map[AttributeName, Attribute] = (fields - TYPE_FIELD - NAME_FIELD).map {
      case (fieldName, fieldValue) =>
        attributeName(entityType, fieldName) -> valueToAttribute(fieldValue)
    }

    // sure would be nice if the attributes were sorted, doesn't seem like downstream serialization supports this
    // val sortMap = scala.collection.immutable.ListMap(attributeMap.toSeq.sortBy(kv => toDelimitedName(kv._1)):_*)

    Entity(name, entityType, attributeMap)
  }

  def valueToAttribute(v: Any): Attribute = v match {
    // simple scalars
    case i:Int => AttributeNumber(i)
    case d:Double => AttributeNumber(d) // I don't think we need float, Double should cover it
    case b:Boolean => AttributeBoolean(b)
    case s:String if s.toLowerCase.startsWith(s"$REF_SCHEMA://") => uriToReference(s) // entity refs
    case s:String => AttributeString(s)

    // arrays
    case al:util.ArrayList[_] =>
      val items:Seq[AttributeValue] = al.asScala.map(item =>
        valueToAttribute(item) match {
          case av:AttributeValue => av
          case x => throw new Exception(s"found ${x.getClass.getSimpleName} in value list!")
        })
      AttributeValueList(items)

    // OpenSearch objects - aka json - are returned as HashMaps
    // TODO: handle entity references better than this!
    case hm: util.HashMap[_, _] =>
      AttributeString(hm.toString)
//      val obj = hm.asScala.toMap
//      // is this a reference?
//      if (obj.keySet == Set("entityName", "entityType") && obj.values.forall(_.isInstanceOf[String])) {
//        val stringMap: Map[String, String] = obj.map {
//          case (k, v) => (k.toString, v.toString)
//        }
//        val entityType = stringMap.getOrElse("entityType", "")
//        val entityName = stringMap.getOrElse("entityName", "")
//
//        AttributeEntityReference(entityType, entityName)
//      } else {
//        // not a reference, create this as raw json ... how?
//        logger.warn(s"found unhandled OpenSearch HashMap: ${hm.toString}")
//        AttributeString(hm.toString)
//      }

    // neither a simple scalar nor an array ...
    case x =>
      // try to parse this as json
      Try(x.toString.parseJson) match {
        case Success(jsv) => AttributeValueRawJson(jsv)
        case Failure(_) =>
          // we don't know what this is, just toString it
          logger.warn(s"found unhandled OpenSearch field type: ${x.getClass.getSimpleName}")
          AttributeString(x.toString)
      }
  }

  def uriToReference(uri: String): AttributeEntityReference = {
    val refUri = Uri(uri)
    val _ = refUri.path.head.toString // unused in deserialization
    val t = URLDecoder.decode(refUri.path.tail.head.toString, StandardCharsets.UTF_8)
    val n = URLDecoder.decode(refUri.path.tail.tail.toString, StandardCharsets.UTF_8)
    AttributeEntityReference(t, n)
  }

}
