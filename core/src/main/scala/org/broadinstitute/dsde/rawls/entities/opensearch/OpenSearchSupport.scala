package org.broadinstitute.dsde.rawls.entities.opensearch

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeBoolean, AttributeEntityReference, AttributeFormat, AttributeName, AttributeNumber, AttributeString, AttributeValue, AttributeValueList, AttributeValueRawJson, Entity, PlainArrayAttributeListSerializer, Workspace, WorkspaceJsonSupport}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RestHighLevelClient
import org.opensearch.common.xcontent.XContentType
import org.opensearch.search.SearchHit
import spray.json._
import spray.json.DefaultJsonProtocol._

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

  // list attributes should serialize as ["foo", "bar"], not as {itemsType: "AttributeValue", items: ["45", "46", "47", "48"]}
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  // OpenSearch index name for this workspace
  // TODO: should we use an alias so we can reindex easily?
  val workspaceIndex = indexName(requestArguments.workspace)

  // ================================================================================================
  // utilities for translating from Terra to OpenSearch

  /** generate the OpenSearch document id for a given Terra entity */
  def documentId(entity: Entity): String = documentId(entity.entityType, entity.name)
  def documentId(entityType: String, entityName: String): String = s"$entityType$ID_DELIMITER$entityName"

  /** generate the OpenSearch field name from a Terra attribute name */
  def fieldName(entityType: String, attrName: AttributeName): String =
    s"$entityType$FIELD_DELIMITER${AttributeName.toDelimitedName(attrName)}"

  /** generate the OpenSearch request to index a single Terra entity */
  def indexableDocument(workspace: Workspace, entity: Entity): IndexRequest = {
    val request = new IndexRequest(workspaceIndex) // Use the workspace-specific index
    request.id(documentId(entity)) // Unique id for this document in the index

    // prepend the entityType to each attribute name, to ensure that same-named
    // attributes within different entity types can have different OpenSearch datatypes
    val entityAttrs = entity.attributes.map {
      case (k, v) => (fieldName(entity.entityType, k), v)
    }

    // append the TYPE_FIELD attribute so OpenSearch can distinguish between types
    // the first-class "type" still exists in OpenSearch but it is deprecated:
    // "Types are in the process of being removed"
    val indexableAttrs = entityAttrs + (TYPE_FIELD -> AttributeString(entity.entityType))

    // add the attributes to the document's source
    // TODO: are we content to rely on JSON-ification here? Will this have problems with entity references?
    // TODO: handle entity references!
    request.source(indexableAttrs.toJson.prettyPrint, XContentType.JSON)

    request
  }

  /** generate the OpenSearch index name for a given Terra workspace */
  def indexName(workspace: Workspace): String = workspace.workspaceId

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
    val attributeMap: Map[AttributeName, Attribute] = (fields - TYPE_FIELD).map {
      case (fieldName, fieldValue) =>
        attributeName(entityType, fieldName) -> valueToAttribute(fieldValue)
    }

    // sure would be nice if the attributes were sorted, doesn't seem like downstream serialization supports this
    // val sortMap = scala.collection.immutable.ListMap(attributeMap.toSeq.sortBy(kv => toDelimitedName(kv._1)):_*)

    Entity(name, entityType, attributeMap)
  }

  def valueToAttribute(v: Any): Attribute = v match {
    // simple scalars
    case s:String => AttributeString(s)
    case i:Int => AttributeNumber(i)
    case d:Double => AttributeNumber(d) // I don't think we need float, Double should cover it
    case b:Boolean => AttributeBoolean(b)
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
      val obj = hm.asScala.toMap
      // is this a reference?
      if (obj.keySet == Set("entityName", "entityType") && obj.values.forall(_.isInstanceOf[String])) {
        val stringMap: Map[String, String] = obj.map {
          case (k, v) => (k.toString, v.toString)
        }
        val entityType = stringMap.getOrElse("entityType", "")
        val entityName = stringMap.getOrElse("entityName", "")

        AttributeEntityReference(entityType, entityName)
      } else {
        // not a reference, create this as raw json ... how?
        logger.warn(s"found unhandled OpenSearch HashMap: ${hm.toString}")
        AttributeString(hm.toString)
      }

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

  //  def toEntityReference(jsv:JsValue): Try[AttributeEntityReference] = {
  //    Try {
  //      val jso = jsv.asJsObject
  //      assert (jso.fields.size == 2) // no extra fields
  //      jso.convertTo[AttributeEntityReference]
  //    }
  //  }


}
