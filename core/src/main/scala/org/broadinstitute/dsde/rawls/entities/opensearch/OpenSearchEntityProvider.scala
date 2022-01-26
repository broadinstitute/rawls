package org.broadinstitute.dsde.rawls.entities.opensearch

import akka.http.scaladsl.model.Uri
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base._
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model._
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest, GetMappingsRequest}
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.{BoolQueryBuilder, QueryStringQueryBuilder, TermsQueryBuilder}
import org.opensearch.rest.RestStatus
import org.opensearch.search.SearchHit
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.opensearch.search.aggregations.{Aggregation, AggregationBuilders}
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

import java.util
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class OpenSearchEntityProvider(requestArguments: EntityRequestArguments, client: RestHighLevelClient)
                              (implicit protected val executionContext: ExecutionContext)
  extends WorkspaceJsonSupport with EntityProvider with LazyLogging with ExpressionEvaluationSupport {

  override val entityStoreId: Option[String] = Option("OpenSearch")

  // list attributes should serialize as ["foo", "bar"], not as {itemsType: "AttributeValue", items: ["45", "46", "47", "48"]}
  override implicit val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  // constants used when talking to OpenSearch
  final val ID_DELIMITER = "/"
  final val FIELD_DELIMITER = "."
  final val TYPE_FIELD = "sys_entity_type"

  // OpenSearch index for this workspace
  val workspaceIndex = indexName(requestArguments.workspace)

  // ================================================================================================
  // API methods we support

  override def entityTypeMetadata(useCache: Boolean = false): Future[Map[String, EntityTypeMetadata]] = {
    // one request to get the entityType->document count aggregation
    val req = new SearchRequest(workspaceIndex)

    val entityTypeAgg = AggregationBuilders.terms(TYPE_FIELD).field(s"$TYPE_FIELD")

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder
      .size(0)
      .aggregation(entityTypeAgg)

    req.source(searchSourceBuilder)

    val resp = client.search(req, RequestOptions.DEFAULT)

    val aggResults = resp.getAggregations
    val typeCounts: Aggregation = aggResults.get(TYPE_FIELD)
    val typesAndCounts = typeCounts match {
      case pst:ParsedStringTerms =>
        pst.getBuckets.asScala.toList.map(bucket => (bucket.getKeyAsString, bucket.getDocCount))
      case x =>
        throw new Exception(s"found unexpected aggregation type: ${x.getClass.getName}")
    }

    // another request to get the field mappings for this index
    val mappingsRequest = new GetMappingsRequest()
    mappingsRequest.indices(workspaceIndex)
    val mappingsResponse = client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT)

    // TODO: find a better way than the asInstanceOfs in here
    val indexMapping = mappingsResponse.mappings().get(workspaceIndex)
    val mapping: Map[String, AnyRef] = indexMapping.getSourceAsMap.asScala.toMap

    def wigglyMapLookup(source: Map[String, AnyRef], key: String): Map[String, AnyRef] = {
      if (!source.contains(key))
        throw new Exception(s"source map does not contain key $key")
      source(key) match {
        case javamap:java.util.LinkedHashMap[_,_] => javamap.asScala.toMap.asInstanceOf[Map[String, AnyRef]]
        case x =>
          throw new Exception(s"'$key' lookup returned ${x.getClass.getSimpleName}, not another map")
      }
    }

    val props: Map[String, AnyRef] = wigglyMapLookup(mapping, "properties")

    val typesAndMetadata = typesAndCounts.sortBy(_._1).map {
      case (entityType, count) =>
        // find the attribute names associated with this entityType
        val thisTypeMappings: Map[String, AnyRef] = wigglyMapLookup(props, entityType)
        val thisTypeAttributes = wigglyMapLookup(thisTypeMappings, "properties").keySet.toSeq.sorted

        entityType -> EntityTypeMetadata(count.toInt, s"${entityType}_id", thisTypeAttributes)
    }.toMap

    Future(typesAndMetadata)

  }

  override def createEntity(entity: Entity): Future[Entity] = {
    // TODO: should reuse indexEntity() instead
    val indexRequest = indexableDocument(requestArguments.workspace, entity)
    indexRequest.create(true) // set as create-only
    val indexResponse = client.index(indexRequest, RequestOptions.DEFAULT)
    if (indexResponse.status() == RestStatus.CREATED) {
      Future(entity)
    } else {
      // TODO: better error messaging
      throw new Exception(s"failed to index document: ${indexResponse.getResult.toString}")
    }
  }

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] = {
    val bulkRequest = new BulkRequest(workspaceIndex)
    // val brb = new BulkRequestBuilder(client.getLowLevelClient, BulkAction.INSTANCE)

    entityRefs.foreach { ref =>
      val docid = documentId(ref.entityType, ref.entityName)
      val deleteRequest = new DeleteRequest(workspaceIndex, docid)
      bulkRequest.add(deleteRequest)
    }

    val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)
    val successes = bulkResponse.getItems.collect {
      case item if !item.isFailed => item
    }

    if (successes.length == entityRefs.length) {
      Future.successful(successes.length)
    } else {
      Future.failed(new Exception(s"Expected to delete ${entityRefs.length}; only deleted ${successes.length}"))
    }
  }

  override def deleteEntityAttributes(entityType: String, attributeNames: Set[AttributeName]) =
    throw new UnsupportedEntityOperationException("delete-attributes not supported by this provider.")

  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    //Getting back the document
    val getRequest = new GetRequest(workspaceIndex, s"${entityType}/${entityName}")
    val getResponse = client.get(getRequest, RequestOptions.DEFAULT)

    if (getResponse.isExists) {
      Try(entity(getResponse.getId, getResponse.getSource.asScala.toMap)) match {
        case Success(e) => Future(e)
        case Failure(ex) =>
          val regrets = new RawlsExceptionWithErrorReport(errorReport = ErrorReport(getResponse.getSourceAsString, ErrorReport(ex)))
          Future.failed(regrets)
      }
    } else {
      Future.failed(new EntityNotFoundException(getResponse.getSourceAsString))
    }
  }

  override def queryEntities(entityType: String, incomingQuery: EntityQuery, parentSpan: Span = null): Future[EntityQueryResponse] = {
    val req = new SearchRequest(workspaceIndex)

    val typeQuery = new TermsQueryBuilder(TYPE_FIELD, entityType)

    // branch depending on if a term is specified
    val filterTermsQueryOption = incomingQuery.filterTerms.map { terms =>
      val params = Uri.Query(("query", terms))
      new QueryStringQueryBuilder(params.toString())
    }

    val finalQuery = filterTermsQueryOption match {
      case None => typeQuery
      case Some(filterTermsQuery) =>
        val bq = new BoolQueryBuilder()
        bq.must(typeQuery)
        bq.must(filterTermsQuery)
        bq
    }

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.size(incomingQuery.pageSize)
    searchSourceBuilder.from((incomingQuery.page-1)*incomingQuery.pageSize)
    searchSourceBuilder.query(finalQuery)
    val sortOrder = if (incomingQuery.sortDirection == SortDirections.Descending)
      SortOrder.DESC
    else {
      SortOrder.ASC
    }
    // TODO OpenSearch: better handling when the sort field does not exist or does not have a .keyword mapping
    val sortField = fieldName(entityType, AttributeName.fromDelimitedName(incomingQuery.sortField)) + ".keyword"
    searchSourceBuilder.sort(sortField, sortOrder)

    req.source(searchSourceBuilder)

    val resp = client.search(req, RequestOptions.DEFAULT)

    val filteredCount = resp.getHits.getTotalHits.value
    val filteredPageCount = Math.max(Math.ceil(filteredCount.toFloat / incomingQuery.pageSize), 1)

    // if we have terms, we need to make a separate query just to get the unfiltered count
    val unfilteredCount = if (incomingQuery.filterTerms.isDefined) {
      val searchSourceBuilder = new SearchSourceBuilder()
      searchSourceBuilder.size(0)
      searchSourceBuilder.query(typeQuery)
      req.source(searchSourceBuilder)
      val resp = client.search(req, RequestOptions.DEFAULT)
      resp.getHits.getTotalHits.value
    } else {
      filteredCount
    }

    val resultMetadata = EntityQueryResultMetadata(unfilteredCount.toInt, filteredCount.toInt, filteredPageCount.toInt)

    val searchHits = resp.getHits.asScala

    val results = searchHits.map { hit =>
      entity(hit)
    }

    Future(EntityQueryResponse(incomingQuery, resultMetadata, results.toSeq))

  }

  override def batchUpsertEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
    throw new UnsupportedEntityOperationException("batch-upsert entities not supported by this provider.")
  }

  // ================================================================================================
  // API methods we haven't implemented

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]] =
    throw new UnsupportedEntityOperationException("evaluateExpressions not supported by this provider.")

  override def expressionValidator: ExpressionValidator =
    throw new UnsupportedEntityOperationException("expressionValidator not supported by this provider.")

  override def batchUpdateEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] =
    throw new UnsupportedEntityOperationException("batch-update entities not supported by this provider.")

  // ================================================================================================
  // class-specific methods
  def createWorkspaceIndex(workspace: Workspace) = {
    val getIndexRequest = new GetIndexRequest(workspaceIndex)
    val getIndexResponse = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!getIndexResponse) {
      logger.info(s"OpenSearch index for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} does not exist; creating ... ")
      val createIndexRequest = new CreateIndexRequest(workspaceIndex)

      // easiest way to create mappings
      val mappings =
        s"""
        {
            "properties": {
                "$TYPE_FIELD": {"type": "keyword"}
            }
        }
        """
      createIndexRequest.mapping(mappings, XContentType.JSON)

      // TODO: explicit mappings for other fields

      val createIndexResponse = client.indices.create(createIndexRequest, RequestOptions.DEFAULT)
      logger.info(s"OpenSearch index for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} created: ${createIndexResponse.index()}")
    } else {
      logger.info(s"OpenSearch index for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} exists already; no action needed.")
    }
  }

  def indexEntity(workspace: Workspace, entity: Entity) = {
    val request = indexableDocument(workspace, entity)
    val indexResponse = client.index(request, RequestOptions.DEFAULT)
    indexResponse.status().toString
  }

  def indexEntities(workspace: Workspace, entities: List[Entity]): List[String] = {
    val bulkRequest = new BulkRequest()
    entities.foreach { entity =>
      val indexRequest = indexableDocument(workspace, entity)
      bulkRequest.add(indexRequest)
    }
    val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)
    val statusResponses = bulkResponse.getItems map { bulkItemResponse => bulkItemResponse.status().toString }
    statusResponses.toList
  }

  // ================================================================================================
  // utilities for translating from Terra to OpenSearch

  /** generate the document id for a given entity */
  private def documentId(entity: Entity): String = documentId(entity.entityType, entity.name)
  private def documentId(entityType: String, entityName: String): String = s"$entityType$ID_DELIMITER$entityName"

  /** generate a field name from an attribute name */
  private def fieldName(entityType: String, attrName: AttributeName): String =
    s"$entityType$FIELD_DELIMITER${AttributeName.toDelimitedName(attrName)}"

  /** generate the OpenSearch request to index a single entity */
  private def indexableDocument(workspace: Workspace, entity: Entity): IndexRequest = {
    val request = new IndexRequest(workspaceIndex) // Use the workspace-specific index
    request.id(documentId(entity)) // Unique id for this document in the index

    // prepend the entityType to each attribute name, to ensure that same-named
    // attributes within different entity types can have different OpenSearch datatypes
    val entityAttrs = entity.attributes.map {
      case (k, v) => (fieldName(entity.entityType, k), v)
    }

    // append the "_type" attribute so OpenSearch can distinguish between types
    // the first-class "type" still exists in OpenSearch but it is deprecated:
    // "Types are in the process of being removed"
    val indexableAttrs = entityAttrs + (TYPE_FIELD -> AttributeString(entity.entityType))

    // add the attributes to the document's source
    // TODO: are we content to rely on JSON-ification here? Will have problems with entity references
    request.source(indexableAttrs.toJson.prettyPrint, XContentType.JSON)

    request
  }

  /** generate the OpenSearch index name for a given workspace */
  private def indexName(workspace: Workspace): String = workspace.workspaceId

  // ================================================================================================
  // utilities for translating from OpenSearch to Terra

  /** generate an entityName from an OpenSearch document id */
  private def entityName(documentId: String): String = documentId.split(ID_DELIMITER).tail.mkString

  /** generate an AttributeName from an OpenSearch field name */
  private def attributeName(entityType: String, fieldName: String) =
    AttributeName.fromDelimitedName(fieldName.replaceFirst(s"$entityType$FIELD_DELIMITER", ""))

  /** generate a Terra entity from an OpenSearch result */
  private def entity(hit: SearchHit): Entity = {

    val fields = hit.getSourceAsMap.asScala.toMap

    entity(hit.getId, fields)
  }

  private def entity(id: String, fields: Map[String, AnyRef]): Entity = {
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

    Entity(name, entityType, attributeMap)
  }

  private def valueToAttribute(v: Any): Attribute = v match {
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

  private def toEntityReference(jsv:JsValue): Try[AttributeEntityReference] = {
    Try {
      val jso = jsv.asJsObject
      assert (jso.fields.size == 2) // no extra fields
      jso.convertTo[AttributeEntityReference]
    }
  }


}
