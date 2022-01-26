package org.broadinstitute.dsde.rawls.entities.opensearch

import akka.http.scaladsl.model.{StatusCode, Uri}
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
import org.opensearch.action.search.SearchRequest
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest, GetMappingsRequest, PutMappingRequest}
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.{BoolQueryBuilder, QueryStringQueryBuilder, TermsQueryBuilder}
import org.opensearch.index.reindex.UpdateByQueryRequest
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.opensearch.search.aggregations.{Aggregation, AggregationBuilders}
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class OpenSearchEntityProvider(override val requestArguments: EntityRequestArguments,
                               override val client: RestHighLevelClient)
                              (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider
    with OpenSearchSupport
    with ExpressionEvaluationSupport {

  override val entityStoreId: Option[String] = Option("OpenSearch")

  // ================================================================================================
  // API methods we support

  /** entity type metadata - in good shape except for unused mappings */
  override def entityTypeMetadata(useCache: Boolean = false): Future[Map[String, EntityTypeMetadata]] = {
    // TODO Opensearch: need a way to detect unused mappings.
    /* To handle deletes and prune unused mappings:
        - reindex into a temp index, which recreate mappings. Compare mappings with the temp index. (yuck)
        - scroll-query all documents, collect field names in middleware (yuck)
        - run exists query for each mapping (https://www.elastic.co/guide/en/elasticsearch/reference/7.10/query-dsl-exists-query.html),
            this will show unused for null, [], larger than ignore_above, malformed and ignore_malformed
        -
     */

    // one request to get the field mappings for this index
    val mappingsRequest = new GetMappingsRequest()
    mappingsRequest.indices(workspaceIndex)
    val mappingsResponse = client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT)

    val indexMapping = mappingsResponse.mappings().get(workspaceIndex)
    val mapping: Map[String, AnyRef] = indexMapping.getSourceAsMap.asScala.toMap

    // TODO: find a better way than the asInstanceOf in here
    def wigglyMapLookup(source: Map[String, AnyRef], key: String): Map[String, AnyRef] = {
      if (!source.contains(key))
        throw new Exception(s"source map does not contain key $key")
      source(key) match {
        case javamap:java.util.LinkedHashMap[_,_] => javamap.asScala.toMap.asInstanceOf[Map[String, AnyRef]]
        case x =>
          throw new Exception(s"'$key' lookup returned ${x.getClass.getSimpleName}, not another map")
      }
    }

    case class TypeAndAttributeName(entityType: String, attributeName: String)

    // props will be a flat list of "entityType/attributeName" values, plus any other fields like sys_entity_type
    val fieldNames: Set[TypeAndAttributeName] = wigglyMapLookup(mapping, "properties") // get the properties from the mappings response
      .keySet                // get just the keys, i.e. the field names
      .map(_.split('/'))     // un-delimit the "entityType/attributeName" field names
      .filter(_.length == 2) // omit "sys_entity_type" or other non-delimited field names (or those with multiple delimiters)
      .map(arr => TypeAndAttributeName(arr.head, arr.last)) // use handy case class instead of raw array

    // another request to get the entityType->document count aggregation
    val req = new SearchRequest(workspaceIndex)

    // by default, terms aggregation will only return 10 buckets. We want to return all types, so we need to know
    // how many types there are in this index. We can get this from the mappings query.
    val distinctTypes = fieldNames.map(_.entityType)

    val entityTypeAgg = AggregationBuilders
      .terms(TYPE_FIELD)
      .field(s"$TYPE_FIELD")
      .size(distinctTypes.size)

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

    assert(typesAndCounts.size == distinctTypes.size, s"typesAndCounts.size of ${typesAndCounts.size} != distinctTypes.size of ${distinctTypes.size}")

    val typesAndMetadata = typesAndCounts.sortBy(_._1).map {
      case (entityType, count) =>
        // find the attribute names associated with this entityType
        val thisTypeAttributes = fieldNames.filter(_.entityType == entityType).map(_.attributeName).toSeq.sorted
        entityType -> EntityTypeMetadata(count.toInt, s"${entityType}_id", thisTypeAttributes)
    }.toMap

    Future(typesAndMetadata)

  }

  /** createEntity - easy, the tricky logic is in indexableDocument */
  override def createEntity(entity: Entity): Future[Entity] = {
    // TODO OpenSearch: should reuse indexEntity() instead
    val indexRequest = indexableDocument(requestArguments.workspace, entity)
    indexRequest.create(true) // set as create-only

    val indexResponseAttempt = Try(client.index(indexRequest, RequestOptions.DEFAULT))
    // TODO OpenSearch: better error messaging
    indexResponseAttempt match {
      case Success(indexResponse) =>
        if (indexResponse.status() == RestStatus.CREATED) {
          Future(entity)
        } else {
          throw new Exception(s"failed to index document: ${indexResponse.getResult.toString}")
        }

      case Failure (se: org.opensearch.OpenSearchStatusException) =>
        val statusCode = se.status().getStatus
        val errRpt = ErrorReport(StatusCode.int2StatusCode(statusCode), se.getRootCause.getMessage, se)
        throw new RawlsExceptionWithErrorReport(errRpt)

      case Failure(re: org.opensearch.client.ResponseException) =>
        val statusCode = re.getResponse.getStatusLine.getStatusCode
        val errRpt = ErrorReport(StatusCode.int2StatusCode(statusCode), re.getMessage, re)
        throw new RawlsExceptionWithErrorReport(errRpt)

      case Failure(ex) =>
        logger.error(s"caught a ${ex.getClass.getName}")
        throw ex
    }
  }

  /** delete entities - easy, but needs some logic to poke a mappings purge in case attributes no longer exist */
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

  /** delete entity attrs - needs scale testing, since this touches all documents of a given type.
    * also needs logic to poke a mappings purge. */
  override def deleteEntityAttributes(entityType: String, attributeNames: Set[AttributeName]): Future[Unit] = {
    if (attributeNames.isEmpty)
      throw new Exception("must remove at least one attribute")

    // query to target all entities of the given type
    val typeQuery = new TermsQueryBuilder(TYPE_FIELD, entityType)

    // translate entity attribute names to OpenSearch field names
    val fieldsToRemove = attributeNames.map(attr => fieldName(entityType, attr))

    // generate the script to remove the field from all documents of this type.
    // optimize for the case of removing a single column.
    val scriptContent = fieldsToRemove.map { field =>
      s"""ctx._source.remove("$field");"""
    }
    val script = new Script(scriptContent.mkString("\n"))

    val updateRequest = new UpdateByQueryRequest(workspaceIndex)
    updateRequest.setConflicts("abort") // (Default) error if another thread updated one of the documents before we get to it
    // updateRequest.setScroll() // (Optional) timeout for the underlying scroll-search context
    updateRequest.setQuery(typeQuery) // which documents to target for updates
    updateRequest.setScript(script) // the updates to perform via script

    val updateResponse = client.updateByQuery(updateRequest, RequestOptions.DEFAULT)

    logger.info(s"deleteEntityAttributes ${updateResponse.getStatus} in ${updateResponse.getTook.toString}: updated ${updateResponse.getUpdated} documents")

    // purgeMappings(requestArguments.workspace, entityType, attributeNames)

    Future(())

  }

  /** Get entity - easy */
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

  /** query entities:
    * need to handle sorting by non-.keyword fields
    * need to handle error messaging for requested sorts that don't exist
    * need to support field selection
    * need to validate correctness of filterTerms
    * */
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

  // TODO: implement
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

  def purgeMappings(workspace: Workspace, entityType: String, attributeNames: Set[AttributeName]): List[String] = {
    // consider only the specified fields to be purged
    val fieldsToPurge = attributeNames.map(attr => fieldName(entityType, attr))

    // TODO OpenSearch: extract a common "get buckets for specified fields" method
    // another request to get the entityType->document count aggregation
    val req = new SearchRequest(workspaceIndex)

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder
      .size(0)

    fieldsToPurge.foreach { fld =>
      val fieldAgg = AggregationBuilders
        .terms(fld)
        .field(s"$fld.keyword")
        .size(1) // we only care if it has ANY values, not what those values are
      searchSourceBuilder.aggregation(fieldAgg)
    }

    req.source(searchSourceBuilder)

    val resp = client.search(req, RequestOptions.DEFAULT)

    val aggResults = resp.getAggregations

    val purgeableFields = aggResults.asScala.collect {
      case pst:ParsedStringTerms if pst.getBuckets.isEmpty => pst.getName
    }

    logger.info(s"********** we should purge: $purgeableFields")

    val deleteMappingsRequest = new PutMappingRequest()

    purgeableFields.toList

  }

  def purgeMappings(workspace: Workspace) = {
    // retrieve mappings for this workspace's index
    val mappingsRequest = new GetMappingsRequest()
    mappingsRequest.indices(workspaceIndex)
    val mappingsResponse = client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT)



    // for each type:
      // perform aggregation query, including an agg for each mapping field. only need size=1 for each aggregation.
      // any aggregation that returned a doc count means this field is populated.


  }




}
