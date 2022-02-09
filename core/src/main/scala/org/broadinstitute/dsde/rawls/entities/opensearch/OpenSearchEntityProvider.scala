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
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.admin.indices.alias.{Alias, IndicesAliasesRequest}
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.bulk.{BulkRequest, BulkResponse}
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.{GetRequest, GetResponse, MultiGetRequest}
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.{ClearScrollRequest, ClearScrollResponse, SearchRequest, SearchScrollRequest}
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.IndicesOptions.WildcardStates
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.client.indices.{CreateIndexRequest, DeleteAliasRequest, GetIndexRequest, GetMappingsRequest}
import org.opensearch.common.StopWatch
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.VersionType
import org.opensearch.index.query.{BoolQueryBuilder, QueryStringQueryBuilder, TermsQueryBuilder}
import org.opensearch.index.reindex.{ReindexRequest, UpdateByQueryRequest}
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.SearchHit
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.opensearch.search.aggregations.{Aggregation, AggregationBuilders}
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder

import java.util
import java.util.EnumSet
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class OpenSearchEntityProvider(override val requestArguments: EntityRequestArguments,
                               override val client: RestHighLevelClient)
                              (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider
    with OpenSearchSupport
    with AttributeSupport
    with ExpressionEvaluationSupport {

  override val entityStoreId: Option[String] = Option("OpenSearch")

  // TODO: need to set index.mapping.total_fields.limit on the index; default limit is 1000

  // TODO: Entity references are stored as URIs, in the form:
  // terra-data-entity://workspaceId/URLEncoded(entityType)/URLEncoded(subject_HCC1143)
  // for example:
  // terra-data-entity://9a8a946b-445a-40b8-8704-7ba6e1ff0d42/participant/subject_HCC1143

  // ================================================================================================
  // API methods we support

  /** entity type metadata - in good shape except for unused mappings */
  override def entityTypeMetadata(useCache: Boolean = false): Future[Map[String, EntityTypeMetadata]] = {
    // TODO OpenSearch: need a way to detect unused mappings.
    /* To handle deletes and prune unused mappings:
        - reindex into a temp index, which recreate mappings. Compare mappings with the temp index. (yuck)
        - scroll-query all documents, collect field names in middleware (yuck)
        - run exists query for each mapping (https://www.elastic.co/guide/en/elasticsearch/reference/7.10/query-dsl-exists-query.html),
            this will show unused for null, [], larger than ignore_above, malformed and ignore_malformed
        -
     */

    // one request to get the field mappings for this index
    val mappingsRequest = new GetMappingsRequest()
    mappingsRequest.indices(workspaceIndexAlias)
    val mappingsResponse = client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT)

    logger.info(s"mappingsResponse: ${mappingsResponse.mappings().keySet()}")

    logQueryTiming(new TimeValue(-1), "mappings query (TODO: measure timing)")

    assert(mappingsResponse.mappings().size() == 1, "found mappings for more than one index!")

    val indexMapping = mappingsResponse.mappings().asScala.head._2
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
    val req = new SearchRequest(workspaceIndexAlias)

    // by default, terms aggregation will only return 10 buckets. We want to return all types, so we need to know
    // how many types there are in this index. We can get this from the mappings query.
    val distinctTypes = fieldNames.map(_.entityType)

    if (distinctTypes.isEmpty) {
      // if there are no types yet, don't bother with any other queries
      Future(Map())
    } else {
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

      logQueryTiming(resp.getTook, "aggregation query for per-type counts")

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



  }

  /** createEntity - easy, the tricky logic is in indexableDocument */
  override def createEntity(entity: Entity): Future[Entity] = {
    val indexResponseAttempt = Try(indexEntity(entity))

    // TODO OpenSearch: better error messaging
    indexResponseAttempt match {
      case Success(indexResponse) =>
        logQueryTiming(new TimeValue(-1), "index single doc query (TODO: measure timing)")
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
    val bulkRequest = new BulkRequest(workspaceIndexAlias)
    // val brb = new BulkRequestBuilder(client.getLowLevelClient, BulkAction.INSTANCE)

    entityRefs.foreach { ref =>
      val docid = documentId(ref.entityType, ref.entityName)
      val deleteRequest = new DeleteRequest(workspaceIndexAlias, docid)
      bulkRequest.add(deleteRequest)
    }

    val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)

    logQueryTiming(bulkResponse.getTook, s"bulk delete request, for ${entityRefs.size} docs")

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

    val updateRequest = new UpdateByQueryRequest(workspaceIndexAlias)
    updateRequest.setConflicts("abort") // (Default) error if another thread updated one of the documents before we get to it
    // updateRequest.setScroll() // (Optional) timeout for the underlying scroll-search context
    updateRequest.setQuery(typeQuery) // which documents to target for updates
    updateRequest.setScript(script) // the updates to perform via script

    val updateResponse = client.updateByQuery(updateRequest, RequestOptions.DEFAULT)
    logQueryTiming(updateResponse.getTook, s"update-by-query request, deleting ${attributeNames.size} fields from ${updateResponse.getTotal} docs")

    logger.info(s"deleteEntityAttributes ${updateResponse.getStatus} in ${updateResponse.getTook.toString}: updated ${updateResponse.getUpdated} documents")

    // purgeMappings(requestArguments.workspace, entityType, attributeNames)

    Future(())

  }

  /** Get entity - easy */
  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    //Getting back the document
    val getRequest = new GetRequest(workspaceIndexAlias, s"$entityType/$entityName")

    val getResponse = timedQuery[GetResponse](Option("get single doc query (measured via stopwatch)")) {
      client.get(getRequest, RequestOptions.DEFAULT)
    }

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
    * need to handle sorting by non-.raw fields
    * need to handle error messaging for requested sorts that don't exist
    * need to support sorting by "name"
    * need to support field selection
    * need to validate correctness of filterTerms
    * */
  override def queryEntities(entityType: String, incomingQuery: EntityQuery, parentSpan: Span = null): Future[EntityQueryResponse] = {
    val req = new SearchRequest(workspaceIndexAlias)

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

    // set the fields to return
    incomingQuery.fields.fields.foreach { fieldNames =>
      searchSourceBuilder.fetchSource(false)
      searchSourceBuilder.docValueField(NAME_FIELD)
      searchSourceBuilder.docValueField(TYPE_FIELD)
      fieldNames.foreach { fld =>
        val esname = fieldName(entityType, AttributeName.fromDelimitedName(fld)) + ".raw"
        searchSourceBuilder.docValueField(esname)
      }
    }

    val sortOrder = if (incomingQuery.sortDirection == SortDirections.Descending)
      SortOrder.DESC
    else {
      SortOrder.ASC
    }
    // TODO OpenSearch: better error messages when the sort field does not exist or does not have a .raw mapping
    val sortField = incomingQuery.sortField match {
      case "name" =>
        // magic string "name" means sort by the entity name, which we store in NAME_FIELD
        NAME_FIELD
      case other =>
        // sorting by something other than "name"; look for the .raw multifield, which is mapped for sorting:
        // keyword for strings, else number/bool for those datatypes
        fieldName(entityType, AttributeName.fromDelimitedName(other)) + ".raw"
    }

    searchSourceBuilder.sort(sortField, sortOrder)

    req.source(searchSourceBuilder)

    val resp = client.search(req, RequestOptions.DEFAULT)

    logQueryTiming(resp.getTook, s"search query, filtered to ${resp.getHits.getTotalHits.value} docs, returning ${resp.getHits.getHits.length} docs")

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

  /** list entities: relatively straightforward to implement via a scroll-search. However, ES documentation
    * suggests using a Point-In-Time context instead, but that is only available in X-Pack. This may
    * need some profiling to inspect actual memory usage and performance. */
  override def listEntities(entityType: String, parentSpan: Span): Future[Seq[Entity]] = {
    val batchSize = 750 // 10000
    val keepAlive = new TimeValue(3, TimeUnit.MINUTES)

    // TODO: recommended way to do large pagination is via a Point-In-Time (PIT) context. This seems to only
    // be available in X-Pack. So, we do a scroll search instead:
    // https://opensearch.org/docs/latest/opensearch/rest-api/scroll/

    // recursive scroll queries, may be used later
    @tailrec
    def doScroll(scrollId: String, hitsSoFar: List[SearchHit] = List.empty[SearchHit]): List[SearchHit] = {
      val searchScrollRequest = new SearchScrollRequest()
      searchScrollRequest.scrollId(scrollId)
      searchScrollRequest.scroll(keepAlive)
      val searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT)

      val combinedHits = hitsSoFar ++ searchResponse.getHits.asScala.toList

      logQueryTiming(searchResponse.getTook, s"additional scroll query (${combinedHits.length}/${searchResponse.getHits.getTotalHits.value}), using scroll: $scrollId")

      if (searchResponse.getHits.getTotalHits.value > combinedHits.length) {
        doScroll(searchResponse.getScrollId, combinedHits)
      } else {

        // delete scroll to free up server memory. It would clear itself in ${keepAlive} anyway.
        // we use the client.clearScrollAsync() method here, with a no-op listener, to fire-and-forget
        // the delete-scroll request. This is an exercise in using the ES client's async methods.
        // here in Scala, we could just as easily wrapped the synchronous client.clearScroll() method
        // in a Future.
        val clearScrollRequest = new ClearScrollRequest()
        clearScrollRequest.addScrollId(searchResponse.getScrollId)
        val clearScrollListener: ActionListener[ClearScrollResponse] = ActionListener.wrap(() => ())
        client.clearScrollAsync(clearScrollRequest, RequestOptions.DEFAULT, clearScrollListener)

        combinedHits
      }
    }


    // initial search, sets filter criteria (entityType) and sort and gets the initial scroll id
    val req = new SearchRequest(workspaceIndexAlias)
    req.scroll(keepAlive) // time for each batch
    val typeQuery = new TermsQueryBuilder(TYPE_FIELD, entityType)
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(typeQuery)
    searchSourceBuilder.size(batchSize) // sets batch size
    searchSourceBuilder.sort("_doc", SortOrder.ASC) // consistent sort, _doc is most efficient according to documentation
    req.source(searchSourceBuilder)
    val initialResponse = client.search(req, RequestOptions.DEFAULT)

    // did we get all the results already?
    val total = initialResponse.getHits.getTotalHits.value
    val thisPage = initialResponse.getHits.getHits.length

    logQueryTiming(initialResponse.getTook, s"initial query for scroll-search ($thisPage/$total)")

    val searchHits = if (total > thisPage) {
      doScroll(initialResponse.getScrollId, initialResponse.getHits.asScala.toList)
    } else {
      initialResponse.getHits.asScala.toList
    }

    val results = searchHits.map { hit =>
      entity(hit)
    }

    Future(results)

  }


  /** batch upsert: can incur large reads and writes, probably should batch these;
    * otherwise relatively easy by reusing applyOperationsToEntity and relying
    * on round-tripping between Entity and Document */
  override def batchUpsertEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
    batchUpdateEntitiesImpl(entityUpdates, upsert = true)
  }

  override def batchUpdateEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
    batchUpdateEntitiesImpl(entityUpdates, upsert = false)
  }

  def batchUpdateEntitiesImpl(entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean): Future[Traversable[Entity]] = {
    // inspect incoming entityUpdates, identify unique entityType/name pairs
    val docids: Set[String] = entityUpdates.map { eud => documentId(eud.entityType, eud.name) }.toSet

    // TODO: batch this so we don't necessarily retrieve all at once

    // retrieve documents from ES based on entityType/name pairs
    val multiGetRequest = new MultiGetRequest()
    docids.foreach { docid =>
      multiGetRequest.add(workspaceIndexAlias, docid)
    }

    val stopwatch = new StopWatch()
    stopwatch.start()
    val multiGetResponse = client.mget(multiGetRequest, RequestOptions.DEFAULT)
    stopwatch.stop()
    logQueryTiming(stopwatch.lastTaskTime(), s"batchUpsert lookup for existing documents (${multiGetResponse.getResponses.length} responses)")

    // abort on any retrieval failure
    val failures = multiGetResponse.getResponses collect {
      case item if item.isFailed => item.getFailure
    }
    if (failures.nonEmpty) {
      // TODO: the list of failures could be large, limit it so we don't create a huge response
      throw new RawlsExceptionWithErrorReport(ErrorReport(s"Error retrieving documents: {${failures.map(_.getMessage).mkString("Array(", ", ", ")")}"))
    }

    // build map of docid -> document, based on the response
    // since we already checked for failures, we are safe to assume
    // none of these failed
    val docs: Map[String, GetResponse] = multiGetResponse.getResponses.map { item =>
      item.getId -> item.getResponse
    }.toMap

    // here are the ES seq number and primary terms for the documents - ensure these get passed back on writes!
    val concurrencyInfo = multiGetResponse.getResponses.map { resp =>
      resp.getResponse.getId ->
        (resp.getResponse.getSeqNo, resp.getResponse.getPrimaryTerm)
    }.toMap
//    logger.info(s"concurrency info for documents being updated: $concurrencyInfo")

    // validate update-only
    if (!upsert) {
      // do any document not exist already?
      val nonExistent = multiGetResponse.getResponses.collect {
        case item if !item.getResponse.isExists => item.getId
      }
      if (nonExistent.nonEmpty)
        throw new RuntimeException(s"Some entities do not exist: ${nonExistent.mkString(", ")}")
    }

    // loop through entity updates
    val entitiesToWrite = entityUpdates.flatMap { eud =>
      val docid = documentId(eud.entityType, eud.name)
      // what did we retrieve from the index?
      val getResponse = docs.getOrElse(docid, throw new Exception(s"should not reach this point: docid $docid had no response"))

      // is this a create or an update?
      val existingEntity = if (getResponse.isExists) {
        // update
        entity(docid, getResponse.getSource.asScala.toMap)
      } else {
        // create, start with a blank entity
        Entity(eud.name, eud.entityType, Map())
      }

      // rely heavily on existing code in applyOperationsToEntity() from AttributeSupport
      val newEntity = applyOperationsToEntity(existingEntity, eud.operations)
      if (newEntity != existingEntity) {
        Option(newEntity)
      } else {
        logger.info(s"$docid has no changes; skipping indexing for this document.")
        None
      }
    }

    if (entitiesToWrite.isEmpty) {
      logger.info("no changes at all; skipping request to OpenSearch.")
      Future(Seq())
    } else {
      stopwatch.start()
      val bulkResponse = indexEntities(entitiesToWrite.toList)
      stopwatch.stop()
      logQueryTiming(stopwatch.lastTaskTime(), s"batchUpsert bulk index (${entitiesToWrite.length} entities)")

      // for logging only
      val indexStatuses = bulkResponse.getItems.map(_.status().toString)
      val statusesByCount = indexStatuses.groupBy(identity).mapValues(_.length)
      statusesByCount foreach {
        case (status, count) =>
          logger.info(s"$status: $count")
      }

      // check for any errors
      if (bulkResponse.hasFailures) {
        // DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.", errorReports)))
        throw new RawlsExceptionWithErrorReport(ErrorReport(bulkResponse.buildFailureMessage()))
      } else {
        Future(entitiesToWrite)
      }
    }

  }

  // ================================================================================================
  // API methods we haven't implemented

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]] =
    throw new UnsupportedEntityOperationException("evaluateExpressions not supported by this provider.")

  override def expressionValidator: ExpressionValidator =
    throw new UnsupportedEntityOperationException("expressionValidator not supported by this provider.")

  // ================================================================================================
  // class-specific methods

  def createWorkspaceIndex(workspace: Workspace): String = {
    val aliasName = workspaceIndexAlias
    // does alias exist?
    // in production code, there exists a race condition where checking existence of the alias would fail during a swap.
    // that's not handled here in demo code.
    val getAliasRequest = new GetAliasesRequest(aliasName)
    val getAliasResponse = client.indices().existsAlias(getAliasRequest, RequestOptions.DEFAULT)
    if (!getAliasResponse) {
      logger.info(s"OpenSearch index alias $workspaceIndexAlias for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} does not exist; creating it and its underlying index ... ")
      createWorkspaceUnderlyingIndex(workspace, alias = Option(aliasName))
    } else {
      logger.info(s"OpenSearch index alias $workspaceIndexAlias for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} exists already; no action needed.")
    }
    aliasName
  }

  def createWorkspaceUnderlyingIndex(workspace: Workspace, alias: Option[String]): String = {
    // TODO: how to init the cluster with the index template in src/main/resources/opensearch?
    // that needs to go in before any index is created. We could do it when the middleware service
    // is booted; better to do it upon cluster creation.
    val underlyingIndexName = s"$workspaceIndexAlias.${System.currentTimeMillis()}"

    val getIndexRequest = new GetIndexRequest(underlyingIndexName)
    getIndexRequest.indicesOptions(new IndicesOptions(util.EnumSet.of(IndicesOptions.Option.IGNORE_ALIASES), util.EnumSet.of(WildcardStates.OPEN)))

    val getIndexResponse = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!getIndexResponse) {
      logger.info(s"OpenSearch underlying index $underlyingIndexName for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} does not exist; creating ... ")
      val createIndexRequest = new CreateIndexRequest(underlyingIndexName)
      alias.map(aliasName => createIndexRequest.alias(new Alias(aliasName)))

      // easiest way to create mappings
      val mappings =
        s"""
        {
            "properties": {
                "$TYPE_FIELD": {"type": "keyword"},
                "$NAME_FIELD": {"type": "keyword"}
            }
        }
        """
      createIndexRequest.mapping(mappings, XContentType.JSON)

      // TODO: explicit mappings for any other fields?

      val createIndexResponse = client.indices.create(createIndexRequest, RequestOptions.DEFAULT)
      logger.info(s"OpenSearch underlying index $underlyingIndexName for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} created: ${createIndexResponse.index()}")
    } else {
      logger.info(s"OpenSearch underlying index $underlyingIndexName for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} exists already; no action needed.")
    }
    underlyingIndexName
  }

  def removeWorkspaceUnderlyingIndex(indexName: String): Boolean = {
    val deleteIndexRequest = new DeleteIndexRequest(indexName)
    val deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT)
    deleteIndexResponse.isAcknowledged
  }

  def removeAlias(concreteIndexName: String): Boolean = {
    val deleteAliasRequest = new DeleteAliasRequest(concreteIndexName, workspaceIndexAlias)
    val deleteAliasResponse = client.indices().deleteAlias(deleteAliasRequest, RequestOptions.DEFAULT)
    timedQuery(s"delete alias from $concreteIndexName") {
      deleteAliasResponse.isAcknowledged
    }
  }

  def addAlias(concreteIndexName: String): Boolean = {
    val addAliasRequest = new IndicesAliasesRequest()
    val addAliasAction = new AliasActions(AliasActions.Type.ADD)
      .index(concreteIndexName)
      .alias(workspaceIndexAlias)
    addAliasRequest.addAliasAction(addAliasAction)
    val addAliasResponse = client.indices().updateAliases(addAliasRequest, RequestOptions.DEFAULT)
    addAliasResponse.isAcknowledged
  }

  def swapAlias(oldConcreteIndexName: String, newConcreteIndexName: String): Boolean= {
    removeAlias(oldConcreteIndexName) && addAlias(newConcreteIndexName)
  }


  def indexEntity(entity: Entity): IndexResponse = {
    // we could just call indexEntities(List(entity)),
    // but this code path for indexing one and only one
    // entity is optimized
    val request = indexableDocument(entity)
    client.index(request, RequestOptions.DEFAULT)
  }

  def indexEntities(entities: List[Entity]): BulkResponse = {
    val bulkRequest = new BulkRequest()
    entities.foreach { entity =>
      val indexRequest = indexableDocument(entity)
      bulkRequest.add(indexRequest)
    }

    val stopWatch = new StopWatch()
    stopWatch.start()
    val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)
    if (bulkResponse.hasFailures)
      logger.info(s"failure message: ${bulkResponse.buildFailureMessage()}")
    stopWatch.stop()
    val stopWatchTook = stopWatch.lastTaskTime()

    logQueryTiming(bulkResponse.getTook, s"bulk index of ${entities.length} documents (measured by client internal clock)")
    logQueryTiming(stopWatchTook, s"bulk index of ${entities.length} documents (measured by stopwatch)")

    bulkResponse
  }

  def reindexWorkspace(): Unit = {
    val stopWatch = new StopWatch()
    stopWatch.start()

    //      1. find name of concrete index underneath the current alias
    val getIndexRequest = new GetIndexRequest(workspaceIndexAlias)
    val getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT)
    assert(getIndexResponse.getIndices.length == 1, s"found ${getIndexResponse.getIndices.length} index names in get-alias response")
    val oldUnderlyingIndexName = getIndexResponse.getIndices.head
//    val getAliasRequest = new GetAliasesRequest(workspaceIndexAlias)
//    val getAliasResponse = client.indices().getAlias(getAliasRequest, RequestOptions.DEFAULT)
//    val aliasMetadataSet = getAliasResponse.getAliases.get(workspaceIndexAlias)
//    assert(aliasMetadataSet.size() == 1)
//    val aliasMetadata = aliasMetadataSet.asScala.head
//    val oldUnderlyingIndexName = aliasMetadata.getSearchRouting // should be the same as indexRouting
    logger.info(s"current underlying concrete index is $oldUnderlyingIndexName")

    //      2. create new concrete index
    val newUnderlyingIndexName = createWorkspaceUnderlyingIndex(requestArguments.workspace, None) // no alias yet
    logger.info(s"new target underlying concrete index is $oldUnderlyingIndexName")

    //      3. reindex from current to new
    val reindexRequest = new ReindexRequest()
    reindexRequest.setSourceIndices(oldUnderlyingIndexName)
    reindexRequest.setDestIndex(newUnderlyingIndexName)
    // reindexRequest.setDestVersionType(VersionType.EXTERNAL) // only if we care to preserve document version numbers
    val reindexResponse = client.reindex(reindexRequest, RequestOptions.DEFAULT)

    //      4. swap alias to new index
    swapAlias(oldUnderlyingIndexName, newUnderlyingIndexName)

    //      5. delete old index
    removeWorkspaceUnderlyingIndex(oldUnderlyingIndexName)

    stopWatch.stop()
    val took = stopWatch.lastTaskTime()

    logQueryTiming(took, s"the entire getUnderlying-createUnderlying-reindex-swapAlias-deleteUnderlying cycle (measured via stopwatch)")
  }

  // unused:
  /*
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
        .field(s"$fld.raw")
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

  def purgeMappings(workspace: Workspace): Unit = {
    // retrieve mappings for this workspace's index
    val mappingsRequest = new GetMappingsRequest()
    mappingsRequest.indices(workspaceIndex)
    val mappingsResponse = client.indices().getMapping(mappingsRequest, RequestOptions.DEFAULT)

    // for each type:
      // perform aggregation query, including an agg for each mapping field. only need size=1 for each aggregation.
      // any aggregation that returned a doc count means this field is populated.
  }
  */

  // convenience/sugar
  def timedQuery[T](extra: String)(queryCode: => T): T = timedQuery[T](Option(extra))(queryCode)

  def timedQuery[T](extra: Option[String] = None)(queryCode: => T): T = {
    val stopwatch = new StopWatch()
    stopwatch.start()

    val response: T = queryCode

    stopwatch.stop()
    val took = stopwatch.lastTaskTime()

    val msg = extra match {
      case None => ""
      case Some(s) => s": $s"
    }
    logger.info(s"OpenSearch query completed in ${took.toString}$msg")

    response
  }

  def logQueryTiming(timeValue: TimeValue, extra: String): Unit = logQueryTiming(timeValue, Option(extra))

  def logQueryTiming(timeValue: TimeValue, extra: Option[String] = None): Unit = {
    val msg = extra match {
      case None => ""
      case Some(s) => s": $s"
    }
    logger.info(s"OpenSearch query completed in ${timeValue.toString}$msg")
  }



}
