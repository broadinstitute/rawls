package org.broadinstitute.dsde.rawls.entities.local

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.{AttributeValue => OpenCensusAttributeValue}
import io.opentelemetry.api.common.AttributeKey
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  DataAccess,
  EntityAndAttributesResult,
  EntityRecord,
  ReadWriteAction
}
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.{EntityRequestArguments, EntityStreamingUtils}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{EntityName, LookupExpression}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionEvaluationSupport, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException, DeleteEntitiesOfTypeConflictException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeName,
  AttributeValue,
  Entity,
  EntityCopyDefinition,
  EntityCopyResponse,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  ErrorReport,
  RawlsRequestContext,
  SamResourceTypeNames,
  SamWorkspaceActions,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue,
  Workspace,
  WorkspaceAttributeSpecs
}
import org.broadinstitute.dsde.rawls.util.TracingUtils._
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, CollectionUtils, EntitySupport}
import slick.jdbc.{ResultSetConcurrency, ResultSetType, TransactionIsolation}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class LocalEntityProvider(requestArguments: EntityRequestArguments,
                          implicit protected val dataSource: SlickDataSource,
                          cacheEnabled: Boolean,
                          queryTimeout: Duration,
                          override val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends EntityProvider
    with LazyLogging
    with EntitySupport
    with AttributeSupport
    with ExpressionEvaluationSupport
    with EntityStatisticsCacheSupport {

  import dataSource.dataAccess.driver.api._

  override val entityStoreId: Option[String] = None

  override val workspaceContext = requestArguments.workspace

  final private val queryTimeoutSeconds: Int = queryTimeout.getSeconds.toInt

  override def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]] =
    // start performance tracing
    traceFutureWithParent("LocalEntityProvider.entityTypeMetadata", requestArguments.ctx) { localContext =>
      setTraceSpanAttribute(localContext, AttributeKey.stringKey("workspace"), workspaceContext.toWorkspaceName.toString)
      setTraceSpanAttribute(localContext, AttributeKey.booleanKey("useCache"), java.lang.Boolean.valueOf(useCache))
      setTraceSpanAttribute(localContext, AttributeKey.booleanKey("cacheEnabled"), java.lang.Boolean.valueOf(cacheEnabled))

      // start transaction
      dataSource.inTransaction(
        dataAccess =>
          if (!useCache || !cacheEnabled) {
            if (!cacheEnabled) {
              logger.info(
                s"entity statistics cache: miss (cache disabled at system level) [${workspaceContext.workspaceIdAsUUID}]"
              )
            } else if (!useCache) {
              logger.info(
                s"entity statistics cache: miss (user request specified cache bypass) [${workspaceContext.workspaceIdAsUUID}]"
              )
            }
            // retrieve metadata, bypassing cache
            calculateMetadataResponse(dataAccess, countsFromCache = false, attributesFromCache = false, localContext)
          } else {
            // system and request both have cache enabled. Check for existence and staleness of cache
            cacheStaleness(dataAccess, localContext).flatMap {
              case None =>
                // cache does not exist - return uncached
                logger.info(
                  s"entity statistics cache: miss (cache does not exist) [${workspaceContext.workspaceIdAsUUID}]"
                )
                calculateMetadataResponse(dataAccess,
                                          countsFromCache = false,
                                          attributesFromCache = false,
                                          localContext
                )
              case Some(0) =>
                // cache is up to date - return cached
                logger.info(s"entity statistics cache: hit [${workspaceContext.workspaceIdAsUUID}]")
                calculateMetadataResponse(dataAccess, countsFromCache = true, attributesFromCache = true, localContext)
              case Some(stalenessSeconds) =>
                // cache exists, but is out of date - check if this workspace has any always-cache feature flags set
                cacheFeatureFlags(dataAccess, localContext).flatMap { flags =>
                  if (flags.alwaysCacheTypeCounts || flags.alwaysCacheAttributes) {
                    setTraceSpanAttribute(localContext,
                                          AttributeKey.booleanKey("alwaysCacheTypeCountsFeatureFlag"),
                                          java.lang.Boolean.valueOf(flags.alwaysCacheTypeCounts)
                    )
                    setTraceSpanAttribute(localContext,
                                          AttributeKey.booleanKey("alwaysCacheAttributesFeatureFlag"),
                                          java.lang.Boolean.valueOf(flags.alwaysCacheAttributes)
                    )
                    logger.info(
                      s"entity statistics cache: partial hit (alwaysCacheTypeCounts=${flags.alwaysCacheTypeCounts}, alwaysCacheAttributes=${flags.alwaysCacheAttributes}, staleness=$stalenessSeconds) [${workspaceContext.workspaceIdAsUUID}]"
                    )
                  } else {
                    logger.info(
                      s"entity statistics cache: miss (cache is out of date, staleness=$stalenessSeconds) [${workspaceContext.workspaceIdAsUUID}]"
                    )
                    // and opportunistically save
                  }
                  calculateMetadataResponse(dataAccess,
                                            countsFromCache = flags.alwaysCacheTypeCounts,
                                            attributesFromCache = flags.alwaysCacheAttributes,
                                            localContext
                  )
                } // end feature-flags lookup
            } // end staleness lookup
          } // end if useCache/cacheEnabled check
        ,
        TransactionIsolation.ReadCommitted
      ) // end transaction
    } // end root trace

  override def createEntity(entity: Entity): Future[Entity] =
    dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Entity)) { dataAccess =>
      dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
        case Some(_) =>
          DBIO.failed(
            new RawlsExceptionWithErrorReport(
              errorReport =
                ErrorReport(StatusCodes.Conflict,
                            s"${entity.entityType} ${entity.name} already exists in ${workspaceContext.toWorkspaceName}"
                )
            )
          )
        case None => dataAccess.entityQuery.save(workspaceContext, entity)
      }
    }

  // EntityApiServiceSpec has good test coverage for this api
  override def deleteEntities(entRefs: Seq[AttributeEntityReference]): Future[Int] =
    dataSource.inTransaction { dataAccess =>
      // withAllEntityRefs throws exception if some entities not found; passes through if all ok
      traceDBIOWithParent("LocalEntityProvider.deleteEntities", requestArguments.ctx) { localContext =>
        setTraceSpanAttribute(localContext,
                              AttributeKey.stringKey("workspaceId"),
                              workspaceContext.workspaceId
        )
        setTraceSpanAttribute(localContext,
                              AttributeKey.longKey("numEntities"),
                              java.lang.Long.valueOf(entRefs.length)
        )
        withAllEntityRefs(workspaceContext, dataAccess, entRefs, localContext) { _ =>
          traceDBIOWithParent("entityQuery.getAllReferringEntities", localContext)(innerSpan =>
            dataAccess.entityQuery.getAllReferringEntities(workspaceContext, entRefs.toSet) flatMap {
              referringEntities =>
                if (referringEntities != entRefs.toSet)
                  throw new DeleteEntitiesConflictException(referringEntities)
                else {
                  traceDBIOWithParent("entityQuery.hide", innerSpan)(_ =>
                    dataAccess.entityQuery
                      .hide(workspaceContext, entRefs)
                      .withStatementParameters(statementInit = _.setQueryTimeout(queryTimeoutSeconds))
                  )
                }
            }
          )
        }
      }
    }

  override def deleteEntitiesOfType(entityType: String): Future[Int] =
    dataSource.inTransaction { dataAccess =>
      traceDBIOWithParent("LocalEntityProvider.deleteEntitiesOfType", requestArguments.ctx) { localContext =>
        setTraceSpanAttribute(localContext,
                              AttributeKey.stringKey("workspaceId"),
                              workspaceContext.workspaceId
        )
        setTraceSpanAttribute(localContext,
                              AttributeKey.stringKey("entityType"),
                              entityType
        )

        dataAccess.entityQuery.countReferringEntitiesForType(workspaceContext, entityType) flatMap {
          referringEntitiesCount =>
            if (referringEntitiesCount != 0)
              throw new DeleteEntitiesOfTypeConflictException(referringEntitiesCount)
            else {
              dataAccess.entityQuery
                .hideType(workspaceContext, entityType)
                .withStatementParameters(statementInit = _.setQueryTimeout(queryTimeoutSeconds))
            }
        }
      }
    }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[Stream[SubmissionValidationEntityInputs]] =
    dataSource.inTransaction { dataAccess =>
      withEntityRecsForExpressionEval(expressionEvaluationContext, workspaceContext, dataAccess) { jobEntityRecs =>
        // Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
        evaluateExpressionsInternal(workspaceContext,
                                    gatherInputsResult.processableInputs,
                                    jobEntityRecs,
                                    dataAccess
        ) map { valuesByEntity =>
          createSubmissionValidationEntityInputs(valuesByEntity)
        }
      }
    }

  override def expressionValidator: ExpressionValidator = new LocalEntityExpressionValidator

  protected[local] def evaluateExpressionsInternal(workspaceContext: Workspace,
                                                   inputs: Set[MethodInput],
                                                   entities: Option[Seq[EntityRecord]],
                                                   dataAccess: DataAccess
  )(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    val entityNames = entities match {
      case Some(recs) => recs.map(_.name)
      case None       => Seq("")
    }

    if (inputs.isEmpty) {
      // no inputs to evaluate = just return an empty map back!
      DBIO.successful(entityNames.map(_ -> Seq.empty[SubmissionValidationValue]).toMap)
    } else {
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        // Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.toSeq.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression, Option(input)).asTry.map {
            tryAttribsByEntity =>
              val validationValuesByEntity: Seq[(EntityName, SubmissionValidationValue)] = tryAttribsByEntity match {
                case Failure(regret) =>
                  // The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                  entityNames
                    .map((_, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)))
                case Success(attributeMap) =>
                  convertToSubmissionValidationValues(attributeMap, input)
              }
              validationValuesByEntity
          }
        }

        // Flip the list of DBIO monads into one on the outside that we can map across and then group by entity.
        DBIO.sequence(resultsByInput) map { results =>
          CollectionUtils.groupByTuples(results.flatten)
        }
      }
    }
  }

  override def getEntity(entityType: String, entityName: String): Future[Entity] =
    dataSource.inTransaction { dataAccess =>
      withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
        DBIO.successful(entity)
      }
    }

  /**
    * Returns the components needed to stream a EntityQueryResponse to an end user in response to the entityQuery API.
    * This method returns fully materialized metadata (row counts, page size, etc) as EntityQueryResultMetadata, and
    * also returns a streaming Source of Entity objects. We avoid materializing the full set of Entity objects for
    * performance and memory reasons.
    *
    * @param entityType the type of entities to return in the result set
    * @param query criteria for filtering and paginating the result set
    * @param parentContext tracing context into which this method will add traces
    * @return a tuple of 1) the fully materialized metadata, and 2) a streaming Source of Entity objects
    */
  override def queryEntitiesSource(entityType: String,
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = {
    // look for a columnFilter that specifies the primary key for this entityType;
    // such a columnFilter means we are filtering by name and can greatly simplify the underlying query.
    val nameFilter: Option[String] = query.columnFilter match {
      case Some(colFilter)
          if colFilter.attributeName == AttributeName.withDefaultNS(
            entityType + Attributable.entityIdAttributeSuffix
          ) =>
        Option(colFilter.term)
      case _ => None
    }

    setTraceSpanAttribute(parentContext, AttributeKey.booleanKey("isFilterByName"), java.lang.Boolean.valueOf(nameFilter.isDefined))
    // if filtering by name, retrieve that entity directly, else do the full query:
    nameFilter match {
      case Some(entityName) =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.entityQuery.loadSingleEntityForPage(workspaceContext, entityType, entityName, query)
        } map { case (unfilteredCount, filteredCount, entity) =>
          val pageCount = if (entity.nonEmpty) 1 else 0
          val entitySource = if (entity.nonEmpty) Source.single(entity.head) else Source.empty

          val metadata: EntityQueryResultMetadata =
            EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount)
          (metadata, entitySource)
        }

      case _ =>
        traceFutureWithParent("loadEntityPage", parentContext) { childContext =>
          setTraceSpanAttribute(childContext, AttributeKey.longKey("pageSize"), java.lang.Long.valueOf(query.pageSize))
          setTraceSpanAttribute(childContext, AttributeKey.longKey("page"), java.lang.Long.valueOf(query.page))
          setTraceSpanAttribute(childContext, AttributeKey.stringKey("filterTerms"), query.filterTerms.getOrElse(""))
          setTraceSpanAttribute(childContext, AttributeKey.stringKey("sortField"), query.sortField)
          setTraceSpanAttribute(childContext, AttributeKey.stringKey("sortDirection"), query.sortDirection.toString)

          // query for the fully-materialized metadata and for the streaming result set, return the two of these
          // as a tuple
          for {
            metadata <- queryForMetadata(entityType, query, childContext)
            entitySource = queryForResultSource(entityType, query, childContext)
          } yield (metadata, entitySource)
        }
    }
  }

  /**
    * generates the metadata (pagination, etc) or the incoming query.
    * Called by queryEntitiesSource.
    *
    * @param entityType the type of entities to query for metadata
    * @param query criteria for filtering and paginating the result set
    * @param parentContext tracing context into which this method will add traces
    * @return the query result metadata
    */
  private def queryForMetadata(entityType: String,
                               query: EntityQuery,
                               parentContext: RawlsRequestContext
  ): Future[EntityQueryResultMetadata] =
    dataSource.inTransaction { dataAccess =>
      for {
        (unfilteredCount, filteredCount) <- dataAccess.entityQuery.loadEntityPageCounts(workspaceContext,
                                                                                        entityType,
                                                                                        query,
                                                                                        parentContext
        )
      } yield {
        val pageCount: Int = Math.ceil(filteredCount.toFloat / query.pageSize).toInt
        if (filteredCount > 0 && query.page > pageCount) {
          throw new DataEntityException(
            code = StatusCodes.BadRequest,
            message = s"requested page ${query.page} is greater than the number of pages $pageCount"
          )
        }

        EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount)
      }
    }

  /**
   * generates a Source[Entity, _] representing the individual Entity results for the incoming query.
    * Called by queryEntitiesSource.
   *
   * @param entityType the type of entities to return in the result set
   * @param query criteria for filtering and paginating the result set
   * @param parentContext tracing context into which this method will add traces
   * @return a streaming Source of the query results
   */
  private def queryForResultSource(entityType: String,
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Source[Entity, _] = {
    val allAttrsStream =
      dataSource.dataAccess.entityQuery
        .loadEntityPageSource(workspaceContext, entityType, query, parentContext)
        .transactionally
        .withTransactionIsolation(TransactionIsolation.ReadCommitted)
        .withStatementParameters(rsType = ResultSetType.ForwardOnly,
                                 rsConcurrency = ResultSetConcurrency.ReadOnly,
                                 fetchSize = dataSource.dataAccess.fetchSize
        )
    val dbSource: Source[EntityAndAttributesResult, NotUsed] =
      Source.fromPublisher(dataSource.database.stream(allAttrsStream))

    EntityStreamingUtils.gatherEntities(dataSource, dbSource)
  }

  /* as of this writing, only used in tests. This queryEntities method materializes the entire result set of
   *  entities and is therefore memory-hungry. Runtime code should not use this and should call queryEntitiesSource
   *  instead. This method is still useful for testing and is called by multiple tests.
   * */
  @deprecated("use queryEntitiesSource instead.", "2024-01-09")
  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[EntityQueryResponse] =
    queryEntitiesSource(entityType, query, parentContext) flatMap { case (entityQueryResultMetadata, entitySource) =>
      implicit val actorSystem: ActorSystem = ActorSystem()
      entitySource.runWith(Sink.seq) map { entities =>
        EntityQueryResponse(query, entityQueryResultMetadata, entities)
      }
    }

  def batchUpdateEntitiesImpl(entityUpdates: Seq[EntityUpdateDefinition],
                              upsert: Boolean
  ): Future[Traversable[Entity]] = {
    val namesToCheck = for {
      update <- entityUpdates
      operation <- update.operations
    } yield operation.name

    traceFutureWithParent("LocalEntityProvider.batchUpdateEntitiesImpl", requestArguments.ctx) { localContext =>
      setTraceSpanAttribute(localContext,
                            AttributeKey.stringKey("workspaceId"),
                            workspaceContext.workspaceId
      )
      setTraceSpanAttribute(localContext,
                              AttributeKey.booleanKey("upsert"),
                              java.lang.Boolean.valueOf(upsert)
      )
      setTraceSpanAttribute(localContext,
                              AttributeKey.longKey("entityUpdatesCount"),
                              java.lang.Long.valueOf(entityUpdates.length)
      )
      setTraceSpanAttribute(localContext,
                              AttributeKey.longKey("entityOperationsCount"),
                              java.lang.Long.valueOf(entityUpdates.map(_.operations.length).sum)
      )

      withAttributeNamespaceCheck(namesToCheck) {
        dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Entity)) { dataAccess =>
          val updateTrialsAction = traceDBIOWithParent("getActiveEntities", localContext)(_ =>
            dataAccess.entityQuery.getActiveEntities(
              workspaceContext,
              entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name))
            )
          ) map { entities =>
            val entitiesByName = entities.map(e => (e.entityType, e.name) -> e).toMap
            entityUpdates.map { entityUpdate =>
              entityUpdate -> (entitiesByName.get((entityUpdate.entityType, entityUpdate.name)) match {
                case Some(e) =>
                  Try(applyOperationsToEntity(e, entityUpdate.operations))
                case None =>
                  if (upsert) {
                    Try(
                      applyOperationsToEntity(Entity(entityUpdate.name, entityUpdate.entityType, Map.empty),
                                              entityUpdate.operations
                      )
                    )
                  } else {
                    Failure(new RuntimeException("Entity does not exist"))
                  }
              })
            }
          }

          val saveAction = updateTrialsAction flatMap { updateTrials =>
            val errorReports = updateTrials.collect { case (entityUpdate, Failure(regrets)) =>
              ErrorReport(s"Could not update ${entityUpdate.entityType} ${entityUpdate.name}", ErrorReport(regrets))
            }
            if (errorReports.nonEmpty) {
              DBIO.failed(
                new RawlsExceptionWithErrorReport(
                  ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.", errorReports)
                )
              )
            } else {
              val t = updateTrials.collect { case (entityUpdate, Success(entity)) => entity }

              dataAccess.entityQuery
                .save(workspaceContext, t)
                .withStatementParameters(statementInit = _.setQueryTimeout(queryTimeoutSeconds))
            }
          }

          traceDBIOWithParent("saveAction", localContext)(_ => saveAction)
        } recover {
          case icve: java.sql.SQLIntegrityConstraintViolationException =>
            val userMessage =
              s"Database error occurred. Check if you are uploading entity names that differ only in case " +
                s"from pre-existing entities."
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, userMessage, icve))
          case bue: java.sql.BatchUpdateException =>
            val maybeCaseIssue = bue.getMessage.startsWith("Duplicate entry")
            val userMessage = if (maybeCaseIssue) {
              s"Database error occurred. Check if you are uploading entity names that differ only in case " +
                s"from pre-existing entities."
            } else {
              s"Database error occurred. Underlying error message: ${bue.getMessage}"
            }
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, userMessage, bue))
        }
      }
    }
  }

  override def batchUpdateEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] =
    batchUpdateEntitiesImpl(entityUpdates, upsert = false)

  override def batchUpsertEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] =
    batchUpdateEntitiesImpl(entityUpdates, upsert = true)

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] =
    traceFutureWithParent("checkAndCopyEntities", parentContext)(_ =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext,
                                                    destWorkspaceContext,
                                                    entityType,
                                                    entityNames,
                                                    linkExistingEntities,
                                                    parentContext
        )
      }
    )
}
