package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.trace
import io.opencensus.trace.{Span, AttributeValue => OpenCensusAttributeValue}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{EntityName, LookupExpression}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionEvaluationSupport, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException, DeleteEntitiesOfTypeConflictException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeValue, Entity, EntityQuery, EntityQueryResponse, EntityQueryResultMetadata, EntityTypeMetadata, ErrorReport, SubmissionValidationEntityInputs, SubmissionValidationValue, Workspace}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.{traceDBIO, traceDBIOWithParent}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, CollectionUtils, EntitySupport}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class LocalEntityProvider(workspace: Workspace, implicit protected val dataSource: SlickDataSource, cacheEnabled: Boolean)
                         (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with LazyLogging
    with EntitySupport with AttributeSupport with ExpressionEvaluationSupport with EntityStatisticsCacheSupport {

  import dataSource.dataAccess.driver.api._

  override val entityStoreId: Option[String] = None

  override val workspaceContext = workspace

  override def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]] = {
    // start performance tracing
    trace("LocalEntityProvider.entityTypeMetadata") { rootSpan =>
      rootSpan.putAttribute("workspace", OpenCensusAttributeValue.stringAttributeValue(workspace.toWorkspaceName.toString))
      rootSpan.putAttribute("useCache", OpenCensusAttributeValue.booleanAttributeValue(useCache))
      rootSpan.putAttribute("cacheEnabled", OpenCensusAttributeValue.booleanAttributeValue(cacheEnabled))
      // start transaction
      dataSource.inTransaction { dataAccess =>
        if (!useCache || !cacheEnabled) {
          if (!cacheEnabled) {
            logger.info(s"entity statistics cache: miss (cache disabled at system level) [${workspaceContext.workspaceIdAsUUID}]")
          } else if (!useCache) {
            logger.info(s"entity statistics cache: miss (user request specified cache bypass) [${workspaceContext.workspaceIdAsUUID}]")
          }
          // retrieve metadata, bypassing cache
          calculateMetadataResponse(dataAccess, countsFromCache = false, attributesFromCache = false, rootSpan)
        } else {
          // system and request both have cache enabled. Check for existence and staleness of cache
          cacheStaleness(dataAccess, rootSpan).flatMap {
            case None =>
              // cache does not exist - return uncached
              logger.info(s"entity statistics cache: miss (cache does not exist) [${workspaceContext.workspaceIdAsUUID}]")
              calculateMetadataResponse(dataAccess, countsFromCache = false, attributesFromCache = false, rootSpan)
            case Some(0) =>
              // cache is up to date - return cached
              logger.info(s"entity statistics cache: hit [${workspaceContext.workspaceIdAsUUID}]")
              calculateMetadataResponse(dataAccess, countsFromCache = true, attributesFromCache = true, rootSpan)
            case Some(stalenessSeconds) =>
              // cache exists, but is out of date - check if this workspace has any always-cache feature flags set
              cacheFeatureFlags(dataAccess, rootSpan).flatMap { flags =>
                if (flags.alwaysCacheTypeCounts || flags.alwaysCacheAttributes) {
                  rootSpan.putAttribute("alwaysCacheTypeCountsFeatureFlag", OpenCensusAttributeValue.booleanAttributeValue(flags.alwaysCacheTypeCounts))
                  rootSpan.putAttribute("alwaysCacheAttributesFeatureFlag", OpenCensusAttributeValue.booleanAttributeValue(flags.alwaysCacheAttributes))
                  logger.info(s"entity statistics cache: partial hit (alwaysCacheTypeCounts=${flags.alwaysCacheTypeCounts}, alwaysCacheAttributes=${flags.alwaysCacheAttributes}, staleness=$stalenessSeconds) [${workspaceContext.workspaceIdAsUUID}]")
                } else {
                  logger.info(s"entity statistics cache: miss (cache is out of date, staleness=$stalenessSeconds) [${workspaceContext.workspaceIdAsUUID}]")
                  // and opportunistically save
                }
                calculateMetadataResponse(dataAccess, countsFromCache = flags.alwaysCacheTypeCounts, attributesFromCache = flags.alwaysCacheAttributes, rootSpan)
              } // end feature-flags lookup
          } // end staleness lookup
        } // end if useCache/cacheEnabled check
      } // end transaction
    } // end root trace
  }

  override def createEntity(entity: Entity): Future[Entity] = {
    dataSource.inTransactionWithAttrTempTable (Set(AttributeTempTableType.Entity)) { dataAccess =>
      dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspace.toWorkspaceName}")))
        case None => dataAccess.entityQuery.save(workspaceContext, entity)
      }
    }
  }

  // EntityApiServiceSpec has good test coverage for this api
  override def deleteEntities(entRefs: Seq[AttributeEntityReference]): Future[Int] = {
    dataSource.inTransaction { dataAccess =>
      // withAllEntityRefs throws exception if some entities not found; passes through if all ok
      traceDBIO("LocalEntityProvider.deleteEntities") { rootSpan =>
        rootSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceContext.workspaceId))
        rootSpan.putAttribute("numEntities", OpenCensusAttributeValue.longAttributeValue(entRefs.length))
        withAllEntityRefs(workspaceContext, dataAccess, entRefs, rootSpan) { _ =>
          traceDBIOWithParent("entityQuery.getAllReferringEntities", rootSpan)(innerSpan => dataAccess.entityQuery.getAllReferringEntities(workspaceContext, entRefs.toSet) flatMap { referringEntities =>
            if (referringEntities != entRefs.toSet)
              throw new DeleteEntitiesConflictException(referringEntities)
            else {
              traceDBIOWithParent("entityQuery.hide", innerSpan)(_ => dataAccess.entityQuery.hide(workspaceContext, entRefs))
            }
          })
        }
      }
    }
  }

  override def deleteEntitiesOfType(entityType: String): Future[Int] = {
    dataSource.inTransaction { dataAccess =>
      // withAllEntityRefs throws exception if some entities not found; passes through if all ok
      traceDBIO("LocalEntityProvider.deleteEntitiesOfType") { rootSpan =>
        rootSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceContext.workspaceId))
        rootSpan.putAttribute("entityType", OpenCensusAttributeValue.stringAttributeValue(entityType))

        dataAccess.entityQuery.getActiveIdsForType(workspaceContext.workspaceIdAsUUID, entityType) flatMap { idsForType =>
          println(idsForType)
          dataAccess.entityQuery.countReferringEntities(workspace, idsForType.keys.toSet) flatMap { referringEntities =>
            println(s"Deleting ${idsForType.size} ${entityType}s")
            println(referringEntities)
            if (referringEntities != 0)
              throw new DeleteEntitiesOfTypeConflictException(referringEntities)
            else {
              dataAccess.entityQuery.hide(workspaceContext, idsForType.values.toSeq)
            }
          }
        }
      }
    }
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]] = {
    dataSource.inTransaction { dataAccess =>
      withEntityRecsForExpressionEval(expressionEvaluationContext, workspace, dataAccess) { jobEntityRecs =>
        //Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
        evaluateExpressionsInternal(workspace, gatherInputsResult.processableInputs, jobEntityRecs, dataAccess) map { valuesByEntity =>
          createSubmissionValidationEntityInputs(valuesByEntity)
        }
      }
    }
  }

  override def expressionValidator: ExpressionValidator = new LocalEntityExpressionValidator

  protected[local] def evaluateExpressionsInternal(workspaceContext: Workspace, inputs: Set[MethodInput], entities: Option[Seq[EntityRecord]], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    import dataAccess.driver.api._

    val entityNames = entities match {
      case Some(recs) => recs.map(_.name)
      case None => Seq("")
    }

    if( inputs.isEmpty ) {
      //no inputs to evaluate = just return an empty map back!
      DBIO.successful(entityNames.map( _ -> Seq.empty[SubmissionValidationValue] ).toMap)
    } else {
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        //Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.toSeq.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression, Option(input)).asTry.map { tryAttribsByEntity =>
            val validationValuesByEntity: Seq[(EntityName, SubmissionValidationValue)] = tryAttribsByEntity match {
              case Failure(regret) =>
                //The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                entityNames.map((_, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)))
              case Success(attributeMap) =>
                convertToSubmissionValidationValues(attributeMap, input)
            }
            validationValuesByEntity
          }
        }

        //Flip the list of DBIO monads into one on the outside that we can map across and then group by entity.
        DBIO.sequence(resultsByInput) map { results =>
          CollectionUtils.groupByTuples(results.flatten)
        }
      }
    }
  }

  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    dataSource.inTransaction { dataAccess =>
      withEntity(workspaceContext, entityType, entityName, dataAccess) {
        entity => DBIO.successful(entity)
      }
    }
  }

  override def queryEntities(entityType: String, query: EntityQuery, parentSpan: Span = null): Future[EntityQueryResponse] = {
    dataSource.inTransaction { dataAccess =>
      traceDBIOWithParent("loadEntityPage", parentSpan) { s1 =>
        s1.putAttribute("pageSize", OpenCensusAttributeValue.longAttributeValue(query.pageSize))
        s1.putAttribute("page", OpenCensusAttributeValue.longAttributeValue(query.page))
        s1.putAttribute("filterTerms", OpenCensusAttributeValue.stringAttributeValue(query.filterTerms.getOrElse("")))
        s1.putAttribute("sortField", OpenCensusAttributeValue.stringAttributeValue(query.sortField))
        s1.putAttribute("sortDirection", OpenCensusAttributeValue.stringAttributeValue(query.sortDirection.toString))

        dataAccess.entityQuery.loadEntityPage(workspaceContext, entityType, query, s1)
      } map { case (unfilteredCount, filteredCount, entities) =>
        createEntityQueryResponse(query, unfilteredCount, filteredCount, entities.toSeq)
      }
    }
  }

  def createEntityQueryResponse(query: EntityQuery, unfilteredCount: Int, filteredCount: Int, page: Seq[Entity]): EntityQueryResponse = {
    val pageCount = Math.ceil(filteredCount.toFloat / query.pageSize).toInt
    if (filteredCount > 0 && query.page > pageCount) {
      throw new DataEntityException(code = StatusCodes.BadRequest, message = s"requested page ${query.page} is greater than the number of pages $pageCount")
    } else {
      EntityQueryResponse(query, EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount), page)
    }
  }

  def batchUpdateEntitiesImpl(entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean): Future[Traversable[Entity]] = {
    val namesToCheck = for {
      update <- entityUpdates
      operation <- update.operations
    } yield operation.name

    trace("LocalEntityProvider.batchUpdateEntitiesImpl") { rootSpan =>
      rootSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceContext.workspaceId))
      rootSpan.putAttribute("isUpsert", OpenCensusAttributeValue.booleanAttributeValue(upsert))
      rootSpan.putAttribute("entityUpdatesCount", OpenCensusAttributeValue.longAttributeValue(entityUpdates.length))
      rootSpan.putAttribute("entityOperationsCount", OpenCensusAttributeValue.longAttributeValue(entityUpdates.map(_.operations.length).sum))

      withAttributeNamespaceCheck(namesToCheck) {
        dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Entity)) { dataAccess =>
          val updateTrialsAction = traceDBIOWithParent("getActiveEntities", rootSpan)(_ => dataAccess.entityQuery.getActiveEntities(workspaceContext, entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name)))) map { entities =>
            val entitiesByName = entities.map(e => (e.entityType, e.name) -> e).toMap
            entityUpdates.map { entityUpdate =>
              entityUpdate -> (entitiesByName.get((entityUpdate.entityType, entityUpdate.name)) match {
                case Some(e) =>
                  Try(applyOperationsToEntity(e, entityUpdate.operations))
                case None =>
                  if (upsert) {
                    Try(applyOperationsToEntity(Entity(entityUpdate.name, entityUpdate.entityType, Map.empty), entityUpdate.operations))
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
              DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.", errorReports)))
            } else {
              val t = updateTrials.collect { case (entityUpdate, Success(entity)) => entity }

              dataAccess.entityQuery.save(workspaceContext, t)
            }
          }

          traceDBIOWithParent("saveAction", rootSpan)(_ => saveAction)
        } recover {
          case icve:java.sql.SQLIntegrityConstraintViolationException =>
            val userMessage =
              s"Database error occurred. Check if you are uploading entity names or entity types that differ only in case " +
                s"from pre-existing entities."
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, userMessage, icve))
          case bue:java.sql.BatchUpdateException =>
            val maybeCaseIssue = bue.getMessage.startsWith("Duplicate entry")
            val userMessage = if (maybeCaseIssue) {
              s"Database error occurred. Check if you are uploading entity names or entity types that differ only in case " +
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

}
