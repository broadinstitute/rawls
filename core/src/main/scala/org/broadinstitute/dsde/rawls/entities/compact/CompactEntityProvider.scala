package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, EntityTypeAndCount, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  EntityNotFoundException,
  EntityReferenceNotFoundException
}
import org.broadinstitute.dsde.rawls.entities.{EntityRequestArguments, EntityStreamingUtils, EntityUtils}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeRename,
  AttributeUpdateOperations,
  AttributeValue,
  Entity,
  EntityCopyResponse,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  EntityTypeRename,
  ErrorReport,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import slick.dbio.{DBIO, Effect}
import slick.jdbc.ResultSetConcurrency.ReadOnly
import slick.jdbc.{ResultSetConcurrency, ResultSetType}
import slick.jdbc.TransactionIsolation.ReadCommitted
import slick.sql.SqlStreamingAction

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Implementation logic for compact data tables. Compact data tables store all an entity's attributes in a
  * single JSON packet within the database. Compare this to the implementation in LocalEntityProvider, which
  * requires at least one row per attribute.
  *
  * @param executionContext scala concurrency context
  */
class CompactEntityProvider(requestArguments: EntityRequestArguments, repository: CompactEntityRepository)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProvider
    with LazyLogging {
  override def entityStoreId: Option[String] = None // unused

  val workspaceId: UUID = requestArguments.workspace.workspaceIdAsUUID // shorthand for methods below
  val workspaceContext = requestArguments.workspace

  override def batchUpdateEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition],
    parentContext: RawlsRequestContext
  ): Future[Traversable[Entity]] = ???

  override def batchUpsertEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition],
    parentContext: RawlsRequestContext
  ): Future[Traversable[Entity]] = ???

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] = ???

  override def createEntity(entity: Entity, parentContext: RawlsRequestContext): Future[Entity] = {
    EntityUtils.validateEntity(entity)
    val createFuture = repository.dataSource.inTransaction { _ =>
      for {
        // does this entity already exist?
        preExisting <- repository.queries.getEntity(workspaceId, entity.entityType, entity.name)
        _ = if (preExisting.nonEmpty)
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.Conflict,
              s"${entity.entityType} ${entity.name} already exists in ${requestArguments.workspace.toWorkspaceName}"
            )
          )
        // find all references in this entity
        refs: Map[AttributeName, Seq[AttributeEntityReference]] = findAllReferences(entity)
        // find all unique references in this entity
        uniqueRefs: Set[AttributeEntityReference] = refs.values.flatten.toSet
        // verify that all references in the entity-to-be-saved actually exist
        referencedIds <- repository.queries.getReferencedIds(workspaceId, uniqueRefs)
        _ = if (uniqueRefs.size != referencedIds.size)
          throw new EntityReferenceNotFoundException("Some entity references do not exist")
        // save the entity
        _ <- repository.queries.createEntity(workspaceId, entity)
        // did it save correctly? re-retrieve it. By re-retrieving it, we can 1) get its id, and 2) get the actual,
        // normalized JSON that was persisted to the db. When we return the entity to the user, we return the
        // normalized version.
        savedEntityRecordOption <- repository.queries.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new DataEntityException("Could not save entity"))
        // save all references from this entity to other entities
        _ <- replaceReferences(savedEntityRecord.id, referencedIds.toSet, isInsert = true)
      } yield savedEntityRecord.toEntity
    }
    // fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(createFuture)

    createFuture
  }

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference],
                              parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] = ???

  override def deleteEntityAttributes(entityType: String,
                                      attributeNames: Set[AttributeName],
                                      parentContext: RawlsRequestContext
  ): Future[Unit] = ???

  override def entityTypeMetadata(useCache: Boolean,
                                  parentContext: RawlsRequestContext
  ): Future[Map[String, EntityTypeMetadata]] =
    repository.dataSource.inTransaction(ReadOnly) { _ =>
      for {
        entityTypeAndKeys <- repository.queries.listEntityKeys(workspaceId)
        entityTypeAndCounts <- repository.queries.countEntitiesGroupedByType(workspaceId)
      } yield {
        // note that entityTypeAndKeys only contains entity types that have at least one key
        // and that entityTypeAndCounts contains all entity types, even those with zero keys
        val keysByType = entityTypeAndKeys.groupMap(_.entityType)(_.attributeKey)
        entityTypeAndCounts.map { case EntityTypeAndCount(entityType, count) =>
          entityType -> EntityTypeMetadata(
            count,
            entityType + Attributable.entityIdAttributeSuffix,
            keysByType.getOrElse(entityType, Seq.empty).map(AttributeName.toDelimitedName).sortBy(_.toLowerCase)
          )
        }.toMap
      }
    }

  override def evaluateExpression(entityType: String,
                                  entityName: String,
                                  expression: String,
                                  parentContext: RawlsRequestContext
  ): Future[Seq[AttributeValue]] = ???

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: MethodConfigResolver.GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] = ???

  override def expressionValidator: ExpressionValidator = ???

  override def getEntity(entityType: String, entityName: String, parentContext: RawlsRequestContext): Future[Entity] = {
    val queryResult = repository.dataSource.inTransaction(ReadOnly) { _ =>
      repository.queries.getEntity(workspaceId, entityType, entityName)
    }
    queryResult map {
      case Some(entityRec) => entityRec.toEntity
      case None            => throw new EntityNotFoundException()
    }
  }

  override def listEntities(entityType: String): Source[Entity, NotUsed] =
    throw new DataEntityException("list all entities not supported for compact data tables.",
                                  code = StatusCodes.NotImplemented
    )

  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] = ???

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
                                   entityQuery: EntityQuery,
                                   parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = {
    case class CountAndSource(count: Int, source: Source[CompactEntityRecord, _])

    val idAttributeName =
      AttributeName(AttributeName.defaultNamespace, entityType + Attributable.entityIdAttributeSuffix)

    // there are 7 cases
    // 1. filterTerms is defined and sortField is name
    // 2. filterTerms is defined and sortField is not name
    // 3. columnFilter is defined and attributeName is idAttributeName (should have 0 or 1 results)
    // 4. columnFilter is defined and attributeName is not idAttributeName and sortField is name
    // 5. columnFilter is defined and attributeName is not idAttributeName and sortField is not name
    // 6. no filterTerms and no columnFilter and sortField is name
    // 7. no filterTerms and no columnFilter and sortField is not name
    // cases 1 & 2 have the same query to count filtered results
    // cases 4 & 5 have the same query to count filtered results
    // cases 6 & 7 have the same query to count results which is also used to populate unfilteredCount
    val filteredCountAndSourceF: Future[CountAndSource] =
      if (entityQuery.filterTerms.isDefined) {
        repository.dataSource
          .inTransaction(ReadCommitted) { _ =>
            repository.queries.countEntitiesWithFilterTerms(workspaceId, entityType, entityQuery)
          }
          .map { count =>
            val source = if (entityQuery.sortField == Attributable.nameReservedAttribute) {
              // case 1
              repository.queries.queryEntitiesWithFilterTermsSortByName(workspaceId, entityType, entityQuery)
            } else {
              // case 2
              repository.queries.queryEntitiesWithFilterTermsSortByAttribute(workspaceId, entityType, entityQuery)
            }
            CountAndSource(count, streamQuery(source))
          }
      } else if (entityQuery.columnFilter.exists(_.attributeName == idAttributeName)) {
        val entityName = entityQuery.columnFilter.get.term
        repository.dataSource.inTransaction(ReadCommitted) { _ =>
          // case 3
          repository.queries.getEntity(workspaceId, entityType, entityName)
        } map {
          case Some(entityRec) => CountAndSource(1, Source.single(entityRec))
          case None            => CountAndSource(0, Source.empty)
        }
      } else if (entityQuery.columnFilter.isDefined) {
        val columnFilter = entityQuery.columnFilter.get
        repository.dataSource
          .inTransaction(ReadCommitted) { _ =>
            repository.queries.countEntitiesWithColumnFilter(workspaceId, entityType, columnFilter)
          }
          .map { count =>
            val source = if (entityQuery.sortField == Attributable.nameReservedAttribute) {
              // case 4
              repository.queries.queryEntitiesWithColumnFilterSortByName(workspaceId,
                                                                         entityType,
                                                                         entityQuery,
                                                                         columnFilter
              )
            } else {
              // case 5
              repository.queries.queryEntitiesWithColumnFilterSortByAttribute(workspaceId,
                                                                              entityType,
                                                                              entityQuery,
                                                                              columnFilter
              )
            }
            CountAndSource(count, streamQuery(source))
          }
      } else {
        repository.dataSource
          .inTransaction(ReadCommitted) { _ =>
            repository.queries.countEntities(workspaceId, entityType)
          }
          .map { count =>
            val source = if (entityQuery.sortField == Attributable.nameReservedAttribute) {
              // case 6
              repository.queries.queryEntitiesWithNoFilterSortByName(workspaceId, entityType, entityQuery)
            } else {
              // case 7
              repository.queries.queryEntitiesWithNoFilterSortByAttribute(workspaceId, entityType, entityQuery)
            }
            CountAndSource(count, streamQuery(source))
          }
      }

    val unfilteredCountF = if (entityQuery.columnFilter.isEmpty && entityQuery.filterTerms.isEmpty) {
      // if there is no filter then the unfiltered count is the same as the filtered count
      filteredCountAndSourceF.map(_.count)
    } else {
      repository.dataSource.inTransaction(ReadCommitted) { _ =>
        repository.queries.countEntities(workspaceId, entityType)
      }
    }

    for {
      filteredCountAndSource <- filteredCountAndSourceF
      unfilteredCount <- unfilteredCountF
    } yield {
      val pageCount: Int = Math.ceil(filteredCountAndSource.count.toFloat / entityQuery.pageSize).toInt
      if (filteredCountAndSource.count > 0 && entityQuery.page > pageCount) {
        throw new DataEntityException(
          code = StatusCodes.BadRequest,
          message = s"requested page ${entityQuery.page} is greater than the number of pages $pageCount"
        )
      }
      (EntityQueryResultMetadata(unfilteredCount, filteredCountAndSource.count, pageCount),
       filteredCountAndSource.source.map(_.toEntity)
      )
    }
  }

  private def streamQuery(query: SqlStreamingAction[Seq[CompactEntityRecord], CompactEntityRecord, Effect.Read]) = {
    import repository.dataSource.dataAccess.driver.api._
    Source.fromPublisher(
      repository.dataSource.database.stream(
        query.transactionally
          .withTransactionIsolation(ReadCommitted)
          .withStatementParameters(rsType = ResultSetType.ForwardOnly,
                                   rsConcurrency = ResultSetConcurrency.ReadOnly,
                                   fetchSize = repository.dataSource.fetchSize
          )
      )
    )
  }

  override def renameAttribute(entityType: String,
                               oldAttributeName: AttributeName,
                               attributeRenameRequest: AttributeRename,
                               parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def renameEntity(entityType: String,
                            entityName: String,
                            newName: String,
                            parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def renameEntityType(oldName: String,
                                renameInfo: EntityTypeRename,
                                parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def updateEntity(entityType: String,
                            entityName: String,
                            operations: Seq[AttributeUpdateOperations.AttributeUpdateOperation],
                            parentContext: RawlsRequestContext
  ): Future[Entity] = ???

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  // given an entity, finds all references in that entity, grouped by their attribute names
  protected[compact] def findAllReferences(entity: Entity): Map[AttributeName, Seq[AttributeEntityReference]] =
    entity.attributes
      .collect {
        case (name: AttributeName, ref: AttributeEntityReference)         => Seq((name, ref))
        case (name: AttributeName, refList: AttributeEntityReferenceList) => refList.list.map(ref => (name, ref))
      }
      .flatten
      .toSeq
      .groupMap(_._1)(_._2)

  // given already-validated references, represented as target ids, update the ENTITY_REFS table for a given source
  // entity
  protected[compact] def replaceReferences(fromId: Long,
                                           toIds: Set[Long],
                                           isInsert: Boolean
  ): ReadWriteAction[(Int, Int)] = {
    // short-circuit
    if (isInsert && toIds.isEmpty) {
      DBIO.successful((0, 0))
    }

    for {
      // delete any reference pointers that should no longer exist
      deletes <-
        if (isInsert) {
          DBIO.successful(0)
        } else {
          repository.queries.deleteReferences(fromId, toIds)
        }
      // upsert all reference pointers that do exist
      upserts <-
        if (toIds.isEmpty) {
          DBIO.successful(0)
        } else {
          repository.queries.upsertReferences(fromId, toIds)
        }
    } yield (deletes, upserts)
  }

  /**
    * Update the workspace's last-modified timestamp - in a separate transaction - upon successful
    * completion of a given Future.
    *
    * @param func the original function
    * @tparam T return type of original function
    */
  protected[compact] def withWorkspaceLastModified[T](func: Future[T]): Unit =
    func.foreach(_ =>
      repository.dataSource
        .inTransaction { _ =>
          repository.updateLastModified(workspaceId)
        }
        .recover { case t: Throwable =>
          logger.warn(
            s"Failed to update workspace last-modified timestamp. Workspace $workspaceId; message: ${t.getMessage}"
          )
        }
    )

}
