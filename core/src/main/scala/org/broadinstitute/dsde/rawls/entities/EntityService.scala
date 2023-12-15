package org.broadinstitute.dsde.rawls.entities

import akka.NotUsed
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityAndAttributesResult, ReadAction}
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException
}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  Entity,
  EntityCopyDefinition,
  EntityQuery,
  ErrorReport,
  SamResourceTypeNames,
  SamWorkspaceActions,
  WorkspaceName,
  _
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperationException
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import slick.jdbc.{ResultSetConcurrency, ResultSetType, TransactionIsolation}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object EntityService {
  def constructor(dataSource: SlickDataSource,
                  samDAO: SamDAO,
                  workbenchMetricBaseName: String,
                  entityManager: EntityManager,
                  pageSizeLimit: Int
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): EntityService =
    new EntityService(ctx, dataSource, samDAO, entityManager, workbenchMetricBaseName, pageSizeLimit)
}

class EntityService(protected val ctx: RawlsRequestContext,
                    val dataSource: SlickDataSource,
                    val samDAO: SamDAO,
                    entityManager: EntityManager,
                    override val workbenchMetricBaseName: String,
                    pageSizeLimit: Int
)(implicit protected val executionContext: ExecutionContext)
    extends WorkspaceSupport
    with EntitySupport
    with AttributeSupport
    with LazyLogging
    with RawlsInstrumented
    with JsonFilterUtils {

  import dataSource.dataAccess.driver.api._

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] =
    withAttributeNamespaceCheck(entity) {
      for {
        workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName,
                                                                SamWorkspaceActions.write,
                                                                Some(WorkspaceAttributeSpecs(all = false))
        )
        entityManager <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, ctx))
        result <- entityManager.createEntity(entity)
      } yield result
    }

  def getEntity(workspaceName: WorkspaceName,
                entityType: String,
                entityName: String,
                dataReference: Option[DataReferenceName],
                billingProject: Option[GoogleProjectId]
  ): Future[Entity] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val entityFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entity <- entityProvider.getEntity(entityType, entityName)
      } yield entity

      entityFuture
        .recover { case _: EntityNotFoundException =>
          // could move this error message into EntityNotFoundException and allow it to bubble up
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName")
          )
        }
        .recover(bigQueryRecover)
    }

  def updateEntity(workspaceName: WorkspaceName,
                   entityType: String,
                   entityName: String,
                   operations: Seq[AttributeUpdateOperation]
  ): Future[Entity] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      getV2WorkspaceContextAndPermissions(workspaceName,
                                          SamWorkspaceActions.write,
                                          Some(WorkspaceAttributeSpecs(all = false))
      ) flatMap { workspaceContext =>
        dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Entity)) { dataAccess =>
          withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
            val updateAction = Try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              dataAccess.entityQuery.save(workspaceContext, updatedEntity)
            } match {
              case Success(result) => result
              case Failure(e: AttributeUpdateOperationException) =>
                DBIO.failed(
                  new RawlsExceptionWithErrorReport(
                    errorReport =
                      ErrorReport(StatusCodes.BadRequest,
                                  s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}",
                                  ErrorReport(e)
                      )
                  )
                )
              case Failure(regrets) => DBIO.failed(regrets)
            }
            updateAction
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName,
                     entRefs: Seq[AttributeEntityReference],
                     dataReference: Option[DataReferenceName],
                     billingProject: Option[GoogleProjectId]
  ): Future[Set[AttributeEntityReference]] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        _ <- entityProvider.deleteEntities(entRefs)
      } yield Set[AttributeEntityReference]()

      deleteFuture
        .recover { case delEx: DeleteEntitiesConflictException =>
          delEx.referringEntities
        }
        .recover(bigQueryRecover)
    }

  def deleteEntitiesOfType(workspaceName: WorkspaceName,
                           entityType: String,
                           dataReference: Option[DataReferenceName],
                           billingProject: Option[GoogleProjectId]
  ) =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        numberOfEntitiesDeleted <- entityProvider.deleteEntitiesOfType(entityType)
      } yield numberOfEntitiesDeleted

      deleteFuture
        .recover { case delEx: DeleteEntitiesOfTypeConflictException =>
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.Conflict,
              s"Entity type [$entityType] cannot be deleted because there are ${delEx.conflictCount} references " +
                s"to this entity type. All references must be removed before deleting a type."
            )
          )
        }
        .recover(bigQueryRecover)
    }

  def deleteEntityAttributes(workspaceName: WorkspaceName,
                             entityType: String,
                             attributeNames: Set[AttributeName]
  ): Future[Unit] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess
          .entityAttributeShardQuery(workspaceContext)
          .deleteAttributes(workspaceContext, entityType, attributeNames) flatMap {
          case Vector(0) =>
            throw new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.BadRequest, s"Could not find any of the given attribute names.")
            )
          case _ => DBIO.successful(())
        }
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[Int] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) =>
              throw new RawlsExceptionWithErrorReport(
                errorReport =
                  ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists")
              )
          }
        }
      }
    }

  def renameEntityType(workspaceName: WorkspaceName, oldName: String, renameInfo: EntityTypeRename): Future[Int] = {
    import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction}

    def validateExistingType(dataAccess: DataAccess,
                             workspaceContext: Workspace,
                             oldName: String
    ): ReadAction[Boolean] =
      dataAccess.entityQuery.doesEntityTypeAlreadyExist(workspaceContext, oldName) map {
        case Some(true) => true
        case Some(false) =>
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.NotFound, s"Can't find entity type ${oldName}")
          )
        case None =>
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.InternalServerError,
                                      s"Unexpected error; could not determine existence of entity type ${oldName}"
            )
          )
      }

    def validateNewType(dataAccess: DataAccess, workspaceContext: Workspace, newName: String): ReadAction[Boolean] =
      dataAccess.entityQuery.doesEntityTypeAlreadyExist(workspaceContext, newName) map {
        case Some(true) =>
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.Conflict, s"${newName} already exists as an entity type")
          )
        case Some(false) => false
        case None =>
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.InternalServerError,
                                      s"Unexpected error; could not determine existence of entity type ${newName}"
            )
          )
      }

    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        for {
          _ <- validateNewType(dataAccess, workspaceContext, renameInfo.newName)
          _ <- validateExistingType(dataAccess, workspaceContext, oldName)
          renameResult <- dataAccess.entityQuery.changeEntityTypeName(workspaceContext, oldName, renameInfo.newName)
        } yield renameResult
      }
    }
  }

  def evaluateExpression(workspaceName: WorkspaceName,
                         entityType: String,
                         entityName: String,
                         expression: String
  ): Future[Seq[AttributeValue]] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSingleEntityRec(entityType, entityName, workspaceContext, dataAccess) { entities =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, Some(entities)) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, expression).asTry map { tryValuesByEntity =>
              tryValuesByEntity match {
                // parsing failure
                case Failure(regret) =>
                  throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret))
                case Success(valuesByEntity) =>
                  if (valuesByEntity.size != 1) {
                    // wrong number of entities?!
                    throw new RawlsException(
                      s"Expression parsing should have returned a single entity for ${entityType}/$entityName $expression, but returned ${valuesByEntity.size} entities instead"
                    )
                  } else {
                    assert(valuesByEntity.head._1 == entityName)
                    valuesByEntity.head match {
                      case (_, Success(result)) => result.toSeq
                      case (_, Failure(regret)) =>
                        throw new RawlsExceptionWithErrorReport(
                          errorReport = ErrorReport(
                            StatusCodes.BadRequest,
                            "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}",
                            ErrorReport(regret)
                          )
                        )
                    }
                  }
              }
            }
          }
        }
      }
    }

  def entityTypeMetadata(workspaceName: WorkspaceName,
                         dataReference: Option[DataReferenceName],
                         billingProject: Option[GoogleProjectId],
                         useCache: Boolean
  ): Future[Map[String, EntityTypeMetadata]] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val metadataFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadata <- entityProvider.entityTypeMetadata(useCache)
      } yield metadata

      metadataFuture.recover(bigQueryRecover)
    }

  /*
   * Queries the db for a stream of entity attributes.
   */
  private def listEntitiesDbSource(workspaceContext: Workspace,
                                   entityType: String
  ): Source[EntityAndAttributesResult, NotUsed] = {
    // note: ReadCommitted transaction isolation level; forward-only/read-only stream.
    val allAttrsStream = dataSource.dataAccess.entityQuery
      .streamActiveEntityAttributesOfType(workspaceContext, entityType)
      .transactionally
      .withTransactionIsolation(TransactionIsolation.ReadCommitted)
      .withStatementParameters(rsType = ResultSetType.ForwardOnly,
                               rsConcurrency = ResultSetConcurrency.ReadOnly,
                               fetchSize = dataSource.dataAccess.fetchSize
      )

    // translate the Slick stream to a Source
    Source.fromPublisher(dataSource.database.stream(allAttrsStream))
  }

  /**
    * Given a Source containing entity attributes, scroll through that source and combine
    * attributes into entities. Emit a Source of entities.
    * <p>
    * IMPORTANT: this !!requires!! that the incoming Source of attributes is ordered by
    * entity ID. If the incoming source is not properly ordered, this method will emit
    * incomplete/duplicate entities.
    * <p>
    * Only used internally by EntityService.listEntities, but public to support unit testing
    * @param dbSource the Source of attributes, typically from a database stream
    * @return a Source of entities constructed from the attributes
    */
  def gatherEntities(dbSource: Source[EntityAndAttributesResult, NotUsed]): Source[Entity, NotUsed] = {
    // interim classes used while iterating through the stream, allows us to accumulate attributes
    // until ready to emit an entity
    trait AttributeStreamElement
    case class AttrAccum(accum: Seq[EntityAndAttributesResult], entity: Option[Entity]) extends AttributeStreamElement
    case object EmptyElement extends AttributeStreamElement

    /*
     * Given the previous and current stream elements, which are produced by the EntityCollector,
     * calculate the AttrAccum to be output
     */
    def gatherOrOutput(previous: AttributeStreamElement, current: AttributeStreamElement): AttrAccum = {
      // utility function called when an entity is finished or when the stream is finished
      def entityFinished(prevAttrs: Seq[EntityAndAttributesResult], nextAttrs: Seq[EntityAndAttributesResult]) = {
        val unmarshalled = dataSource.dataAccess.entityQuery.unmarshalEntities(prevAttrs)
        // safety check - did the attributes we gathered all marshal into a single entity?
        if (unmarshalled.size != 1)
          throw new DataEntityException(s"gatherOrOutput expected only one entity, found ${unmarshalled.size}")
        AttrAccum(nextAttrs, Some(unmarshalled.head))
      }

      // inspect the variations of previous and current
      (previous, current) match {
        // if both previous and current are empty, it means no entities found
        case (EmptyElement, EmptyElement) =>
          AttrAccum(Seq(), None)

        // if previous is empty but current is not, it's the first element
        case (EmptyElement, curr: AttrAccum) =>
          curr

        // midstream, we notice that the current entity is the same as the previous entity.
        // keep gathering attributes for this entity, and don't emit an entity yet.
        case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id == curr.accum.head.entityRecord.id =>
          val newAccum = prev.accum ++ curr.accum
          AttrAccum(newAccum, None)

        // midstream, we notice that the current entity's id is greater than the previous entity's id.
        // take all the attributes we have gathered for the previous entity,
        // marshal them into an Entity object, emit that Entity, and start a new accumulator
        // for the new/current entity
        case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id < curr.accum.head.entityRecord.id =>
          entityFinished(prev.accum, curr.accum)

        // midstream, we notice that the current entity's id is LESS than the previous entity's id.
        // this breaks the assumption that entities are ordered by their ids ascending, and indicates a coding
        // error. Throw an exception.
        case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id > curr.accum.head.entityRecord.id =>
          throw new RawlsException(
            "Unexpected internal error; the previous results may be incomplete. Cause: entity source input is in unexpected order."
          )

        // if current is empty but previous is not, it means the stream has finished.
        // Marshal and output the final Entity.
        case (prev: AttrAccum, EmptyElement) =>
          entityFinished(prev.accum, Seq())

        // relief valve, this should not happen, but if it does we should log it
        case _ =>
          throw new Exception(
            s"gatherOrOutput encountered unexpected input, cannot continue. Prev: $previous :: Curr: $current"
          )
      }
    }

    /* custom stream stage that allows us to compare the current stream element
       to the previous stream element. In turn, this allows us to accumulate attributes
       until we notice that the current element is from a different entity than the previous attribute;
       when that happens, we marshal and emit an entity.
     */
    class EntityCollector extends GraphStage[FlowShape[AttrAccum, AttrAccum]] {
      val in = Inlet[AttrAccum]("EntityCollector.in")
      val out = Outlet[AttrAccum]("EntityCollector.out")
      override val shape = FlowShape(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
        private var prev: AttributeStreamElement = EmptyElement // note: var!

        // if our downstream pulls on us, propagate that pull to our upstream
        setHandler(out,
                   new OutHandler {
                     override def onPull(): Unit = pull(in)
                   }
        )

        setHandler(
          in,
          new InHandler {
            // when a new element arrives ...
            override def onPush(): Unit = {
              // send it to gatherOrOutput which has most of the logic
              val next = gatherOrOutput(prev, grab(in))
              // save the current element to "prev" to prepare for the next iteration
              prev = next
              // emit whatever gatherOrOutput returned
              emit(out, next)
            }

            // when the upstream finishes ...
            override def onUpstreamFinish(): Unit = {
              // ensure we marshal and emit the last entity
              emit(out, gatherOrOutput(prev, EmptyElement))
              completeStage()
            }
          }
        )
      }
    }

    val pipeline = dbSource
      .map(entityAndAttributesResult =>
        AttrAccum(Seq(entityAndAttributesResult), None)
      ) // transform EntityAndAttributesResult to AttrAccum
      .via(new EntityCollector()) // execute the business logic to accumulate attributes and emit entities
      .collect { // "flatten" the stream to only emit entities
        case AttrAccum(_, Some(entity)) => entity
      }
      .log("gatherEntities")
      .addAttributes(
        Attributes.logLevels(onElement = Attributes.LogLevels.Debug,
                             onFinish = Attributes.LogLevels.Info,
                             onFailure = Attributes.LogLevels.Error
        )
      )

    Source.fromGraph(pipeline) // return a Source, which akka-http natively knows how to stream to the caller
  }

  def listEntities(workspaceName: WorkspaceName, entityType: String) =
    getWorkspaceContextAndPermissions(workspaceName,
                                      SamWorkspaceActions.read,
                                      Some(WorkspaceAttributeSpecs(all = false))
    ) map { workspaceContext =>
      val dbSource = listEntitiesDbSource(workspaceContext, entityType)
      gatherEntities(dbSource)
    }

  def queryEntities(workspaceName: WorkspaceName,
                    dataReference: Option[DataReferenceName],
                    entityType: String,
                    query: EntityQuery,
                    billingProject: Option[GoogleProjectId]
  ): Future[EntityQueryResponse] = {
    if (query.pageSize > pageSizeLimit) {
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, s"Page size cannot exceed $pageSizeLimit")
      )
    }

    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.read,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)

      val queryFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <- entityProvider.queryEntities(entityType, query, ctx)
      } yield entities

      queryFuture.recover(bigQueryRecover)
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, linkExistingEntities: Boolean): Future[EntityCopyResponse] =
    for {
      destWsCtx <- getV2WorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace,
                                                       SamWorkspaceActions.write,
                                                       Some(WorkspaceAttributeSpecs(all = false))
      )
      sourceWsCtx <- getV2WorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,
                                                         SamWorkspaceActions.read,
                                                         Some(WorkspaceAttributeSpecs(all = false))
      )
      sourceAD <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWsCtx.workspaceId, ctx)
      destAD <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWsCtx.workspaceId, ctx)
      _ = authDomainCheck(sourceAD.toSet, destAD.toSet)
      entityRequestArguments = EntityRequestArguments(destWsCtx, ctx, None, None)
      entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
      entityCopyResponse <- entityProvider
        .copyEntities(sourceWsCtx,
                      destWsCtx,
                      entityCopyDef.entityType,
                      entityCopyDef.entityNames,
                      linkExistingEntities,
                      ctx
        )
        .recover(bigQueryRecover)
    } yield entityCopyResponse

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName,
                                  entityUpdates: Seq[EntityUpdateDefinition],
                                  upsert: Boolean,
                                  dataReference: Option[DataReferenceName],
                                  billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    getV2WorkspaceContextAndPermissions(workspaceName,
                                        SamWorkspaceActions.write,
                                        Some(WorkspaceAttributeSpecs(all = false))
    ) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, ctx, dataReference, billingProject)
      for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <-
          if (upsert) {
            entityProvider.batchUpsertEntities(entityUpdates)
          } else {
            entityProvider.batchUpdateEntities(entityUpdates)
          }
      } yield entities
    }

  def batchUpdateEntities(workspaceName: WorkspaceName,
                          entityUpdates: Seq[EntityUpdateDefinition],
                          dataReference: Option[DataReferenceName],
                          billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, dataReference, billingProject)

  def batchUpsertEntities(workspaceName: WorkspaceName,
                          entityUpdates: Seq[EntityUpdateDefinition],
                          dataReference: Option[DataReferenceName],
                          billingProject: Option[GoogleProjectId]
  ): Future[Traversable[Entity]] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, dataReference, billingProject)

  def renameAttribute(workspaceName: WorkspaceName,
                      entityType: String,
                      oldAttributeName: AttributeName,
                      attributeRenameRequest: AttributeRename
  ): Future[Int] =
    withAttributeNamespaceCheck(Seq(attributeRenameRequest.newAttributeName)) {
      getV2WorkspaceContextAndPermissions(workspaceName,
                                          SamWorkspaceActions.write,
                                          Some(WorkspaceAttributeSpecs(all = false))
      ) flatMap { workspaceContext =>
        def validateNewAttributeName(dataAccess: DataAccess,
                                     workspaceContext: Workspace,
                                     entityType: String,
                                     attributeName: AttributeName
        ): ReadAction[Boolean] =
          dataAccess
            .entityAttributeShardQuery(workspaceContext)
            .doesAttributeNameAlreadyExist(workspaceContext, entityType, attributeName) map {
            case Some(false) => false
            case Some(true) =>
              throw new RawlsExceptionWithErrorReport(
                errorReport =
                  ErrorReport(StatusCodes.Conflict,
                              s"${AttributeName.toDelimitedName(attributeName)} already exists as an attribute name"
                  )
              )
            case None =>
              throw new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(
                  StatusCodes.InternalServerError,
                  s"Unexpected error; could not determine existence of attribute name ${AttributeName.toDelimitedName(attributeName)}"
                )
              )
          }

        def validateRowsUpdated(rowsUpdated: Int, oldAttributeName: AttributeName): Boolean =
          rowsUpdated match {
            case 0 =>
              throw new RawlsExceptionWithErrorReport(
                errorReport =
                  ErrorReport(StatusCodes.NotFound,
                              s"Can't find attribute name ${AttributeName.toDelimitedName(oldAttributeName)}"
                  )
              )
            case _ => true
          }

        dataSource.inTransaction { dataAccess =>
          val newAttributeName = attributeRenameRequest.newAttributeName
          for {
            _ <- validateNewAttributeName(dataAccess, workspaceContext, entityType, newAttributeName)
            rowsUpdated <- dataAccess
              .entityAttributeShardQuery(workspaceContext)
              .renameAttribute(workspaceContext, entityType, oldAttributeName, newAttributeName)
            _ = validateRowsUpdated(rowsUpdated, oldAttributeName)
          } yield rowsUpdated
        }
      }
    }

  private def bigQueryRecover[U]: PartialFunction[Throwable, U] = {
    case dee: DataEntityException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(dee.code, dee.getMessage))
    case bqe: BigQueryException =>
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.getForKey(bqe.getCode).getOrElse(StatusCodes.InternalServerError), bqe.getMessage)
      )
    case gjre: GoogleJsonResponseException =>
      // unlikely to hit this case; we should see BigQueryExceptions instead of GoogleJsonResponseExceptions
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.getForKey(gjre.getStatusCode).getOrElse(StatusCodes.InternalServerError),
                    gjre.getMessage
        )
      )
    case report: RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex: Exception =>
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex)
      )
  }

}
