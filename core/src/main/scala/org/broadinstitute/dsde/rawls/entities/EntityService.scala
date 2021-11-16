package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.deltalayer.DeltaLayerException
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityCopyDefinition, EntityQuery, ErrorReport, SamResourceTypeNames, SamWorkspaceActions, UserInfo, WorkspaceName, _}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperationException
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import slick.jdbc.{ResultSetConcurrency, ResultSetType, TransactionIsolation}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object EntityService {
  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workbenchMetricBaseName: String, entityManager: EntityManager)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): EntityService = {

    new EntityService(userInfo, dataSource, samDAO, entityManager, workbenchMetricBaseName)
  }
}

class EntityService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, entityManager: EntityManager, override val workbenchMetricBaseName: String)(implicit protected val executionContext: ExecutionContext)
  extends WorkspaceSupport with EntitySupport with AttributeSupport with LazyLogging with RawlsInstrumented with JsonFilterUtils {

  import dataSource.dataAccess.driver.api._

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] = {
    withAttributeNamespaceCheck(entity) {
      for {
        workspaceContext <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false)))
        entityManager <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, userInfo))
        result <- entityManager.createEntity(entity)
      } yield result
    }
  }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Entity] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val entityFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entity <- entityProvider.getEntity(entityType, entityName)
      } yield {
        entity
      }

      entityFuture.recover {
        case _: EntityNotFoundException =>
          // could move this error message into EntityNotFoundException and allow it to bubble up
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName"))
      }.recover(bigQueryRecover)
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[Entity] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
        dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Entity)) { dataAccess =>
          withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
            val updateAction = Try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              dataAccess.entityQuery.save(workspaceContext, updatedEntity)
            } match {
              case Success(result) => result
              case Failure(e: AttributeUpdateOperationException) =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}", ErrorReport(e))))
              case Failure(regrets) => DBIO.failed(regrets)
            }
            updateAction
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Set[AttributeEntityReference]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        _ <- entityProvider.deleteEntities(entRefs)
      } yield {
        Set[AttributeEntityReference]()
      }

      deleteFuture.recover {
        case delEx: DeleteEntitiesConflictException => delEx.referringEntities
      }.recover(bigQueryRecover)
    }

  def deleteEntityAttributes(workspaceName: WorkspaceName, entityType: String, attributeNames: Set[AttributeName]): Future[Unit] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.entityQuery.deleteAttributes(workspaceContext, entityType, attributeNames) flatMap {
          case Vector(0) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Could not find any of the given attribute names."))
          case _ => DBIO.successful(())
        }
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[Int] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
          }
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[Seq[AttributeValue]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSingleEntityRec(entityType, entityName, workspaceContext, dataAccess) { entities =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, Some(entities)) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, expression).asTry map { tryValuesByEntity => tryValuesByEntity match {
              //parsing failure
              case Failure(regret) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret))
              case Success(valuesByEntity) =>
                if (valuesByEntity.size != 1) {
                  //wrong number of entities?!
                  throw new RawlsException(s"Expression parsing should have returned a single entity for ${entityType}/$entityName $expression, but returned ${valuesByEntity.size} entities instead")
                } else {
                  assert(valuesByEntity.head._1 == entityName)
                  valuesByEntity.head match {
                    case (_, Success(result)) => result.toSeq
                    case (_, Failure(regret)) =>
                      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}", ErrorReport(regret)))
                  }
                }
            }
            }
          }
        }
      }
    }

  def entityTypeMetadata(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId], useCache: Boolean): Future[Map[String, EntityTypeMetadata]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val metadataFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadata <- entityProvider.entityTypeMetadata(useCache)
      } yield {
        metadata
      }

      metadataFuture.recover(bigQueryRecover)
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String) = {

    import dataSource.dataAccess.entityQuery.EntityAndAttributesResult

    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) map { workspaceContext =>
      // note ReadCommitted transaction isolation level
      val allAttrsStream = dataSource.dataAccess.entityQuery.streamActiveEntityAttributesOfType(workspaceContext, entityType)
        .transactionally.withTransactionIsolation(TransactionIsolation.ReadCommitted)
        .withStatementParameters(
          rsType = ResultSetType.ForwardOnly,
          rsConcurrency = ResultSetConcurrency.ReadOnly,
          fetchSize = dataSource.dataAccess.fetchSize)

      // database source stream
      val dbSource = Source.fromPublisher(dataSource.database.stream(allAttrsStream)) // this REQUIRES an order by ENTITY.id

      // interim class used while iterating through the stream, allows us to accumulate attributes
      // until ready to emit an entity
      trait AttributeStreamElement
      case class AttrAccum(accum: Seq[EntityAndAttributesResult], entity: Option[Entity]) extends AttributeStreamElement
      case object EmptyElement extends AttributeStreamElement

      def gatherOrOutput(previous: AttributeStreamElement, current: AttributeStreamElement): AttrAccum = {
        // utility function called when an entity is finished or when the stream is finished
        def entityFinished(prevAttrs: Seq[EntityAndAttributesResult], nextAttrs: Seq[EntityAndAttributesResult]) = {
          val unmarshalled = dataSource.dataAccess.entityQuery.unmarshalEntities(prevAttrs, workspaceContext.shardState)
          // safety check - did the attributes we gathered all marshal into a single entity?
          if (unmarshalled.size != 1)
            throw new DataEntityException(s"gatherOrOutput expected only one entity, found ${unmarshalled.size}")
          AttrAccum(nextAttrs, Some(unmarshalled.head))
        }

        (previous, current) match {
          // the first element
          case (EmptyElement, curr: AttrAccum) =>
            curr

          // midstream, we notice that the current entity is the same as the previous entity.
          // keep gathering attributes for this entity, and don't emit an entity yet.
          case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id == curr.accum.head.entityRecord.id =>
            val newAccum = prev.accum ++ curr.accum
            AttrAccum(newAccum, None)

          // midstream, we notice that the current entity is DIFFERENT from the previous entity.
          // take all the attributes we have gathered for the previous entity,
          // marshal them into an Entity object, emit that Entity, and start a new accumulator
          // for the new/current entity
          case (prev: AttrAccum, curr: AttrAccum) if prev.accum.head.entityRecord.id != curr.accum.head.entityRecord.id =>
            entityFinished(prev.accum, curr.accum)

          // the stream has finished (curr == EmptyElement). marshal and output the final Entity.
          case (prev: AttrAccum, EmptyElement) =>
            entityFinished(prev.accum, Seq())

          // relief valve, this should not happen
          case _ =>
            throw new Exception(s"gatherOrOutput encountered unexpected input, cannot continue. Prev: $previous :: Curr: $current")
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
          setHandler(out, new OutHandler {
            override def onPull(): Unit = pull(in)
          })

          setHandler(in, new InHandler {
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
          })
        }
      }

      val pipeline = dbSource
        .map(x => AttrAccum(Seq(x), None)) // transform EntityAndAttributesResult to AttrAccum
        .via(new EntityCollector())        // execute the business logic to accumulate attributes and emit entities
        .collect {                         // "flatten" the stream to only emit entities
          case x if x.entity.isDefined => x.entity.get
        }

      Source.fromGraph(pipeline) // return a Source, which akka-http natively knows how to stream to the caller
    }
  }

  def queryEntities(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], entityType: String, query: EntityQuery, billingProject: Option[GoogleProjectId], parentSpan: Span = null): Future[EntityQueryResponse] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val queryFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <- entityProvider.queryEntities(entityType, query, parentSpan)
      } yield {
        entities
      }

      queryFuture.recover(bigQueryRecover)
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean, parentSpan: Span = null): Future[EntityCopyResponse] =

    getWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { destWorkspaceContext =>
      getWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { sourceWorkspaceContext =>
        dataSource.inTransaction { dataAccess =>
          for {
            sourceAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceContext.workspaceId, userInfo))
            destAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWorkspaceContext.workspaceId, userInfo))
            result <- authDomainCheck(sourceAD.toSet, destAD.toSet) flatMap { _ =>
              val entityNames = entityCopyDef.entityNames
              val entityType = entityCopyDef.entityType
              val copyResults = traceDBIOWithParent("checkAndCopyEntities", parentSpan)( s1 => dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, linkExistingEntities, s1))
              copyResults
            }
          } yield result
        }
      }
    }

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Traversable[Entity]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)
      (for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities       <- if (upsert) {
                            entityProvider.batchUpsertEntities(entityUpdates)
                          } else {
                            entityProvider.batchUpdateEntities(entityUpdates)
                          }
      } yield {
        entities
      }) recover {
        case dle: DeltaLayerException =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(dle.code, dle.getMessage, dle))
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Traversable[Entity]] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, dataReference, billingProject)

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Traversable[Entity]] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, dataReference, billingProject)

  private def bigQueryRecover[U]: PartialFunction[Throwable, U] = {
    case dee:DataEntityException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(dee.code, dee.getMessage))
    case bqe:BigQueryException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.getForKey(bqe.getCode).getOrElse(StatusCodes.InternalServerError), bqe.getMessage))
    case gjre:GoogleJsonResponseException =>
      // unlikely to hit this case; we should see BigQueryExceptions instead of GoogleJsonResponseExceptions
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.getForKey(gjre.getStatusCode).getOrElse(StatusCodes.InternalServerError), gjre.getMessage))
    case report:RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex:Exception =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex))
  }

}
