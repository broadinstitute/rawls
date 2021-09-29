package org.broadinstitute.dsde.rawls.entities

import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.{Attributes, FlowShape, IOResult, Inlet, Outlet}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, StreamConverters}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory, JsonGenerator}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.EntityAttributeRecord
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.deltalayer.DeltaLayerException
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeFormat.{ENTITY_NAME_KEY, ENTITY_TYPE_KEY}
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityCopyDefinition, EntityQuery, ErrorReport, SamResourceTypeNames, SamWorkspaceActions, UserInfo, WorkspaceName, _}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperationException
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.json.DefaultJsonProtocol._

import java.io.{ByteArrayOutputStream, OutputStream}
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

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val entityFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entity <- entityProvider.getEntity(entityType, entityName)
      } yield {
        PerRequest.RequestComplete(StatusCodes.OK, entity)
      }

      entityFuture.recover {
        case _: EntityNotFoundException =>
          // could move this error message into EntityNotFoundException and allow it to bubble up
          RequestComplete(ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName"))
      }.recover(bigQueryRecover)
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
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
            updateAction.map(RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        _ <- entityProvider.deleteEntities(entRefs)
      } yield {
        PerRequest.RequestComplete(StatusCodes.NoContent)
      }

      deleteFuture.recover {
        case delEx: DeleteEntitiesConflictException => RequestComplete(StatusCodes.Conflict, delEx.referringEntities)
      }.recover(bigQueryRecover)
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
          } map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[PerRequestMessage] =
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
                    case (_, Success(result)) => RequestComplete(StatusCodes.OK, result.toSeq)
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

  def entityTypeMetadata(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId], useCache: Boolean): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val metadataFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadata <- entityProvider.entityTypeMetadata(useCache)
      } yield {
        PerRequest.RequestComplete(StatusCodes.OK, metadata)
      }

      metadataFuture.recover(bigQueryRecover)
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String) = {

    import dataSource.dataAccess.entityQuery.EntityAndAttributesRawSqlQuery.EntityAndAttributesResult
//    import spray.json._
//    import spray.json.DefaultJsonProtocol._
//
//    implicit val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) map { workspaceContext =>
      // TODO: set transaction isolation level
      // TODO: play with fetchSize
      val listAllAttrs = dataSource.dataAccess.entityQuery.streamActiveEntityAttributesOfType(workspaceContext, entityType)
        .transactionally
        .withStatementParameters(fetchSize = 1000)

      // database source stream
      val dbPublisher = dataSource.database.stream(listAllAttrs)
      val dbSource = Source.fromPublisher(dbPublisher) // this REQUIRES an order by e.id, a.namespace, a.name, a.list_index

      implicit val system = ActorSystem()

      case class Magpie(accum: Seq[EntityAndAttributesResult], entity: Option[Entity])

      def gatherOrOutput(prev: Magpie, curr: Magpie, forceMarshal: Boolean = false): Magpie = {
        // logger.info(s"*** iterating: $prev :: $curr")
        if (forceMarshal) {
          val unmarshalled = dataSource.dataAccess.entityQuery.unmarshalEntities(curr.accum)
          if (unmarshalled.size != 1) {
            throw new Exception("how did we have more than one entity?")
          }
          val entity = unmarshalled.head
          logger.info(s"*** LAST/FORCED ENTITY: ${entity.name} (${entity.attributes.size})")
          Magpie(curr.accum, Some(entity))
        } else if (prev.accum.isEmpty) {
          // should only happen on the first element
          logger.info(s"*** FIRST ELEMENT")
          curr
        } else if (prev.accum.head.entityRecord.id == curr.accum.head.entityRecord.id) {
          // we are in the same entity, keep gathering attributes
          val newAccum = prev.accum ++ curr.accum
          logger.info(s"*** SAME ENTITY ${newAccum.size}")
          Magpie(newAccum, None)
        } else if (prev.accum.head.entityRecord.id != curr.accum.head.entityRecord.id) {
          // we have started a new entity. Marshal and output what we've accumulated so far,
          // and start a new accumulator for the new entity
          val unmarshalled = dataSource.dataAccess.entityQuery.unmarshalEntities(prev.accum)
          if (unmarshalled.size != 1) {
            throw new Exception("how did we have more than one entity?")
          }
          val entity = unmarshalled.head
          logger.info(s"*** NEW ENTITY: ${entity.name} (${entity.attributes.size})")
          Magpie(curr.accum, Some(entity))
        } else {
          throw new Exception(s"we shouldn't be here: $prev :: $curr")
        }
      }

      class Nest extends GraphStage[FlowShape[Magpie, Magpie]] {
        val in = Inlet[Magpie]("DigestCalculator.in")
        val out = Outlet[Magpie]("DigestCalculator.out")
        override val shape = FlowShape(in, out)

        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
          private var prev: Magpie = Magpie(Seq(), None)

          setHandler(out, new OutHandler {
            override def onPull(): Unit = pull(in)
          })

          setHandler(in, new InHandler {
            override def onPush(): Unit = {
              val next = gatherOrOutput(prev, grab(in))
              prev = next
              emit(out, next)
            }
            override def onUpstreamFinish(): Unit = {
              emit(out, gatherOrOutput(Magpie(Seq(), None), prev, true))
              completeStage()
            }
          })

        }


      }


      val pipeline = dbSource
        .map(x => Magpie(Seq(x), None))
        .via(new Nest())
        .map { x =>
          logger.info(s"********** in this iteration, entity is: ${x.entity}")
          x
        }
        .collect {
          case x if x.entity.isDefined =>
            logger.info(s"********** we have an entity in collect: ${x.entity.map(_.name)}")
            x.entity.get
        }.map { e =>
          logger.info(s"********** we have an entity in map: ${e.name}")
          e
        }

      val entitySource = Source.fromGraph(pipeline)

      entitySource

    }

      /*
      // Jackson streaming-json generator
      val outputStream = new ByteArrayOutputStream()
      val jsonGenerator: JsonGenerator = new JsonFactory().createGenerator(outputStream, JsonEncoding.UTF8)

      def writeAttrPart(prev: EntityAndAttributesResult, curr: EntityAndAttributesResult): EntityAndAttributesResult = {

        val prevListIndex = prev.attributeRecord.flatMap(_.listIndex)
        val currListIndex = curr.attributeRecord.flatMap(_.listIndex)

        // do we need to end an attribute value array?
        (prevListIndex, currListIndex) match {
          case (Some(_), None) => jsonGenerator.writeEndArray()
          case (Some(p), Some(c)) if c <= p => jsonGenerator.writeEndArray()
          case _ => // noop
        }

        // do we need to end the previous entity and start a new one?
        if (curr.entityRecord.id != prev.entityRecord.id) {
          // close previous entity's attributes
          jsonGenerator.writeEndObject()
          // close previous entity
          jsonGenerator.writeEndObject()

          // start new entity
          jsonGenerator.writeStartObject()
          // write entity name and type
          jsonGenerator.writeStringField("name", curr.entityRecord.name)
          jsonGenerator.writeStringField("entityType", curr.entityRecord.entityType)
          // start attributes array
          jsonGenerator.writeFieldName("attributes")
          jsonGenerator.writeStartObject()
        }

        // write current attribute
        curr.attributeRecord.foreach { attr =>
          val fieldName = toDelimitedName(AttributeName(attr.namespace, attr.name))

          // do we need to start an attribute value array?
          (prevListIndex, currListIndex) match {
            case (None, Some(_)) => jsonGenerator.writeStartArray()
            case (Some(p), Some(c)) if c <= p => jsonGenerator.writeStartArray()
            case _ => // noop
          }
          // output the attribute value
          AttributeString
          if (curr.refEntityRecord.isDefined) {
            val ref = dataSource.dataAccess.entityAttributeQuery.unmarshalReference(curr.refEntityRecord.get)
            jsonGenerator.writeFieldName(fieldName)
            jsonGenerator.writeStartObject()
            jsonGenerator.writeStringField(ENTITY_TYPE_KEY, ref.entityType)
            jsonGenerator.writeStringField(ENTITY_NAME_KEY, ref.entityName)
            jsonGenerator.writeEndObject()
          } else {
            val attrValue = dataSource.dataAccess.entityAttributeQuery.unmarshalValue(attr)
            jsonGenerator.writeFieldName(fieldName)
            attrValue match {
              case AttributeNull => jsonGenerator.writeNull()
              case AttributeBoolean(b) => jsonGenerator.writeBoolean(b)
              case AttributeNumber(n) => jsonGenerator.writeNumber(n.toFloat)
              case AttributeString(s) => jsonGenerator.writeString(s)
              case AttributeValueRawJson(j) => jsonGenerator.writeRaw(j.prettyPrint)
            }
          }
        }

        curr
      }

      // output sink, which goes unused
      val sink = Sink.fold[EntityAndAttributesResult, EntityAndAttributesResult](null) ( (acc, element) => writeAttrPart(acc, element))

      dbSource.toMat(sink)(Keep.right).run()

      StreamConverters.fromOutputStream(() => outputStream)
      */
    // }

//    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
//      dataSource.inTransaction { dataAccess =>
//        dataAccess.entityQuery.listActiveEntitiesOfType(workspaceContext, entityType).map(r => RequestComplete(StatusCodes.OK, r.toSeq))
//      }
//    }
  }

  def queryEntities(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], entityType: String, query: EntityQuery, billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val queryFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <- entityProvider.queryEntities(entityType, query)
      } yield {
        PerRequest.RequestComplete(StatusCodes.OK, entities)
      }

      queryFuture.recover(bigQueryRecover)
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean): Future[PerRequestMessage] =

    getWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { destWorkspaceContext =>
      getWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { sourceWorkspaceContext =>
        dataSource.inTransaction { dataAccess =>
          for {
            sourceAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceContext.workspaceId, userInfo))
            destAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWorkspaceContext.workspaceId, userInfo))
            result <- authDomainCheck(sourceAD.toSet, destAD.toSet) flatMap { _ =>
              val entityNames = entityCopyDef.entityNames
              val entityType = entityCopyDef.entityType
              val copyResults = dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, linkExistingEntities)
              copyResults.map { response =>
                if (response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntities)) RequestComplete(StatusCodes.Created, response)
                else RequestComplete(StatusCodes.Conflict, response)
              }
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

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, dataReference, billingProject).map (_ =>
      RequestComplete(StatusCodes.NoContent))

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, dataReference, billingProject).map (_ =>
      RequestComplete(StatusCodes.NoContent))

  private def bigQueryRecover: PartialFunction[Throwable, PerRequestMessage] = {
    case dee:DataEntityException =>
      RequestComplete(ErrorReport(dee.code, dee.getMessage))
    case bqe:BigQueryException =>
      RequestComplete(ErrorReport(StatusCodes.getForKey(bqe.getCode).getOrElse(StatusCodes.InternalServerError), bqe.getMessage))
    case gjre:GoogleJsonResponseException =>
      // unlikely to hit this case; we should see BigQueryExceptions instead of GoogleJsonResponseExceptions
      RequestComplete(ErrorReport(StatusCodes.getForKey(gjre.getStatusCode).getOrElse(StatusCodes.InternalServerError), gjre.getMessage))
    case report:RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex:Exception =>
      RequestComplete(ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex))
  }

}
