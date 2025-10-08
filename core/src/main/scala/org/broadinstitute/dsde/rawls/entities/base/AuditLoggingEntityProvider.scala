package org.broadinstitute.dsde.rawls.entities.base

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import net.logstash.logback.argument.StructuredArguments
import org.apache.commons.lang3.time.StopWatch
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction, WriteAction}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeRename,
  AttributeValue,
  Entity,
  EntityCopyResponse,
  EntityPointer,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  EntityTypeRename,
  JsonSupport,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

// Case classes for structured audit logging
case class WorkspaceInfo(id: String, namespace: String, name: String)
case class UserInfo(id: String, email: String)
case class AuditInfo(function: String, workspace: WorkspaceInfo, user: UserInfo)

// JSON support for audit case classes
object AuditJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val WorkspaceInfoFormat: RootJsonFormat[WorkspaceInfo] = jsonFormat3(WorkspaceInfo)
  implicit val UserInfoFormat: RootJsonFormat[UserInfo] = jsonFormat2(UserInfo)
  implicit val AuditInfoFormat: RootJsonFormat[AuditInfo] = jsonFormat3(AuditInfo)
}

/**
 * EntityProvider implementation that logs audit information before delegating to another EntityProvider
 * @param delegate The EntityProvider implementation to delegate to after logging
 * @param requestArguments The request arguments containing workspace and context information
 */
class AuditLoggingEntityProvider(val delegate: EntityProvider,
                                 val requestArguments: EntityRequestArguments,
                                 metricsPrefix: String
)(implicit executionContext: ExecutionContext)
    extends EntityProvider
    with LazyLogging
    with EntityProviderMetrics {
  override def entityStoreId: Option[String] = delegate.entityStoreId

  /**
   * Helper method to log audit information
   * @param functionName The name of the function being called
   */
  private def logAudit(functionName: String): Unit = {
    val workspace = requestArguments.workspace
    val ctx = requestArguments.ctx

    // Create structured audit info using case classes
    val auditInfo = AuditInfo(
      function = functionName,
      workspace = WorkspaceInfo(
        id = workspace.workspaceId,
        namespace = workspace.namespace,
        name = workspace.name
      ),
      user = UserInfo(
        id = ctx.userInfo.userSubjectId.value,
        email = ctx.userInfo.userEmail.value
      )
    )

    import AuditJsonSupport._
    logger.info("Entity operation audit", StructuredArguments.raw("audit", auditInfo.toJson.compactPrint))
  }

  override protected val workbenchMetricBaseName: String = metricsPrefix

  private def instrument[T](functionName: String)(op: Unit => T): T = {
    logAudit(functionName) // log the action
    val stopwatch = StopWatch.createStarted() // start a timer
    val tryResult: Try[T] = Try(op(())) // execute the function being wrapped
    tryResult match {
      // Handle the case where T is a Future (which may succeed or fail). In this case,
      // register a callback to record success/error metrics once the Future completes,
      // then return the original Future.
      case Success(future: Future[_]) =>
        future.onComplete {
          case Success(_) =>
            stopwatch.stop() // stop the timer
            recordFunctionLatency(functionName, delegate, stopwatch.getDuration.toMillis)
          case Failure(ex) =>
            stopwatch.stop() // stop the timer
            recordError(functionName, delegate, ex)
        }
        // for legal syntax, this needs to return T, not Future[_]
        tryResult.get
      // T is not a Future: on success, capture latency and count metrics for the wrapped
      // function then return the wrapped function's result
      case Success(result) =>
        stopwatch.stop() // stop the timer
        recordFunctionLatency(functionName, delegate, stopwatch.getDuration.toMillis)
        result
      // T is not a Future: on error, increment the error count metric and rethrow the exception
      case Failure(exception) =>
        stopwatch.stop() // stop the timer
        recordError(functionName, delegate, exception)
        throw exception
    }
  }

  override def batchUpdateEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                                   parentContext: RawlsRequestContext
  ): Future[Int] =
    instrument("batchUpdateEntities") { _ =>
      delegate.batchUpdateEntities(entityUpdates, parentContext)
    }

  override def batchUpsertEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                                   parentContext: RawlsRequestContext
  ): Future[Int] =
    instrument("batchUpsertEntities") { _ =>
      delegate.batchUpsertEntities(entityUpdates, parentContext)
    }

  override def saveWorkflowOutputEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntities: Seq[Entity]
  ): ReadWriteAction[Int] =
    instrument("saveWorkflowOutputEntities") { _ =>
      delegate.saveWorkflowOutputEntities(dataAccess, workspace, updatedEntities)
    }

  override def listWorkflowEntities(dataAccess: DataAccess,
                                    workspace: Workspace,
                                    entityIds: Seq[Long]
  ): ReadAction[Map[Long, Entity]] =
    instrument("listWorkflowEntities") { _ =>
      delegate.listWorkflowEntities(dataAccess, workspace, entityIds)
    }

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] =
    instrument("copyEntities") { _ =>
      delegate.copyEntities(sourceWorkspaceContext,
                            destWorkspaceContext,
                            entityType,
                            entityNames,
                            linkExistingEntities,
                            parentContext
      )
    }

  override def clone(sourceWorkspaceContext: Workspace,
                     destWorkspaceContext: Workspace,
                     parentContext: RawlsRequestContext
  ): WriteAction[(Int, Int)] =
    instrument("clone") { _ =>
      delegate.clone(sourceWorkspaceContext, destWorkspaceContext, parentContext)
    }

  override def createEntity(entity: Entity, parentContext: RawlsRequestContext): Future[Entity] =
    instrument("createEntity") { _ =>
      delegate.createEntity(entity, parentContext)
    }

  override def deleteEntities(pointers: Seq[EntityPointer], parentContext: RawlsRequestContext): Future[Int] =
    instrument("deleteEntities") { _ =>
      delegate.deleteEntities(pointers, parentContext)
    }

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] =
    instrument("deleteEntitiesOfType") { _ =>
      delegate.deleteEntitiesOfType(entityType, parentContext)
    }

  override def deleteEntityAttributes(entityType: String,
                                      attributeNames: Set[AttributeName],
                                      parentContext: RawlsRequestContext
  ): Future[Unit] =
    instrument("deleteEntityAttributes") { _ =>
      delegate.deleteEntityAttributes(entityType, attributeNames, parentContext)
    }

  override def entityTypeMetadata(useCache: Boolean,
                                  parentContext: RawlsRequestContext
  ): Future[Map[String, EntityTypeMetadata]] =
    instrument("entityTypeMetadata") { _ =>
      delegate.entityTypeMetadata(useCache, parentContext)
    }

  override def evaluateExpression(entityType: String,
                                  entityName: String,
                                  expression: String,
                                  parentContext: RawlsRequestContext
  ): Future[Seq[AttributeValue]] =
    instrument("evaluateExpression") { _ =>
      delegate.evaluateExpression(entityType, entityName, expression, parentContext)
    }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] =
    instrument("evaluateExpressions") { _ =>
      delegate.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, workspaceExpressionResults)
    }

  override def expressionValidator: ExpressionValidator = delegate.expressionValidator

  override def getEntity(entityType: String, entityName: String, parentContext: RawlsRequestContext): Future[Entity] =
    instrument("getEntity") { _ =>
      delegate.getEntity(entityType, entityName, parentContext)
    }

  override def listEntities(entityType: String): Source[Entity, NotUsed] =
    instrument("listEntities") { _ =>
      delegate.listEntities(entityType)
    }

  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] =
    instrument("queryEntities") { _ =>
      delegate.queryEntities(entityType, query, parentContext)
    }

  override def queryEntitiesSource(entityType: String,
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] =
    instrument("queryEntitiesSource") { _ =>
      delegate.queryEntitiesSource(entityType, query, parentContext)
    }

  override def renameAttribute(entityType: String,
                               oldAttributeName: AttributeName,
                               attributeRenameRequest: AttributeRename,
                               parentContext: RawlsRequestContext
  ): Future[Int] =
    instrument("renameAttribute") { _ =>
      delegate.renameAttribute(entityType, oldAttributeName, attributeRenameRequest, parentContext)
    }

  override def renameEntity(entityType: String,
                            entityName: String,
                            newName: String,
                            parentContext: RawlsRequestContext
  ): Future[Int] =
    instrument("renameEntity") { _ =>
      delegate.renameEntity(entityType, entityName, newName, parentContext)
    }

  override def renameEntityType(oldName: String,
                                renameInfo: EntityTypeRename,
                                parentContext: RawlsRequestContext
  ): Future[Int] =
    instrument("renameEntityType") { _ =>
      delegate.renameEntityType(oldName, renameInfo, parentContext)
    }

  override def updateEntity(entityType: String,
                            entityName: String,
                            operations: Seq[AttributeUpdateOperation],
                            parentContext: RawlsRequestContext
  ): Future[Entity] =
    instrument("updateEntity") { _ =>
      delegate.updateEntity(entityType, entityName, operations, parentContext)
    }

}
