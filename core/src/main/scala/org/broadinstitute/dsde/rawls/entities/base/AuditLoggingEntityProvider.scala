package org.broadinstitute.dsde.rawls.entities.base

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import net.logstash.logback.argument.StructuredArguments
import nl.grons.metrics4.scala.Timer
import org.apache.commons.lang3.time.StopWatch
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction, WriteAction}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
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

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.util.Try

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
class AuditLoggingEntityProvider(val delegate: EntityProvider, val requestArguments: EntityRequestArguments)
    extends EntityProvider
    with LazyLogging
    with RawlsInstrumented {
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

  override protected val workbenchMetricBaseName: LookupExpression = "something"

  private def entityProviderMetrics: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(WorkspaceDataMetricKey, "entityProvider")

  private def requestLatency(functionName: String, providerName: String): Timer =
    entityProviderMetrics
      .expand("function", functionName)
      .expand("provider", providerName)
      .asTimer("latency")

  private def instrument[T](functionName: String)(op: Unit => T): T = {
    logAudit(functionName)
    val stopwatch = StopWatch.createStarted()
    val result = op(())
    stopwatch.stop()
    requestLatency(functionName, delegate.getClass.getSimpleName)
      .update(stopwatch.getDuration.toMillis, TimeUnit.MILLISECONDS)
    result
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
