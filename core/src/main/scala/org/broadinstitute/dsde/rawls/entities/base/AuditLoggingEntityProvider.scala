package org.broadinstitute.dsde.rawls.entities.base

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction}
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
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.Try

/**
 * EntityProvider implementation that logs audit information before delegating to another EntityProvider
 * @param delegate The EntityProvider implementation to delegate to after logging
 * @param requestArguments The request arguments containing workspace and context information
 */
class AuditLoggingEntityProvider(val delegate: EntityProvider, val requestArguments: EntityRequestArguments)
    extends EntityProvider {
  private val log = LoggerFactory.getLogger(classOf[AuditLoggingEntityProvider])

  override def entityStoreId: Option[String] = delegate.entityStoreId

  /**
   * Helper method to log audit information
   * @param functionName The name of the function being called
   */
  private def logAudit(functionName: String): Unit = {
    val workspace = requestArguments.workspace
    val ctx = requestArguments.ctx

    // Using a Map structure that will be properly serialized by the logging framework
    val auditInfo = Map(
      "audit" -> Map(
        "function" -> functionName,
        "workspace" -> Map(
          "id" -> workspace.workspaceId,
          "namespace" -> workspace.namespace,
          "name" -> workspace.name
        ),
        "user" -> Map(
          "id" -> ctx.userInfo.userSubjectId,
          "email" -> ctx.userInfo.userEmail
        )
      )
    )

    log.info("Entity operation audit", auditInfo)
  }

  override def batchUpdateEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                                   parentContext: RawlsRequestContext
  ): Future[Int] = {
    logAudit("batchUpdateEntities")
    delegate.batchUpdateEntities(entityUpdates, parentContext)
  }

  override def batchUpsertEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                                   parentContext: RawlsRequestContext
  ): Future[Int] = {
    logAudit("batchUpsertEntities")
    delegate.batchUpsertEntities(entityUpdates, parentContext)
  }

  override def saveWorkflowOutputEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntities: Seq[Entity]
  ): ReadWriteAction[Traversable[Entity]] = {
    logAudit("saveWorkflowOutputEntities")
    delegate.saveWorkflowOutputEntities(dataAccess, workspace, updatedEntities)
  }

  override def listWorkflowEntities(dataAccess: DataAccess,
                                    workspace: Workspace,
                                    entityIds: Seq[Long]
  ): ReadAction[Map[Long, Entity]] = {
    logAudit("listWorkflowEntities")
    delegate.listWorkflowEntities(dataAccess, workspace, entityIds)
  }

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] = {
    logAudit("copyEntities")
    delegate.copyEntities(sourceWorkspaceContext,
                          destWorkspaceContext,
                          entityType,
                          entityNames,
                          linkExistingEntities,
                          parentContext
    )
  }

  override def createEntity(entity: Entity, parentContext: RawlsRequestContext): Future[Entity] = {
    logAudit("createEntity")
    delegate.createEntity(entity, parentContext)
  }

  override def deleteEntities(pointers: Seq[EntityPointer], parentContext: RawlsRequestContext): Future[Int] = {
    logAudit("deleteEntities")
    delegate.deleteEntities(pointers, parentContext)
  }

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] = {
    logAudit("deleteEntitiesOfType")
    delegate.deleteEntitiesOfType(entityType, parentContext)
  }

  override def deleteEntityAttributes(entityType: String,
                                      attributeNames: Set[AttributeName],
                                      parentContext: RawlsRequestContext
  ): Future[Unit] = {
    logAudit("deleteEntityAttributes")
    delegate.deleteEntityAttributes(entityType, attributeNames, parentContext)
  }

  override def entityTypeMetadata(useCache: Boolean,
                                  parentContext: RawlsRequestContext
  ): Future[Map[String, EntityTypeMetadata]] = {
    logAudit("entityTypeMetadata")
    delegate.entityTypeMetadata(useCache, parentContext)
  }

  override def evaluateExpression(entityType: String,
                                  entityName: String,
                                  expression: String,
                                  parentContext: RawlsRequestContext
  ): Future[Seq[AttributeValue]] = {
    logAudit("evaluateExpression")
    delegate.evaluateExpression(entityType, entityName, expression, parentContext)
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] = {
    logAudit("evaluateExpressions")
    delegate.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, workspaceExpressionResults)
  }

  override def expressionValidator: ExpressionValidator = delegate.expressionValidator

  override def getEntity(entityType: String, entityName: String, parentContext: RawlsRequestContext): Future[Entity] = {
    logAudit("getEntity")
    delegate.getEntity(entityType, entityName, parentContext)
  }

  override def listEntities(entityType: String): Source[Entity, NotUsed] = {
    logAudit("listEntities")
    delegate.listEntities(entityType)
  }

  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] = {
    logAudit("queryEntities")
    delegate.queryEntities(entityType, query, parentContext)
  }

  override def queryEntitiesSource(entityType: String,
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = {
    logAudit("queryEntitiesSource")
    delegate.queryEntitiesSource(entityType, query, parentContext)
  }

  override def renameAttribute(entityType: String,
                               oldAttributeName: AttributeName,
                               attributeRenameRequest: AttributeRename,
                               parentContext: RawlsRequestContext
  ): Future[Int] = {
    logAudit("renameAttribute")
    delegate.renameAttribute(entityType, oldAttributeName, attributeRenameRequest, parentContext)
  }

  override def renameEntity(entityType: String,
                            entityName: String,
                            newName: String,
                            parentContext: RawlsRequestContext
  ): Future[Int] = {
    logAudit("renameEntity")
    delegate.renameEntity(entityType, entityName, newName, parentContext)
  }

  override def renameEntityType(oldName: String,
                                renameInfo: EntityTypeRename,
                                parentContext: RawlsRequestContext
  ): Future[Int] = {
    logAudit("renameEntityType")
    delegate.renameEntityType(oldName, renameInfo, parentContext)
  }

  override def updateEntity(entityType: String,
                            entityName: String,
                            operations: Seq[AttributeUpdateOperation],
                            parentContext: RawlsRequestContext
  ): Future[Entity] = {
    logAudit("updateEntity")
    delegate.updateEntity(entityType, entityName, operations, parentContext)
  }
}

// Companion object to provide a factory method
object AuditLoggingEntityProvider {

  /**
   * Creates a new AuditLoggingEntityProvider that wraps the provided delegate
   * @param delegate The EntityProvider implementation to delegate to after logging
   * @param requestArguments The request arguments containing workspace and context information
   * @return A new AuditLoggingEntityProvider instance
   */
  def apply(delegate: EntityProvider, requestArguments: EntityRequestArguments): AuditLoggingEntityProvider =
    new AuditLoggingEntityProvider(delegate, requestArguments)
}
