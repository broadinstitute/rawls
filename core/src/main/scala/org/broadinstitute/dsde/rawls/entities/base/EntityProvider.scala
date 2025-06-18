package org.broadinstitute.dsde.rawls.entities.base

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
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

import scala.concurrent.Future
import scala.util.Try

/**
 * trait definition for entity providers.
 */
trait EntityProvider {
  // entityStoreId is used by subclasses to identify themselves
  def entityStoreId: Option[String]

  // ----- implementation methods follow:

  def batchUpdateEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                          parentContext: RawlsRequestContext
  ): Future[Int]

  def batchUpsertEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                          parentContext: RawlsRequestContext
  ): Future[Int]

  def copyEntities(sourceWorkspaceContext: Workspace,
                   destWorkspaceContext: Workspace,
                   entityType: String,
                   entityNames: Seq[String],
                   linkExistingEntities: Boolean,
                   parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse]

  def createEntity(entity: Entity, parentContext: RawlsRequestContext): Future[Entity]

  def deleteEntities(pointers: Seq[EntityPointer], parentContext: RawlsRequestContext): Future[Int]

  def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int]

  def deleteEntityAttributes(entityType: String,
                             attributeNames: Set[AttributeName],
                             parentContext: RawlsRequestContext
  ): Future[Unit]

  def entityTypeMetadata(useCache: Boolean, parentContext: RawlsRequestContext): Future[Map[String, EntityTypeMetadata]]

  def evaluateExpression(entityType: String,
                         entityName: String,
                         expression: String,
                         parentContext: RawlsRequestContext
  ): Future[Seq[AttributeValue]]

  /**
  The overall approach is:
        - Parse the input expression using ANTLR Extended JSON parser
        - Visit the parsed tree to find all the lookup expressions (e.g. this.attribute)
        - If there are lookup expressions:
            - for each lookup expressions, evaluate in the entity provider specific way
            - through a series of transformations, generate a Map of entity name to Map of lookup expressions and their
              evaluated value for that entity
            - for each entity, substitute the evaluated values back into the input expression
    To help understand the approach if there are lookup expressions present, we will follow the below example roughly:
      expression = "{"exampleRef1":this.bam, "exampleIndex":this.index}"
      rootEntities = Seq(101, 102) (here we assume the entity name is 101 for Entity Record 1 and 102 for Entity record 2)

    The final output will be:
    ReadWriteAction(Map(
          "101" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://abc", "exampleIndex":123}"))),
          "102" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://def", "exampleIndex":456}")))
        )
    )

    see core/src/main/antlr4/org/broadinstitute/dsde/rawls/expressions/parser/antlr/TerraExpression.g4
    */
  def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                          gatherInputsResult: GatherInputsResult,
                          workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]]

  def expressionValidator: ExpressionValidator

  def getEntity(entityType: String, entityName: String, parentContext: RawlsRequestContext): Future[Entity]

  def listEntities(entityType: String): Source[Entity, NotUsed]

  def listWorkflowEntities(dataAccess: DataAccess,
                           workspace: Workspace,
                           entityIds: Seq[Long]
  ): ReadAction[Map[Long, Entity]]

  def queryEntities(entityType: String,
                    query: EntityQuery,
                    parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse]

  def queryEntitiesSource(entityType: String,
                          query: EntityQuery,
                          parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])]

  def renameAttribute(entityType: String,
                      oldAttributeName: AttributeName,
                      attributeRenameRequest: AttributeRename,
                      parentContext: RawlsRequestContext
  ): Future[Int]

  def renameEntity(entityType: String,
                   entityName: String,
                   newName: String,
                   parentContext: RawlsRequestContext
  ): Future[Int]

  def renameEntityType(oldName: String, renameInfo: EntityTypeRename, parentContext: RawlsRequestContext): Future[Int]

  def saveWorkflowOutputEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntities: Seq[Entity]
  ): ReadWriteAction[Int]

  def updateEntity(entityType: String,
                   entityName: String,
                   operations: Seq[AttributeUpdateOperation],
                   parentContext: RawlsRequestContext
  ): Future[Entity]
}
