package org.broadinstitute.dsde.rawls.entities.base

import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeValue,
  Entity,
  EntityCopyDefinition,
  EntityCopyResponse,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace,
  WorkspaceName
}

import scala.concurrent.Future
import scala.util.Try

/**
 * trait definition for entity providers.
 */
trait EntityProvider {
  def entityStoreId: Option[String]

  def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]]

  def createEntity(entity: Entity): Future[Entity]

  def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int]

  def deleteEntitiesOfType(entityType: String): Future[Int]

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

  def getEntity(entityType: String, entityName: String): Future[Entity]

  def queryEntitiesSource(entityType: String,
                          query: EntityQuery,
                          parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])]

  def queryEntities(entityType: String,
                    query: EntityQuery,
                    parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse]

  def batchUpdateEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]]

  def batchUpsertEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]]

  def copyEntities(sourceWorkspaceContext: Workspace,
                   destWorkspaceContext: Workspace,
                   entityType: String,
                   entityNames: Seq[String],
                   linkExistingEntities: Boolean,
                   parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse]

  def renameEntity(entity: AttributeEntityReference, newName: String): Future[Int]
}
