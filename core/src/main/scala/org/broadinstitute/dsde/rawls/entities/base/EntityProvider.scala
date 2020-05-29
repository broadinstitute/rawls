package org.broadinstitute.dsde.rawls.entities.base

import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata, Workspace, SubmissionValidationEntityInputs}

import scala.concurrent.Future

/**
 * trait definition for entity providers.
 */
trait EntityProvider {

  def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]]

  def createEntity(entity: Entity): Future[Entity]

  def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int]

  def evaluateExpressions(workspaceContext: Workspace, expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult): Future[Stream[SubmissionValidationEntityInputs]]

  def expressionValidator: ExpressionValidator

}
