package org.broadinstitute.dsde.rawls.entities.base

case class ExpressionEvaluationContext(
  entityType: Option[String],
  entityName: Option[String],
  expression: Option[String],
  rootEntityType: Option[String]
)
