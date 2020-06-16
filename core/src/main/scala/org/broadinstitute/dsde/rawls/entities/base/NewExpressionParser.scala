package org.broadinstitute.dsde.rawls.entities.base

import org.broadinstitute.dsde.rawls.entities.base.ExpressionTypes.ExpressionType

class NewExpressionParser {

}

case class ParsedExpression(expression: List[String], expressionType: ExpressionType, errorMessage: Option[String] = None)
case class ParsedMCExpression(inputs: List[ParsedExpression], outputs: List[ParsedExpression])

object ExpressionTypes {
  sealed trait ExpressionType

  case object Workspace extends ExpressionType
  case object Entity extends ExpressionType
  case object Json extends ExpressionType
  case object Invalid extends ExpressionType
}
