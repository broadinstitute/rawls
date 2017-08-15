package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeName, ErrorReportSource}

sealed trait OutputExpressionTarget {
  val prefix: String
  def remainder(s: String): String = s.stripPrefix(prefix)
}
case object ThisEntityTarget extends OutputExpressionTarget { override val prefix = "this." }
case object WorkspaceTarget extends OutputExpressionTarget { override val prefix = "workspace." }

sealed trait OutputExpression
case object UnboundOutputExpression extends OutputExpression
case class BoundOutputExpression(target: OutputExpressionTarget, attributeName: AttributeName, attribute: Attribute) extends OutputExpression

object OutputExpression extends StringValidationUtils {
  override implicit val errorReportSource = ErrorReportSource("rawls")

  def apply(expr: String, attribute: Attribute): OutputExpression = {
    if (expr.isEmpty) UnboundOutputExpression
    else {
      val target = if (expr.startsWith(ThisEntityTarget.prefix))
        ThisEntityTarget
      else if (expr.startsWith(WorkspaceTarget.prefix))
        WorkspaceTarget
      else
        throw new RawlsException(s"Invalid output expression: $expr")

      val attributeName = AttributeName.fromDelimitedName(target.remainder(expr))
      validateUserDefinedString(attributeName.name)

      BoundOutputExpression(target, attributeName, attribute)
    }
  }
}
