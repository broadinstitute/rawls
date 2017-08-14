package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}
import org.broadinstitute.dsde.rawls.model.{AttributeName, ErrorReportSource}

sealed trait OutputExpressionTarget {
  val prefix: String
  def remainder(s: String): String = s.stripPrefix(prefix)
}
case object ThisEntityTarget extends OutputExpressionTarget { override val prefix = "this." }
case object WorkspaceTarget extends OutputExpressionTarget { override val prefix = "workspace." }

case class OutputExpression(target: OutputExpressionTarget, attributeName: AttributeName)

object OutputExpression extends StringValidationUtils {
  override implicit val errorReportSource = ErrorReportSource("rawls")

  def apply(s: String): OutputExpression = {
    val target = if(s.startsWith(ThisEntityTarget.prefix))
      ThisEntityTarget
    else if(s.startsWith(WorkspaceTarget.prefix))
      WorkspaceTarget
    else
      throw new RawlsException(s"Invalid output expression: $s")

    val attributeName = AttributeName.fromDelimitedName(target.remainder(s))
    validateUserDefinedString(attributeName.name)

    OutputExpression(target, attributeName)
  }
}
