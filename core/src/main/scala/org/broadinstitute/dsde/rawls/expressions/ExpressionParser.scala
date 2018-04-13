package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeString, ParsedMCExpressions}

import scala.util.{Failure, Success, Try}
import spray.json._

import cats.syntax.functor._
import cats.instances.try_._

// a thin abstraction layer over SlickExpressionParser

object ExpressionParser {
  def parseMCExpressions(inputs: Map[String, AttributeString], outputs: Map[String, AttributeString], allowRootEntity: Boolean, parser: SlickExpressionParser): ParsedMCExpressions = {
    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[Unit] ) = {
      val parsed = m map { case (key, attr) => (key, parseFunc(attr.value)) }
      ( parsed collect { case (key, Success(_)) => key } toSeq,
        parsed collect { case (key, Failure(regret)) => (key, regret.getMessage) } )
    }
    val (successInputs, failedInputs)   = parseAndPartition(inputs, parseInputExpr(allowRootEntity, parser) )
    val (successOutputs, failedOutputs) = parseAndPartition(outputs, parseOutputExpr(allowRootEntity, parser) )

    ParsedMCExpressions(successInputs, failedInputs, successOutputs, failedOutputs)
  }

  private def parseInputExpr(allowRootEntity: Boolean, parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    // JSON expressions are valid inputs and do not need to be parsed
    Try(expression.parseJson).recoverWith { case _ => parser.parseAttributeExpr(expression, allowRootEntity) }.void
  }

  private def parseOutputExpr(allowRootEntity: Boolean, parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    parser.parseOutputAttributeExpr(expression, allowRootEntity).void
  }
}
