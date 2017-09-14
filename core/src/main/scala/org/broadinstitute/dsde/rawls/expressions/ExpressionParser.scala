package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeString, MethodConfiguration, ValidatedMethodConfiguration}

import scala.util.{Failure, Success, Try}

import spray.json._

// a thin abstraction layer over SlickExpressionParser

object ExpressionParser {
  def parseMCExpressions(methodConfiguration: MethodConfiguration, parser: SlickExpressionParser): ValidatedMethodConfiguration = {
    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[Unit] ) = {
      val parsed = m map { case (key, attr) => (key, parseFunc(attr.value)) }
      ( parsed collect { case (key, Success(_)) => key } toSeq,
        parsed collect { case (key, Failure(regret)) => (key, regret.getMessage) } )
    }
    val (successInputs, failedInputs)   = parseAndPartition(methodConfiguration.inputs, parseAttributeExpr(parser) )
    val (successOutputs, failedOutputs) = parseAndPartition(methodConfiguration.outputs, parseOutputExpr(parser) )

    ValidatedMethodConfiguration(methodConfiguration, successInputs, failedInputs, successOutputs, failedOutputs)
  }

  private def parseAttributeExpr(parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    Try(expression.parseJson) match {
      case Success(parsed) => Success(true)
      case Failure(regret) => parser.parseAttributeExpr(expression) map (_ => ())
    }
  }

  private def parseOutputExpr(parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    parser.parseOutputExpr(expression) map (_ => ())
  }
}
