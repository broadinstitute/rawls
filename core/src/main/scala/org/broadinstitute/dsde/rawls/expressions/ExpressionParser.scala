package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.model.{AttributeString, ParsedMCExpressions}

import scala.util.{Failure, Success, Try}
import scala.language.postfixOps
import spray.json._
import cats.syntax.functor._
import cats.instances.try_._
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{ExtendedJSONLexer, ExtendedJSONParser}

// a thin abstraction layer over SlickExpressionParser

object ExpressionParser {
  def parseMCExpressions(inputs: Map[String, AttributeString], outputs: Map[String, AttributeString], allowRootEntity: Boolean, parser: SlickExpressionParser): ParsedMCExpressions = {
    val noEntityAllowedErrorMsg = "Expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."
    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[Unit] ) = {
      val parsed = m map { case (key, attr) => (key, parseFunc(attr.value)) }
      ( parsed collect { case (key, Success(_)) => key } toSet,
        parsed collect { case (key, Failure(regret)) =>
          if (!allowRootEntity && m.get(key).isDefined && m.get(key).get.value.startsWith("this."))
            (key, noEntityAllowedErrorMsg)
          else
            (key, regret.getMessage)}
      )
    }

    val (successInputs, failedInputs)   = parseAndPartition(inputs, parseInputExpr(allowRootEntity, parser) )
    val (successOutputs, failedOutputs) = parseAndPartition(outputs, parseOutputExpr(allowRootEntity, parser) )

    ParsedMCExpressions(successInputs, failedInputs, successOutputs, failedOutputs)
  }

  private def parseInputExpr(allowRootEntity: Boolean, parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    // Extended JSON inputs need to parsed to find out attribute expressions

    // call ANTLR parser here

//    antlrParser(expression)

    Try(expression.parseJson).recoverWith { case _ => parser.parseAttributeExpr(expression, allowRootEntity) }.void
  }

  private def parseOutputExpr(allowRootEntity: Boolean, parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    parser.parseOutputAttributeExpr(expression, allowRootEntity).void
  }

  def antlrParser(expression: String): ExtendedJSONParser = {
    import org.antlr.v4.runtime.CodePointCharStream
    import org.antlr.v4.runtime.CharStreams
    import org.antlr.v4.runtime.CommonTokenStream


    val inputStream: CodePointCharStream = CharStreams.fromString(expression)
    val lexer: ExtendedJSONLexer         = new ExtendedJSONLexer(inputStream)
    val tokenStream                      = new CommonTokenStream(lexer)
    new ExtendedJSONParser(tokenStream)
  }
}
