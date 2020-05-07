package org.broadinstitute.dsde.rawls.expressions

import cats.instances.try_._
import cats.syntax.functor._
import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}
/*

Are you here because you're using IntelliJ, and got an error:
  object ExtendedJSONLexer is not a member of package org.broadinstitute.dsde.rawls.expressions.parser.antlr

From your rawls directory, run:
   sbt antlr4:antlr4Generate

 */
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{ErrorThrowingListener, ExtendedJSONLexer, ExtendedJSONParser, ExtendedJSONVisitorImpl}
import org.broadinstitute.dsde.rawls.model.{AttributeString, ParsedMCExpressions}
//import spray.json._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

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

  private def getExtendedJSONParser(expression: String): ExtendedJSONParser = {
    val errorThrowingListener = new ErrorThrowingListener()
    val inputStream: CodePointCharStream = CharStreams.fromString(expression)

    val lexer: ExtendedJSONLexer = new ExtendedJSONLexer(inputStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(errorThrowingListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser: ExtendedJSONParser = new ExtendedJSONParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(errorThrowingListener)

    parser
  }

  private def parseInputExpr(allowRootEntity: Boolean, slickParser: SlickExpressionParser)(expression: String): Try[Unit] = {
    // Extended JSON inputs need to parsed to find out attribute expressions

    val extendedJsonParser = getExtendedJSONParser(expression)
    val visitor = new ExtendedJSONVisitorImpl(allowRootEntity, slickParser)

    /*
      parse the expression using ANTLR parser for extended JSON expressions and walk the tree using `visit()` to examine
      child nodes. During the walk, if any child node is a lookup expression, i.e. attribute expressions, it calls the
      `slickParser.parseAttributeExpr()` for that expression and parses it
     */
    Try(extendedJsonParser.value()).flatMap(visitor.visit)

//    Try(expression.parseJson).recoverWith { case _ => slickParser.parseAttributeExpr(expression, allowRootEntity) }.void
  }

  private def parseOutputExpr(allowRootEntity: Boolean, parser: SlickExpressionParser)(expression: String): Try[Unit] = {
    parser.parseOutputAttributeExpr(expression, allowRootEntity).void
  }
}
