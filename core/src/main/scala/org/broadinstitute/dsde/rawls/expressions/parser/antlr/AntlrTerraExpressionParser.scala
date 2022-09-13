package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.AttributeNameContext
import org.broadinstitute.dsde.rawls.model.AttributeName

object AntlrTerraExpressionParser {

  def getParser(expression: String): TerraExpressionParser = {
    val errorThrowingListener = new ErrorThrowingListener()
    val inputStream: CodePointCharStream = CharStreams.fromString(expression)

    val lexer: TerraExpressionLexer = new TerraExpressionLexer(inputStream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(errorThrowingListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser: TerraExpressionParser = new TerraExpressionParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(errorThrowingListener)

    parser
  }

  /**
    * Convert AttributeNameContext to AttributeName
    */
  def toAttributeName(ctx: AttributeNameContext): AttributeName = {
    val namespace = Option(ctx.namespace()).map(_.getText).getOrElse(AttributeName.defaultNamespace)
    AttributeName(namespace, ctx.name().getText)
  }
}
