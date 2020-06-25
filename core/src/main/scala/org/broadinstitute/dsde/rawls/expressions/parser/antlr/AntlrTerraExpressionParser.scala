package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}

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
}
