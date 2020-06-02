package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}

object AntlrExtendedJSONParser {

  def getParser(expression: String): ExtendedJSONParser = {
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
}
