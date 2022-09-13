package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.{BaseErrorListener, RecognitionException, Recognizer}
import org.broadinstitute.dsde.rawls.RawlsException

/*
  According to the documentation, when we use `DefaultErrorStrategy` or `BailErrorStrategy`, the
  `ParserRuleContext.exception` field is set for any parse tree node in the resulting parse tree where an error occurred.
  But this does not cause the parser to throw an exception. Instead the default way is to println the errors and return
  the parsing as a success. What we want is a different way to report the errors. This can be achieved by creating our
  own Listener class and replace the default listeners with instance of this class while creating the parser.
  Reference: https://stackoverflow.com/questions/18132078/handling-errors-in-antlr4/18137301#18137301
 */
class ErrorThrowingListener extends BaseErrorListener {

  /*
    Below function overrides a Java method. IntelliJ's Scala plugin for type-aware highlighting reports
    "Method overrides nothing". The issue is that it does not seem to know the relationship between Java base class and
    Scala subclass and shows a false positive error message. Instead of disabling the Scala type-aware plugin, enclosing
    such methods in `/*_*/..../*_*/` will disable the type-aware checking for that method.
    Reference: https://stackoverflow.com/questions/36679973/controlling-false-intellij-code-editor-error-in-scala-plugin
   */
  /*_*/
  override def syntaxError(recognizer: Recognizer[_, _],
                           offendingSymbol: AnyRef,
                           line: Int,
                           charPositionInLine: Int,
                           msg: String,
                           e: RecognitionException
  ): Unit = {
    val errorMsg =
      s"Error while parsing the expression. Offending symbol is on line $line at position $charPositionInLine. Error: $msg"
    throw new RawlsException(errorMsg, new ParseCancellationException(errorMsg, e))
  }
  /*_*/
}
