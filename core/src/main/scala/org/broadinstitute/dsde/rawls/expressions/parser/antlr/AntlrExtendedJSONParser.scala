package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext

object AntlrExtendedJSONParser {

  def findLookupNode(node: ParseTree): Set[String] = {
    val children = List.range(0, node.getChildCount)

    children.flatMap{ index =>
      val child = node.getChild(index)

      child match {
        case _: LookupContext =>
          //found attribute reference
          Some(child.getText)
        case _: TerminalNode =>
          // found terminal node
          None
        case _ =>
          findLookupNode(child)
      }
    }.toSet
  }

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
