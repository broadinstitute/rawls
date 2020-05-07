package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.expressions.ExpressionFixture
import org.scalatest.FlatSpec

import scala.util.Try


class AntlrExpressionParserSpec extends FlatSpec with ExpressionFixture with TestDriverComponent {

  def antlrParser(expression: String): ExtendedJSONParser = {
    import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}


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

  it should "be backwards compatible" in {
    val input = """{"more":{"elaborate":{"reference1": this.val1, "path":"gs://abc/123"}}}"""
//        val input = """"a string literal""""
//        val input = "this.example"
//    val input = """
//                  |{
//                  |"more" : {
//                  |   "example": {
//                  |     "ab":this.foo:example,
//                  |     "def":workspace.cohort,
//                  |     "xyz": "123"
//                  |   }
//                  |}
//                  |}
//                """.stripMargin

//    val input = "this.bad|character|another"

//    val result = parseableInputExpressionsWithNoRoot.map(x => (x, ExpressionParser.antlrParser(x))).map(x => (x._1, x._2.value()))


    val visitor = new ExtendedJSONVisitorImpl(true, this)
    val parser = antlrParser(input)


    val result = Try(parser.value()).flatMap(visitor.visit)

    result match {
      case scala.util.Success(_) =>
        println("Wohoo")
      case scala.util.Failure(e) =>
        println(s"Received error: ${e.getMessage}")
    }


//    val lookupExpressions: mutable.Seq[List[String]] = result.children.asScala.map(recursivelyFindLookupNode)

//    println("---------------------------------")
//    println(s"INPUT: $input")
//    println(s"Expressions to lookup: $lookupExpressions")


//    parseableInputExpressionsWithNoRoot.foreach { input =>
//      val parsed = ExpressionParser.antlrParser(input)
//      val result = parsed.value()
//      val lookupExpressions: mutable.Seq[List[String]] = result.children.asScala.map(recursivelyFindLookupNode)
//
//      println("---------------------------------")
//      println(s"INPUT: $input")
//      println(s"Expressions to lookup: $lookupExpressions")
//    }


  }

//  it should "be backwards compatible for parseableInputExpressionsWithRoot" in {
//
//    val result = parseableInputExpressionsWithRoot.map(x => (x, ExpressionParser.antlrParser(x))).map(x => (x._1, x._2.value()))
//
//    println(result)
//  }
//
//  it should "barf on invalid things" in {
//
//    //    import org.antlr.v4.runtime.RecognitionException
//
//    unparseableInputExpressions.map {
//      x => (x, ExpressionParser.antlrParser(x))
//    } map { x: (String, ExtendedJSONParser) =>
//      val value = x._2.value()
//      (x._1, value)
//    }
//
//  }
}
