package org.broadinstitute.dsde.rawls.expressions.parser.antlr

//import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.LookupContext
import org.broadinstitute.dsde.rawls.expressions.{ExpressionFixture, ExpressionParser}
import org.scalatest.FlatSpec


class AntlrExpressionParserSpec extends FlatSpec with ExpressionFixture with TestDriverComponent {

  def recursivelyFindLookupNode(node: ParseTree): List[String] = {
    val children = List.range(0, node.getChildCount)

    children.flatMap{ index =>
      val child = node.getChild(index)

      child match {
        case _: LookupContext =>
          //found lookup; stop recursion and note it
//          println(s"look up: ${child.getText}")
          Some(child.getText)
        case _: TerminalNode =>
          // found terminal node
//          println(s"terminal node: ${child.getText}")
          None
        case _ =>
          //            println("doing recursion")
          recursivelyFindLookupNode(child)
      }
    }
  }


  it should "be backwards compatible" in {
//    val input = """{"more":{"elaborate":this.example}}"""
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

    val input = "this.bad|character"

//    val result = parseableInputExpressionsWithNoRoot.map(x => (x, ExpressionParser.antlrParser(x))).map(x => (x._1, x._2.value()))


    val visitor = new ExtendedJSONVisitorImpl(true, this)
    val parsed: ExtendedJSONParser = ExpressionParser.antlrParser(input)

    val result = parsed.value()

    visitor.visit(result) match {
      case scala.util.Success(_) => println("Wohoo")
      case scala.util.Failure(e) => println(s"Received error: ${e.getMessage}")
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
