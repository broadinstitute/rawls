package org.broadinstitute.dsde.rawls

import com.tinkerpop.blueprints.{Graph, Edge, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.{Pipe, PipeFunction}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.scalatest.FunSuite

import scala.collection.JavaConversions._

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.combinator._

/**
 * Created by abaumann on 5/12/15.
 */
class ParserCombinatorTest extends FunSuite with OrientDbTestFixture {
  override val testDbName = "ParserCombinatorTest"

  override def initializeGraph: Unit = {
    val workspace = graph.addVertex(null, "_namespace", "workspaces", "_name", "Workspace1", "_entityType", "Workspace")

    val sample1 = graph.addVertex(null, "_name", "sample1", "_entityType", "Sample", "type", "normal")
    val sample2 = graph.addVertex(null, "_name", "sample2", "_entityType", "Sample", "type", "tumor")
    val sample3 = graph.addVertex(null, "_name", "sample3", "_entityType", "Sample", "type", "tumor")
    val pair1 = graph.addVertex(null, "_name", "pair1", "_entityType", "Pair")
    val pair2 = graph.addVertex(null, "_name", "pair2", "_entityType", "Pair")
    val sampleSets = graph.addVertex(null, "_name", "sampleSets")
    val sampleSet1 = graph.addVertex(null, "_name", "sampleSet1", "_entityType", "SampleSet")
    val pairSet1 = graph.addVertex(null, "_name", "pairSet1", "_entityType", "Pair")

    workspace.addEdge("samples", sample1)
    workspace.addEdge("samples", sample2)
    workspace.addEdge("samples", sample3)

    workspace.addEdge("pairs", pair1)
      pair1.addEdge("case", sample1)
      pair1.addEdge("control", sample2)

    workspace.addEdge("pairs", pair2)
      pair2.addEdge("case", sample1)
      pair2.addEdge("control", sample3)

    workspace.addEdge("sampleSets", sampleSets)
      sampleSets.addEdge("sampleSets", sampleSet1)
        sampleSet1.addEdge("contains", sample1)
        sampleSet1.addEdge("contains", sample2)
        sampleSet1.addEdge("contains", sample3)

    workspace.addEdge("pairSets", pairSet1)
      pairSet1.addEdge("contains", pair1)
      pairSet1.addEdge("contains", pair2)


    graph.commit()
  }

  test("get sample from sample set") {
    println("get sampleSet1's sample called sample1 from the root sampleSets")
    val value = new QueryExecutor(graph).execute("root.ref(sampleSet1).ref(sample1)")("sampleSets")
    println("results: " + value)
    println()
  }

  test("get sample from sample set attribute") {
    println("get the entityType from sampleSet1's sample called sample1 from the root sampleSets")
    val value = new QueryExecutor(graph).execute("root.ref(sampleSet1).@(_entityType)")("sampleSets")
    println("results: " + value)
    println()
  }
}


class QueryExecutor(graph:Graph) extends JavaTokenParsers {

  def applySteps(rootName:String, steps:PipelineQuery, pipeline:PipeType, pipelineText:String) = {
    // TODO: more idiomatic way to do this?  the GremlinPipeline has side effects, and adds pipes to each .out() and similar calls anyway, so maybe not...
    var pipelineQueryText = pipelineText
    var result:PipeResult[_] = null

    // apply the root function
    result = steps.rootFunc(rootName, pipeline)
    pipelineQueryText += result.pipeAction

    // then apply each pipe from this root
    for (step <- steps.pipes) {
      result = step(pipeline)

      pipelineQueryText += result.pipeAction
    }

    println("DEBUG: running query: " + pipelineQueryText)

    /*** WARNING disgusting code below ***/
    // TODO: somehow we need to have the last step in the pipeline return Vertex(es) or attribute(s), never a GremlinPipeline
    // TODO: we probably want the parser to figure out the last step and have PipelineQuery have a root, pipes, and finalStep, where
    // TODO  finalStep returns the type we can know at compile time - perhaps a Seq of Any
    // if the result is a GremlinPipeline, then get the Vertex(es) list
    if (classOf[PipeType] isAssignableFrom result.result.getClass) {
      result.result.asInstanceOf[PipeType].toList.map((v:Vertex) => "<Vertex: " + v.getPropertyKeys.map((k:String)=>k+":"+v.getProperty(k).toString).mkString(", ") + ">")
    }
    // otherwise it is the only case below that does not return a GremlinPipeline, which is the attributes, which return a list of strings
    else {
      result.result
    }
  }

  def execute(expression:String)(rootName:String) = parseAll(path, expression) match {
    case Success(result, _) => {
      def workspaceFunc = new PipeFunction[Vertex, java.lang.Boolean] {
        override def compute(a: Vertex) = {
          (a.getProperty("_namespace") == "workspaces" && a.getProperty("_name") == "Workspace1")
        }
      }

      val pipeline = new PipeType(graph).V("_entityType", "Workspace").filter(workspaceFunc)

      // result is a list of functions to apply to the pipeline in order

      applySteps(rootName, result, pipeline, """new GremlinPipeline(graph).V("_entityType", "Workspace").filter(workspaceFunc)""")
    }
    case NoSuccess(msg, next) => {
      println("Failed at line %s, column %s: %s".format(
        next.pos.line, next.pos.column, msg))

      println(expression.substring(0, next.pos.column-1) + "<" + expression.substring(next.pos.column-1, next.pos.column) + ">" + expression.substring(next.pos.column))
      null
    }
  }



  // based upon http://bitwalker.org/blog/2013/08/10/learn-by-example-scala-parser-combinators/
  /** combinators:
    *      |     is the alternation combinator. It says “succeed if either the left or right operand parse successfully”
    *      ~     is the sequential combinator. It says “succeed if the left operand parses successfully, and then the right parses successfully on the remaining input”
    *      ~>    says “succeed if the left operand parses successfully followed by the right, but do not include the left content in the result”
    *      <~    is the reverse, “succeed if the left operand is parsed successfully followed by the right, but do not include the right content in the result”
    *      ^^    is the transformation combinator. It says “if the left operand parses successfully, transform the result using the function on the right”
    *      rep   simply says “expect N-many repetitions of parser X” where X is the parser passed as an argument to rep
    */



  case class PipeResult[RS](result:RS, pipeAction:String)
  type PipeResultPipeline = PipeResult[PipeType]
  type PipeResultAny = PipeResult[Any]
  type PipeResultVertexes = PipeResult[Iterable[Vertex]]
  type PipeType = GremlinPipeline[Vertex, Vertex]
  type PipeFunc = PipeType => PipeResult[_]
  type RootFunc = (String, PipeType) => PipeResultPipeline
  case class PipelineQuery(rootFunc:RootFunc, pipes:List[PipeFunc])

  /** syntax definition **/
  private def path:Parser[PipelineQuery] =
    // we expect an entity identifier followed by 0 or more entities referenced by each proceeding reference
    root ~ references ~ (attribute?) ^^
      // match against entity followed by 0 or more refs, create list from entity followed by flattened ref lists
      {
        case root ~ refs ~ None => PipelineQuery(root, refs)
        case root ~ refs ~ Some(att) => PipelineQuery(root, refs :+ att)
      }

  private def attribute:Parser[PipeFunc] =
    ".@(" ~> ident <~ ")" ^^
      {case name => attributePipeFunc(name)}

  private def references:Parser[List[PipeFunc]] =
    rep("." ~> reference) ^^
      {case reps => reps}//List(ref, reps).flatten}


  private def reference:Parser[PipeFunc] =
    "ref(" ~> ident <~ ")" ^^
      {case ref => referencePipeFunc(ref.toString)}


  private def root:Parser[RootFunc] =
    "root" ^^ {_ => rootFunc} //(" ~> ident <~ ")" ^^
     // {case e => rootFunc(e.toString)}


  private def entityFunc(entityName:String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(a: Vertex) = {
      a.getProperty("_name") == entityName
    }
  }
  /** functions against pipes **/
  private def referencePipeFunc(ref:String)(pipe:PipeType):PipeResultPipeline = {

    PipeResult(
      pipe.out().filter(entityFunc(ref)),
      // TODO: is there a way to automate returning this code-string from the code itself? (used for debugging purposes)
      s".out().filter(entityFunc($ref))"
    )
  }
  private def rootFunc(rootName:String, pipe:PipeType):PipeResultPipeline = {
    PipeResult(
      pipe.out().filter(entityFunc(rootName)),
      s".out($rootName)"
    )
  }
  private def attributePipeFunc(attributeName:String)(pipe:PipeType):PipeResultAny = {
    def getAttribute(v:Vertex, att:String): String = {
      Option(v.getProperty[Any](attributeName)) match {
        case Some(a) => a.toString
        case None => ""
      }
    }
    val attr = pipe.toList.map((v:Vertex) => getAttribute(v, attributeName))

    println(attr);

    PipeResult(
      attr,
      s""".toList.map((v:Vertex) => getProperty("$attributeName"))"""
    )
  }
  /*****************************/

//  private def wrapper(f: => Any)(code:String): Any = {
//    val result = f
//    result
//  }




  //val pipeLine = new GremlinPipeline(graph).V("_entityType", "Workspace").filter(workspaceFunc)/*.step(new PipeFunction[Any, Unit] {

  //  private def path:Parser[GremlinPipeline[_,_]] = entityIdentifier ~ rep("." ~ referenceHierarchy) ^^ {case e => println("path");null} //pipeline.V("_name", entityName)}
  //    //{case entityName => Foo(entityName.toString)}//(" [entity:" + _.toString()+"] ")
  //
  //  private def referenceHierarchy:Parser[GremlinPipeline[_,_]] = reference ~ rep("." ~ referenceHierarchy) ^^ {case e => println("refHierarchy");null}
  //
  //  private def reference:Parser[GremlinPipeline[_,_]] = "ref(" ~> ident <~ ")" ^^ {case ref => println("ref: " + ref.toString);null}//pipeline.out().filter(entityFunc(ref.toString))}
  //  private def entityIdentifier:Parser[GremlinPipeline[_,_]] = "e(" ~> entityName <~ ")" ^^ {case e => println("entityName: " + e.toString);null}//pipeline.out(e.toString)}
  //  private def entityName:Parser[String] = ident ^^ (_.toString)


  //  def wrap(ret:String)(value:String) = {ret}
  //
  //  private def path:Parser[List[String => String]] = entityIdentifier ~ rep("." ~> referenceHierarchy) ^^ {case e ~ reps => println("path: " + reps); e +: reps.flatten} //pipeline.V("_name", entityName)}
  //  //{case entityName => Foo(entityName.toString)}//(" [entity:" + _.toString()+"] ")
  //
  //  private def referenceHierarchy:Parser[List[String => String]] = reference ~ rep("." ~> referenceHierarchy) ^^ {case ref ~ reps => println("refHierarchy:" + ref + " " +reps);ref +: reps.flatten}
  //  private def reference:Parser[String => String] = "ref(" ~> ident <~ ")" ^^ {case ref => println("ref: " + ref.toString);wrap(ref.toString)_}//pipeline.out().filter(entityFunc(ref.toString))}
  //  private def entityIdentifier:Parser[String => String] = "e(" ~> ident <~ ")" ^^ {case e => wrap(e.toString)_}//pipeline.out(e.toString)}
  //  private def entityName:Parser[String => String] = ident ^^ {case name => println("entityName: " + name); wrap(name.toString)_}


  //  private val pipeClosure = (op:GremlinPipeline[_,_], codeStr:String) => ((f:GremlinPipeline[_,_]) => {op; codeStr})_
}
