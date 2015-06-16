package org.broadinstitute.dsde.rawls.expressions

import com.tinkerpop.blueprints.{Direction, Edge, Vertex, Graph}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.scalatest.FunSuite
import scala.collection.JavaConversions._
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by abaumann on 5/21/15.
 */
class SimpleExpressionParserTest extends FunSuite with OrientDbTestFixture {
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
    val sampleSet2 = graph.addVertex(null, "_name", "sampleSet2", "_entityType", "SampleSet")
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
    sampleSets.addEdge("sampleSet", sampleSet1)
    sampleSet1.addEdge("samples", sample1)
    sampleSet1.addEdge("samples", sample2)
    sampleSet1.addEdge("samples", sample3)
    sampleSets.addEdge("sampleSet", sampleSet2)
    sampleSet2.addEdge("samples", sample2)

    workspace.addEdge("pairSets", pairSet1)
    pairSet1.addEdge("contains", pair1)
    pairSet1.addEdge("contains", pair2)


    graph.commit()
  }

  test("simple expression") {
    try {
      println("get the entity type from all samples from all sample sets from the root sampleSets")
      val value = new SimpleExpressionEvaluator(graph).evaluate("root.sampleSet.samples._entityType")("sampleSets")
      println("results: " + value)
      println()
    } finally {
    }
  }

  class Foo() {
    def ret() = { "var!" }
  }
}
class SimpleExpressionEvaluator(graph:Graph) extends JavaTokenParsers {

  def applySteps(rootName:String, steps:PipelineQuery):Seq[String] = {
    val pipeline = new PipeType(graph)

    // TODO: more idiomatic way to do this?  the GremlinPipeline has side effects, and adds pipes to each .out() and similar calls anyway, so maybe not...
    var pipelineQueryText = ""
    var result:PipeResult = null

    // apply the root function, this will give us the root vertex we will traverse from
    result = steps.rootFunc(pipeline, "workspaces", "Workspace1", rootName)
    pipelineQueryText += result.pipeAction

    // then if we have any pipes between root and last, apply each all but last pipes from this root
    steps.pipes.foreach(pipes => {
      for (step <- pipes) {
        result = step(pipeline)

        pipelineQueryText += result.pipeAction
      }
    })

    // apply the last action
    steps.lastAction match {
      // if there
      case Some(last) => {
        val finalResult = last(pipeline)
        pipelineQueryText += finalResult.pipeAction
        println("DEBUG: running query: " + pipelineQueryText)
        finalResult.result
      }
      // we had a root, but nothing else
      // TODO: if we are going to support just the root by itself, then we should return JSON representation for debugging expressions,
      // TODO: otherwise this should throw an exception if we always require some attribute of the root to be present
      case None => {
        pipelineQueryText += ".toList.map(pretty)"
        println("DEBUG: running query: " + pipelineQueryText)
        result.result.toList.map(short);//pretty)
      }
    }
  }

  def evaluate(expression:String)(rootName:String) = parseAll(path, expression) match {
    case Success(result, _) => {

      val value = applySteps("sampleSets", result)
      value
    }
    case NoSuccess(msg, next) => {
      println("Failed at line %s, column %s: %s".format(
        next.pos.line, next.pos.column, msg))
      println("On expression: " + next.source)

      //      println(expression.substring(0, next.pos.column-1) + "<" + expression.substring(next.pos.column-1, next.pos.column) + ">" + expression.substring(next.pos.column))
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
  // to make a less verbose type
  type PipeType = GremlinPipeline[Vertex, Vertex]

  type PipeFunc = PipeType => PipeResult
  // case class that also carries the string of what is added to the pipe (For debugging purposes)
  case class PipeResult(result:PipeType, pipeAction:String)

  type ResultFunc = PipeType => FinalResult
  // case class that also carries the string of what is added to the pipe (For debugging purposes)
  case class FinalResult(result:Seq[String], pipeAction:String)

  // types to clarify what a RootFunc takes
  type WorkspaceName = String
  type WorkspaceNamespace = String
  type RootName = String

  type RootFunc = (PipeType, WorkspaceNamespace, WorkspaceName, RootName) => PipeResult


  // a query goes from a root through pipes to the last action, which returns a Seq[String] of processed data from the requested Entities or attributes on entities
  case class PipelineQuery(rootFunc:RootFunc, pipes:Option[List[PipeFunc]], lastAction:Option[ResultFunc])


  /** syntax definition **/
  // this is the definition for just a simple entity.entity.attribute expression.  This type of expression may be found
  // in a more complex expression
  private def path:Parser[PipelineQuery] =
  // TODO: can we have just a root?
  // we expect just a root, or root followed by refs to other entities followed by an attribute of the entity
    root ^^ {
      case root  => PipelineQuery(root, None, None)
    } |
      // root.entity_name.other_entity_name.(...).some_attribute, where some_attribute could be an entity name as well
      rootDot ~ rep(entityRefName) ~ lastAttribute ^^ {
        case root ~ Nil ~ last => PipelineQuery(root, None, Option(last))
        case root ~ ref ~ last => PipelineQuery(root, Option(ref), Option(last))
      }

  // an entity reference will be followed by a dot for the attribute upon it
  private def entityRefName:Parser[PipeFunc] =
    ident <~ "." ^^
      {case name => entityNameAttributePipeFunc(name)}

  // the last attribute has no dot after it
  private def lastAttribute:Parser[ResultFunc] =
    ident ^^
      {case name => lastAttributePipeFunc(name)}

  // just root by itself with no refs or attributes
  private def root:Parser[RootFunc] =
    "root$".r ^^ {_ => rootFunc}
  // root followed by dot meaning it is to be followed by refs or attributes
  private def rootDot:Parser[RootFunc] =
    "root" ~ "." ^^ {_ => rootFunc}


  /** functions against pipes **/
  // get a vertex where the name matches the entity we are looking for
  private def entityFunc(entityName:String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(a: Vertex) = {
      propString(a, "_name") == entityName
    }
  }

  // the root function starts the pipeline at some root entity type in the workspace
  private def rootFunc(graphPipeline:PipeType, workspaceNamespace:WorkspaceNamespace, workspaceName:WorkspaceName, rootName:String):PipeResult = {
    def workspaceFunc = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(a: Vertex) = {
        propString(a, "_namespace") == workspaceNamespace && propString(a, "_name") == workspaceName
      }
    }

    PipeResult(
      // all vertexes of type Workspace filtered for the given namespace and name
      graphPipeline.V("_entityType", "Workspace").filter(workspaceFunc)
        // then entities from that workspace that match the root entity we are starting at
        .out().filter(entityFunc(rootName)),

      // text for what the pipeline looks like at this step
      s"""new GremlinPipeline(graph).V("_entityType", "Workspace").filter(workspaceFunc).out($rootName)"""
    )
  }

  // TODO: If the last attribute in the expression is a Vertex, we could return JSON representation of the vertex from the model
  private def lastAttributePipeFunc(attributeName:String)(pipe:PipeType):FinalResult = {
    val lastVertexes = pipe.toList

    // TODO: do we throw an exception here, or could someone say "root.foo.bar.some_attribute" and if foo or bar
    // TODO  don't exist we just return nothing?
    if(lastVertexes.size() == 0) {
      throw new RuntimeException("No vertexes were returned")
    }

    // check that every vertex returned has the property we queried for
    // TODO: or should it be that at least one has it?
    val hasProperty = lastVertexes.forall((v:Vertex) => v.getPropertyKeys.contains(attributeName))

    // TODO: should we throw an exception if both the attribute doesn't exist and the attribute does not represent an entity reference?
    // if we have the property we are looking for, then we will return a set of the property values,
    // otherwise we will see if the expression intended for the last attribute to mean a reference to another entity
    if (hasProperty) {
      FinalResult(lastVertexes.map((v:Vertex) => {propString(v, attributeName)}), s".getProperty($attributeName)")
    }
    else {
      // TODO: in this case would we want distinct?  for attributes would it be all, and in which case order probably matters?
      // TODO: for example sampleSets.sample.bam, we want ALL bams for all samples in all sampleSets, and order will be important, correct?
      // TODO:     This may have repeated values if not distinct, but that is likely the use case...
      // the expression ended in an attribute that was not a property, so we assume it meant an entity type referenced from this entity
      FinalResult(new PipeType(lastVertexes).out(attributeName).toList.distinct map(short), s".out($attributeName)")
    }
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(entityRefName:String)(pipe:PipeType):PipeResult = {
    PipeResult(
      // an entity name is on the outgoing edge label
      pipe.out(entityRefName),
      // TODO: is there a way to automate returning this code-string from the code itself? (used for debugging purposes)
      s".out($entityRefName)"
    )
  }
  /*****************************/

  // utility functions
  implicit def name(e:Edge):String = {
    e.getLabel
  }
  implicit def pretty(e:Edge):String = {
    name(e.getVertex(Direction.OUT)) + "---" + name(e) + "-->" + name(e.getVertex(Direction.IN))
  }
  implicit def name(v:Vertex):String = {
    v.getProperty("_name")
  }
  def propString(v:Vertex, property:String):String = {
    // TODO: why do we need the [Any] here?  If we don't have it we get casting exceptions from Scala...
    Option(v.getProperty[Any](property)) match {
      case Some(p) => p.toString
      case None => null
    }
  }
  implicit def pretty(v:Vertex):String = {
    val edges = v.getEdges(Direction.OUT)
    "Vertex: " + name(v) + ", keys: " + v.getPropertyKeys + (edges.size match {
      case 0 => ""
      case _ => " -> [edges: " + (edges.map(pretty).mkString(",")) + "]"
    })
  }
  implicit def short(v:Vertex):String = {
    "Vertex: " + name(v)
  }
}
