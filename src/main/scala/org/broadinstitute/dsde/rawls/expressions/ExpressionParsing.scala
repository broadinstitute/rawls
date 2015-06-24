package org.broadinstitute.dsde.rawls.expressions

import com.tinkerpop.blueprints._
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction
import org.broadinstitute.dsde.rawls.expressions
import org.broadinstitute.dsde.rawls.model.Workspace
import scala.collection.JavaConversions._
import scala.util.{Try, Failure, Success}
import scala.util.parsing.combinator
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.Reader

object ExpressionTypes {

  // to make a less verbose type
  type PipeType = GremlinPipeline[Vertex, Vertex]

  case class ExpressionContext(workspaceNamespace: String, workspaceName: String, rootType: String, rootName: String)

  //A fully built query is just a list of functions that can be foldl'd.
  case class PipelineQuery(steps: List[PipeFunc], finalStep: FinalFunc)

  // The first n steps of a pipe return pipes. pipeAction is the string representation, for debugging
  type PipeFunc = (ExpressionContext, PipeType) => PipeResult
  case class PipeResult(result: PipeType, pipeAction: String)

  // The final step of a pipe may return >1 result, hence Seq[Any]. pipeAction is the step's string representation, for debugging
  type FinalFunc = (ExpressionContext, PipeType) => FinalResult
  case class FinalResult(result: Seq[Any], pipeAction: String)

  // utility functions
  def name(e:Edge):String = {
    e.getLabel
  }
  def pretty(e:Edge):String = {
    name(e.getVertex(Direction.OUT)) + "---" + name(e) + "-->" + name(e.getVertex(Direction.IN))
  }
  def name(v:Vertex):String = {
    v.getProperty("_name").asInstanceOf[String]
  }
  def propString(v:Vertex, property:String):String = {
    // TODO: why do we need the [Any] here?  If we don't have it we get casting exceptions from Scala...
    Option(v.getProperty[Any](property)) match {
      case Some(p) => p.toString
      case None => null
    }
  }
  def pretty(v:Vertex):String = {
    val edges = v.getEdges(Direction.OUT)
    "Vertex: " + name(v) + ", keys: " + v.getPropertyKeys + (edges.size match {
      case 0 => ""
      case _ => " -> [edges: " + (edges.map(pretty).mkString(",")) + "]"
    })
  }
  def short(v:Vertex):String = {
    "Vertex: " + name(v)
  }
}

import ExpressionTypes._

class ExpressionParser extends JavaTokenParsers {
  /** Parser definition **/
  // Entity expressions take the general form entity.ref.ref.attribute.
  // For now, we expect the initial entity to be the special token "this", which is bound at evaluation time to a root entity.
  private def path: Parser[PipelineQuery] = {
    /* TODO: For now, only supporting entity expressions that end in attribute.
  // The simplest case: just return the root entity
    root ^^ {
      case root => (List(root), outputEntityResult)
    } |
  */
    // root.ref.(...).attribute

    rootDot ~ rep(entityRefName) ~ lastAttribute ^^ {
      case root ~ Nil ~ last => PipelineQuery(List(root), last)
      case root ~ ref ~ last => PipelineQuery(List(root) ++ ref, last)
    }
  }

  // just root by itself with no refs or attributes
  private def root: Parser[PipeFunc] =
    "this$".r ^^ { _ => rootFunc}

  // root followed by dot meaning it is to be followed by refs or attributes
  private def rootDot: Parser[PipeFunc] =
    "this." ^^ { _ => rootFunc}

  // an entity reference will be followed by a dot for the attribute upon it
  private def entityRefName: Parser[PipeFunc] =
    ident <~ "." ^^ { case name => entityNameAttributePipeFunc(name)}

  // the last attribute has no dot after it
  private def lastAttribute: Parser[FinalFunc] =
    ident ^^ { case name => lastAttributePipeFunc(name)}


  def parse(expression: String) = {
    //Attempt to parse the expression into a pipeline query to hand off to Gremlin
    //TODO: add caching here? Or move the creation of Gremlin pipelines here so we can cache those?
    parseAll(path, expression) match {
      case Success(result, _) => {
        scala.util.Success(result)
      }
      case NoSuccess(msg, next) => {
        scala.util.Failure(new RuntimeException("Failed at line %s, column %s: %s".format(next.pos.line, next.pos.column, msg)))
      }
    }
  }

  /** functions against pipes **/

  // the root function starts the pipeline at some root entity type in the workspace
  private def rootFunc(context: ExpressionContext, graphPipeline: PipeType): PipeResult = {
    def workspaceFunc = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(a: Vertex) = {
        propString(a, "_namespace") == context.workspaceNamespace && propString(a, "_name") == context.workspaceName
      }
    }

    def entityFunc = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(a: Vertex) = {
        propString(a, "_name") == context.rootName
      }
    }

    PipeResult(
      // all vertexes of type Workspace filtered for the given namespace and name
      graphPipeline.V("_clazz", classOf[Workspace].getSimpleName).filter(workspaceFunc)
        // then entities from that workspace that match the root entity we are starting at
        .out(context.rootType).filter(entityFunc),

      // text for what the pipeline looks like at this step
      s"""new GremlinPipeline(graph).V("_entityType", "Workspace").filter(workspaceFunc).out(${context.rootType},${context.rootName})"""
    )
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(entityRefName: String)(context: ExpressionContext, graphPipeline: PipeType): PipeResult = {
    PipeResult(
      // an entity name is on the outgoing edge label
      graphPipeline.out(entityRefName),
      s".out($entityRefName)"
    )
  }

  //TODO: Needs implementation! Return false if reference type.
  private def isValueType(attributeName: String): Boolean = { true }

  private def lastAttributePipeFunc(attributeName: String)(context: ExpressionContext, graphPipeline: PipeType): FinalResult = {
    //The below will very likely change. My suspicion is that we can't tell whether the attribute is a value
    //or reference type without evaluating the pipe passed in and then seeing which of .getProperty or .out works.
    //Particularly perverse graphs might have a mixture of both! (We should handle such cases cleanly, for some value of "clean".)
    if (isValueType(attributeName)) {
      val lastVertices = graphPipeline.toList

      if (lastVertices.size() == 0) {
        throw new RuntimeException(s"Could not dereference $attributeName because pipe returned no entities")
      }

      // Look up the attributes on the list of vertices returned from the pipe. This will insert nulls into the list if
      // the attribute doesn't exist.
      FinalResult(lastVertices.map((v: Vertex) => {
        v.getProperty(attributeName).asInstanceOf[Object]
      }), s".getProperty($attributeName)")
    } else {
      outputEntityResult( context, graphPipeline.out(attributeName) )
    }
  }


  //Takes a list of entities at the end of a pipeline and returns them in final format.
  //TODO: Determine how we want to return these entities. GraphEntityDAO.loadEntity?
  private def outputEntityResult(context: ExpressionContext, graphPipeline: PipeType): FinalResult = {
    //graphPipeline.toList.map( some_conversion_to_something_else )
    FinalResult(List(), "")
  }
}

class ExpressionEvaluator(graph:Graph, parser:ExpressionParser)  {
  def evaluate(workspaceNamespace:String, workspaceName:String, rootType:String, rootName:String, expression:String):Try[Seq[Any]] = {
    parser.parse(expression) match {
      case Success(pipe) => runPipe( ExpressionContext(workspaceNamespace, workspaceName, rootType, rootName), pipe)
      case Failure(regret) => Failure(regret)
    }
  }

  def runPipe(expressionContext: ExpressionContext, pipe:PipelineQuery):Try[Seq[Any]] = {
    val gremlin = new PipeType(graph)

    //Build the gremlin pipeline up to the penultimate step.
    val builtPipe = pipe.steps.foldLeft(PipeResult(gremlin, "")){ ( graph, func ) => func(expressionContext, graph.result) }

    //Run the final step. This executes the pipeline and returns its output.
    Try( pipe.finalStep( expressionContext, builtPipe.result ) ) match {
      case Success(finalResult) => Success(finalResult.result)
      case Failure(regret) => Failure(regret)
    }
  }
}
