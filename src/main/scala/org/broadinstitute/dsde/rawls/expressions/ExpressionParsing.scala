package org.broadinstitute.dsde.rawls.expressions

import java.lang.Iterable
import java.util

import com.tinkerpop.blueprints._
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction
import scala.collection.JavaConversions._
import scala.util.parsing.combinator.JavaTokenParsers

trait ExpressionTypes {
  // to make a less verbose type
  type PipeType = GremlinPipeline[Vertex, Vertex]

  // The output of a single step of the Gremlin pipe. pipeAction is the string representation, for debugging
  case class PipeResult(result: PipeType, pipeAction: String)

  // types to clarify what a RootFunc takes
  type WorkspaceName = String
  type WorkspaceNamespace = String
  type RootType = String
  type RootName = String

  // The initial step of a Gremlin pipe.
  type RootFunc = (PipeType, WorkspaceNamespace, WorkspaceName, RootType, RootName) => PipeResult

  // A single step of the Gremlin pipe.
  type PipeFunc = PipeType => PipeResult

  // The output of a completed Gremlin pipe; may return >1 result, hence Seq[String]. pipeAction is the step's string representation, for debugging
  case class FinalResult(result: Seq[Any], pipeAction: String)

  // The terminal step of a Gremlin pipe.
  type ResultFunc = PipeType => FinalResult

  // a query goes from a root through pipes to the last action, which returns a Seq[String] of processed data from the requested Entities or attributes on entities
  case class PipelineQuery(rootFunc: RootFunc, pipes: Option[List[PipeFunc]], lastAction: Option[ResultFunc])

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

class ExpressionParser extends JavaTokenParsers with ExpressionTypes {
  /** Parser definition **/
  // Entity expressions take the general form entity.ref.ref.attribute.
  // For now, we expect the initial entity to be the special token "this", which is bound at evaluation time to a root entity.
  private def path: Parser[PipelineQuery] = {
    /* TODO: For now, only supporting entity expressions that end in attribute.
  // The simplest case: just return the root entity
    root ^^ {
      case root => PipelineQuery(root, None, None)
    } |
  */
    // root.ref.(...).attribute
    rootDot ~ rep(entityRefName) ~ lastAttribute ^^ {
      case root ~ Nil ~ last => PipelineQuery(root, None, Option(last))
      case root ~ ref ~ last => PipelineQuery(root, Option(ref), Option(last))
    }
  }

  // just root by itself with no refs or attributes
  private def root: Parser[RootFunc] =
    "this$".r ^^ { _ => rootFunc}

  // root followed by dot meaning it is to be followed by refs or attributes
  private def rootDot: Parser[RootFunc] =
    "this." ^^ { _ => rootFunc}

  // an entity reference will be followed by a dot for the attribute upon it
  private def entityRefName: Parser[PipeFunc] =
    ident <~ "." ^^ { case name => entityNameAttributePipeFunc(name)}

  // the last attribute has no dot after it
  private def lastAttribute: Parser[ResultFunc] =
    ident ^^ { case name => lastAttributePipeFunc(name)}


  def parse(expression: String)(workspaceNamespace: String, workspaceName: String, rootType: String, rootName: String) = {
    //Attempt to parse the expression into a pipeline query to hand off to Gremlin
    parseAll(path, expression) match {
      case Success(result, _) => {
        // TODO: the creation of the pipeline(s) needs to occur at the parser level because we could have multiple statements
        // TODO: that we need to evalulate using separate pipelines
        // result is a list of functions to apply to the pipeline in order

        Some(result)
      }
      case NoSuccess(msg, next) => {
        println("Failed at line %s, column %s: %s".format(
          next.pos.line, next.pos.column, msg))
        println("On expression: " + next.source)

        None
      }
    }
  }

  /** functions against pipes **/

  // the root function starts the pipeline at some root entity type in the workspace
  private def rootFunc(graphPipeline: PipeType, workspaceNamespace: WorkspaceNamespace, workspaceName: WorkspaceName, rootType: String, rootName: String): PipeResult = {
    def workspaceFunc = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(a: Vertex) = {
        propString(a, "_namespace") == workspaceNamespace && propString(a, "_name") == workspaceName
      }
    }

    PipeResult(
      // all vertexes of type Workspace filtered for the given namespace and name
      graphPipeline.V("_entityType", "Workspace").filter(workspaceFunc)
        // then entities from that workspace that match the root entity we are starting at
        .out(rootType).filter(entityFunc(rootName)),

      // text for what the pipeline looks like at this step
      s"""new GremlinPipeline(graph).V("_entityType", "Workspace").filter(workspaceFunc).out($rootType,$rootName)"""
    )
  }

  // get a vertex where the name matches the entity we are looking for
  private def entityFunc(entityName: String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(a: Vertex) = {
      propString(a, "_name") == entityName
    }
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(entityRefName: String)(pipe: PipeType): PipeResult = {
    PipeResult(
      // an entity name is on the outgoing edge label
      pipe.out(entityRefName),
      // TODO: is there a way to automate returning this code-string from the code itself? (used for debugging purposes)
      s".out($entityRefName)"
    )
  }

  // TODO: We only support attributes here, not reference types (edges)
  private def lastAttributePipeFunc(attributeName: String)(pipe: PipeType): FinalResult = {
    val lastVertices = pipe.toList

    if (lastVertices.size() == 0) {
      throw new RuntimeException(s"Could not dereference $attributeName because pipe returned no entities")
    }

    // Look up the attributes on the list of vertices returned from the pipe. This will insert nulls into the list if
    // the attribute doesn't exist.
    FinalResult(lastVertices.map((v: Vertex) => {
      v.getProperty(attributeName)
    }), s".getProperty($attributeName)")
  }
}

class ExpressionEvaluator(graph:Graph) extends ExpressionTypes {
  def runPipe(workspaceNamespace:String, workspaceName:String, rootType:String, rootName:String, steps:PipelineQuery):Seq[Any] = {
    val pipeline = new PipeType(graph)

    // TODO: more idiomatic way to do this?  the GremlinPipeline has side effects, and adds pipes to each .out() and similar calls anyway, so maybe not...
    var pipelineQueryText = ""
    var result:PipeResult = null

    // apply the root function, this will give us the root vertex we will traverse from
    result = steps.rootFunc(pipeline, workspaceNamespace, workspaceName, rootType, rootName)
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
      // evaluate root-only expression
      case None => {
        pipelineQueryText += ".toList.map(pretty)"
        println("DEBUG: running query: " + pipelineQueryText)
        result.result.toList.map(short);//pretty)
      }
    }
  }
}
