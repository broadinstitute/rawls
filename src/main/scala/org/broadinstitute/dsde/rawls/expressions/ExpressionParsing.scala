package org.broadinstitute.dsde.rawls.expressions

import java.lang

import com.tinkerpop.blueprints._
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.{Pipe, PipeFunction}
import com.tinkerpop.pipes.branch.LoopPipe
import org.broadinstitute.dsde.rawls.dataaccess.{VertexSchema, GraphEntityDAO}
import org.broadinstitute.dsde.rawls.expressions
import org.broadinstitute.dsde.rawls.model.{Entity, AttributeConversions, AttributeValue, Workspace}
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
    v.getProperty("name").asInstanceOf[String]
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
  /** Parser definitions **/
  // Entity expressions take the general form entity.ref.ref.attribute.
  // For now, we expect the initial entity to be the special token "this", which is bound at evaluation time to a root entity.

  //Parser for expressions ending in a value (literal) attribute
  private def attributeExpression: Parser[PipelineQuery] = {
    rootDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case root ~ Nil ~ last => PipelineQuery(List(root), last)
      case root ~ ref ~ last => PipelineQuery(List(root) ++ ref, last)
    }
  }

  //Parser for expressions ending in an attribute that's a reference to another entity
  private def entityExpression: Parser[PipelineQuery] = {
    root ^^ {
      case root => PipelineQuery(List(root), outputEntityResult)
    } |
    rootDot ~ rep(entityRefDot) ~ entityRef ^^ {
      case root ~ Nil ~ last => PipelineQuery(List(root) :+ last, outputEntityResult)
      case root ~ ref ~ last => PipelineQuery(List(root) ++ ref :+ last, outputEntityResult)
    }
  }

  // just root by itself with no refs or attributes
  private def root: Parser[PipeFunc] =
    "this$".r ^^ { _ => rootFunc}

  // root followed by dot meaning it is to be followed by refs or attributes
  private def rootDot: Parser[PipeFunc] =
    "this." ^^ { _ => rootFunc}

  // an entity reference as the final attribute in an expression
  private def entityRef: Parser[PipeFunc] =
    ident ^^ { case entity => entityNameAttributePipeFunc(entity)}

  // an entity reference in the middle of an expression
  private def entityRefDot: Parser[PipeFunc] =
    ident <~ "." ^^ { case name => entityNameAttributePipeFunc(name)}

  // the last attribute has no dot after it
  private def valueAttribute: Parser[FinalFunc] =
    ident ^^ { case name => lastAttributePipeFunc(name)}

  def parseAttributeExpr(expression: String) = {
    parse(expression, attributeExpression)
  }

  def parseEntityExpr(expression: String) = {
    parse(expression, entityExpression)
  }

  private def parse(expression: String, parser: Parser[PipelineQuery] ) = {
    //Attempt to parse the expression into a pipeline query to hand off to Gremlin
    //TODO: add caching here? Or move the creation of Gremlin pipelines here so we can cache those?
    parseAll(parser, expression) match {
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
        a.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.Workspace) &&
          propString(a, "namespace") == context.workspaceNamespace && propString(a, "name") == context.workspaceName
      }
    }

    def entityFunc = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(a: Vertex) = {
        a.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.Entity) &&
        propString(a, "name") == context.rootName
      }
    }

    PipeResult(
      // all vertexes of type Workspace filtered for the given namespace and name
      graphPipeline.V().filter(workspaceFunc)
        // then entities from that workspace that match the root entity we are starting at
        .out(context.rootType).filter(entityFunc),

      // text for what the pipeline looks like at this step
      // TODO make this less prone to breaking when our graph structure changes
      s"""new GremlinPipeline(graph).V().filter(workspaceFunc).out(${context.rootType}).filter(entityFunc)"""
    )
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(entityRefName: String)(context: ExpressionContext, graphPipeline: PipeType): PipeResult = {
    def whileNotEntityFunc = new PipeFunction[LoopPipe.LoopBundle[Vertex], java.lang.Boolean] {
      override def compute(bundle: LoopPipe.LoopBundle[Vertex]): java.lang.Boolean = {
        bundle.getObject.asInstanceOf[OrientVertex].getRecord.getClassName != VertexSchema.Entity
      }
    }

    def isVertexOfClass(clazz: String) = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(v: Vertex) = v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(clazz)
    }

    def entityRefPipe(entityRefName: String) = new GremlinPipeline[Vertex, Vertex]().out(entityRefName)
    def entityRefListPipe(entityRefName: String) = entityRefPipe(entityRefName).as("listOut").out().loop("listOut", whileNotEntityFunc)

    PipeResult(
      // an reference name is on the outgoing edge label
      // if it is a single reference, the vertex is on the other end
      // if it's a List-type reference, there are some vertices in the middle so loop until you find entity classes
      // then merge both steps together
      graphPipeline.out("attributes").copySplit(entityRefPipe(entityRefName), entityRefListPipe(entityRefName)).exhaustMerge().asInstanceOf[GremlinPipeline[Vertex, Vertex]].filter(isVertexOfClass(VertexSchema.Entity)),
      s""".out("attributes").copySplit(entityRefPipe(entityRefName), entityRefListPipe(entityRefName)).exhaustMerge().filter(isVertexOfClass(VertexSchema.Entity))"""
    )
  }

  private def lastAttributePipeFunc(attributeName: String)(context: ExpressionContext, graphPipeline: PipeType): FinalResult = {
    val lastVertices = graphPipeline.toList

    if (lastVertices.isEmpty) {
      throw new RuntimeException(s"Could not dereference $attributeName because pipe returned no entities")
    }

    // Look up the attributes on the list of vertices returned from the pipe. This will insert nulls into the list if
    // the attribute doesn't exist.
    // Note that this requires traversing one additional time, from the "last" vertices to their subordinate attribute map vertices.
    FinalResult(
      lastVertices.map((v: Vertex) => {
        v.getVertices(Direction.OUT, "attributes").headOption match {
          case Some(mapVertex) => mapVertex.getProperty(attributeName).asInstanceOf[Object]
          case None => throw new RuntimeException("Boo hoo, what should I do")
        }
      }),
      s""".out("attributes").getProperty($attributeName)"""
    )
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def outputEntityResult(context: ExpressionContext, graphPipeline: PipeType): FinalResult = {
    val dao = new GraphEntityDAO()
    FinalResult( graphPipeline.toList.map( dao.loadEntity(_, context.workspaceNamespace, context.workspaceName) ), "" )
  }
}

class ExpressionEvaluator(graph:Graph, parser:ExpressionParser)  {
  def evalFinalAttribute(workspaceNamespace:String, workspaceName:String, rootType:String, rootName:String, expression:String):Try[Seq[AttributeValue]] = {
    parser.parseAttributeExpr(expression)
      .flatMap( runPipe(ExpressionContext(workspaceNamespace, workspaceName, rootType, rootName), _) )
      .flatMap( ls => Success(ls.map(AttributeConversions.propertyToAttribute(_))) )
  }

  def evalFinalEntity(workspaceNamespace:String, workspaceName:String, rootType:String, rootName:String, expression:String):Try[Seq[Entity]] = {
    parser.parseEntityExpr(expression)
      .flatMap( runPipe(ExpressionContext(workspaceNamespace, workspaceName, rootType, rootName), _) )
      .flatMap( ls => Success(ls.map(_.asInstanceOf[Entity])) )
  }

  def runPipe(expressionContext: ExpressionContext, pipe:PipelineQuery):Try[Seq[Any]] = {
    val gremlin = new PipeType(graph)

    //Build the gremlin pipeline up to the penultimate step.
    val builtPipe = pipe.steps.foldLeft(PipeResult(gremlin, "")){ ( graph, func ) => func(expressionContext, graph.result) }

    //Run the final step. This executes the pipeline and returns its output.
    Try {
      pipe.finalStep( expressionContext, builtPipe.result )
    } match {
      case Success(finalResult) => Success(finalResult.result)
      case Failure(regret) => Failure(regret)
    }
  }
}
