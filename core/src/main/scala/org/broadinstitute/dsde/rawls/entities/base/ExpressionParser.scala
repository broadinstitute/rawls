package org.broadinstitute.dsde.rawls.entities.base

import cats.instances.try_._
import cats.syntax.functor._
import org.broadinstitute.dsde.rawls.model.{Attributable, AttributeName, AttributeString, ParsedMCExpressions}
import spray.json._

import scala.language.{higherKinds, postfixOps}
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

/**
  *
  * @tparam F effect type
  * @tparam ExpressionContext a context object that a particular implementation can use for context things
  * @tparam PipeType a query that results in the root entity's name, entity records and a sort ordering
  */
trait ExpressionParser[F[_], ExpressionContext, PipeType] extends JavaTokenParsers {
  // A parsed expression will result in a PipelineQuery. Each step in a query traverses from entity to
  // entity following references. The final step takes the last entity, producing a result dependent on the query
  // (e.g. loading the entity itself, getting an attribute). The root step produces an entity to start the pipeline.
  // If rootStep is None, steps should also be empty and finalStep does all the work.
  case class PipelineQuery(rootStep: Option[RootFunc], steps: List[PipeFunc], finalStep: FinalFunc)

  // starting with just an expression context produces a PipeResult
  type RootFunc = (ExpressionContext) => PipeType

  // extends the input PipeType producing a PipeType that traverses further down the chain
  type PipeFunc = (ExpressionContext, PipeType) => PipeType

  // converts the incoming PipeType into the appropriate db action
  // PipeType may be None when there is no pipeline (e.g. workspace attributes)
  // Returns a map of entity names to an iterable of the expression result
  type FinalFunc = (ExpressionContext, Option[PipeType]) => ExpressionOutputType
  
  type ExpressionOutputType = F[Map[String, Iterable[Any]]]

  def parseMCExpressions(inputs: Map[String, AttributeString], outputs: Map[String, AttributeString], allowRootEntity: Boolean): ParsedMCExpressions = {
    val noEntityAllowedErrorMsg = "Expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."
    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[Unit] ) = {
      val parsed = m map { case (key, attr) => (key, parseFunc(attr.value)) }
      ( parsed collect { case (key, scala.util.Success(_)) => key } toSet,
        parsed collect { case (key, scala.util.Failure(regret)) =>
          if (!allowRootEntity && m.get(key).isDefined && m.get(key).get.value.startsWith("this."))
            (key, noEntityAllowedErrorMsg)
          else
            (key, regret.getMessage)}
      )
    }

    val (successInputs, failedInputs)   = parseAndPartition(inputs, parseInputExpr(allowRootEntity) )
    val (successOutputs, failedOutputs) = parseAndPartition(outputs, parseOutputExpr(allowRootEntity) )

    ParsedMCExpressions(successInputs, failedInputs, successOutputs, failedOutputs)
  }

  private def parseInputExpr(allowRootEntity: Boolean)(expression: String): Try[Unit] = {
    // JSON expressions are valid inputs and do not need to be parsed
    Try(expression.parseJson).recoverWith { case _ => parseAttributeExpr(expression, allowRootEntity) }.void
  }

  private def parseOutputExpr(allowRootEntity: Boolean)(expression: String): Try[Unit] = {
    parseOutputAttributeExpr(expression, allowRootEntity).void
  }

  /** Parser definitions **/
  // Entity expressions take the general form entity.ref.ref.attribute.
  // For now, we expect the initial entity to be the special token "this", which is bound at evaluation time to a root entity.

  //Parser for expressions ending in an attribute value (not an entity reference)
  private def attributeExpression(allowRootEntity: Boolean): Parser[PipelineQuery] = {

    // the basic case: this.(ref.)*attribute
    val entityExpr = entityRootDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case root ~ Nil ~ last => PipelineQuery(Option(root), List.empty, last)
      case root ~ ref ~ last => PipelineQuery(Option(root), ref, last)
    }

    // attributes at the end of a reference chain starting at a workspace: workspace.ref.(ref.)*attribute
    val workspaceExpr = workspaceEntityRefDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case workspace ~ Nil ~ last => PipelineQuery(Option(workspace), List.empty, last)
      case workspace ~ ref ~ last => PipelineQuery(Option(workspace), ref, last)
    } |
      // attributes directly on a workspace: workspace.attribute
      workspaceAttribute ^^ {
        case workspace => PipelineQuery(None, List.empty, workspace)
      }

    if(allowRootEntity) {
      entityExpr | workspaceExpr
    } else {
      workspaceExpr
    }
  }

  //Parser for output expressions: this.attribute or workspace.attribute (no entity references in the middle)
  private def outputAttributeExpression(allowRootEntity: Boolean): Parser[PipelineQuery] = {
    // this.attribute
    val entityOutput = entityRootDot ~ valueAttribute ^^ {
      case root ~ attr => PipelineQuery(Option(root), List.empty, attr)
    }
    // workspace.attribute
    val workspaceOutput = workspaceAttribute ^^ {
      case workspace => PipelineQuery(None, List.empty, workspace)
    }

    if(allowRootEntity) {
      entityOutput | workspaceOutput
    } else {
      workspaceOutput
    }
  }

  //Parser for expressions ending in an attribute that's a reference to another entity
  private def entityExpression: Parser[PipelineQuery] = {
    // reference IS the entity: this
    entityRoot ^^ {
      case root => PipelineQuery(Option(root), List.empty, entityFinalFunc _)
    } |
      // reference chain starting with an entity: this.(ref.)*ref
      entityRootDot ~ rep(entityRefDot) ~ entityRef ^^ {
        case root ~ Nil ~ last => PipelineQuery(Option(root), List(last), entityFinalFunc)
        case root ~ ref ~ last => PipelineQuery(Option(root), ref :+ last, entityFinalFunc)
      } |
      // reference chain starting with the workspace: workspace.(ref.)*ref
      workspaceEntityRefDot ~ rep(entityRefDot) ~ entityRef ^^ {
        case workspace ~ Nil ~ last => PipelineQuery(Option(workspace), List(last), entityFinalFunc)
        case workspace ~ ref ~ last => PipelineQuery(Option(workspace), ref :+ last, entityFinalFunc)
      } |
      // reference directly off the workspace: workspace.ref
      workspaceEntityRef ^^ {
        case workspace => PipelineQuery(None, List.empty, workspace)
      }
  }

  // just root by itself with no refs or attributes
  private def entityRoot: Parser[RootFunc] =
    "this$".r ^^ { _ => entityRootFunc}

  // root followed by dot meaning it is to be followed by refs or attributes
  private def entityRootDot: Parser[RootFunc] =
    "this." ^^ { _ => entityRootFunc}

  // matches some_attr_namespace:attr_name or attr_name
  private val attributeIdent: Parser[AttributeName] = opt(ident ~ ":") ~ ident ^^ {
    case Some(namespace ~ ":") ~ name => AttributeName(namespace, name)
    case _ ~ name => AttributeName.withDefaultNS(name)
  }

  // workspace.attribute, note that this is a FinalFunc - because workspaces are not entities they can be piped the same way
  private def workspaceAttribute: Parser[FinalFunc] =
    "workspace." ~> attributeIdent ^^ {
      case attrName =>
        if (Attributable.reservedAttributeNames.contains(attrName.name)) workspaceReservedAttributeFinalFunc(attrName.name)
        else workspaceAttributeFinalFunc(attrName)
    }

  // an entity reference as the final attribute in an expression
  private def entityRef: Parser[PipeFunc] =
    attributeIdent ^^ { case entityRef => entityNameAttributePipeFunc(entityRef)}

  // an entity reference in the middle of an expression
  private def entityRefDot: Parser[PipeFunc] =
    attributeIdent <~ "." ^^ { case entityRef => entityNameAttributePipeFunc(entityRef)}

  // an entity reference after the workspace, this can be a RootFunc because it resolves to an entity query that can be piped
  private def workspaceEntityRefDot: Parser[RootFunc] =
    "workspace." ~> attributeIdent <~ "." ^^ { case entityRef => workspaceEntityRefRootFunc(entityRef)}

  // an entity reference after the workspace, note that this is a FinalFunc - because workspaces are not entities they can be piped the same way
  private def workspaceEntityRef: Parser[FinalFunc] =
    "workspace." ~> attributeIdent ^^ { case entityRef => workspaceEntityFinalFunc(entityRef)}

  // the last attribute has no dot after it
  private def valueAttribute: Parser[FinalFunc] =
    attributeIdent ^^ {
      case attrName =>
        if (Attributable.reservedAttributeNames.contains(attrName.name)) entityReservedAttributeFinalFunc(attrName.name)
        else entityAttributeFinalFunc(attrName)
    }

  def parseAttributeExpr(expression: String, allowRootEntity: Boolean): Try[PipelineQuery] = {
    parse(expression, attributeExpression(allowRootEntity))
  }

  def parseOutputAttributeExpr(expression: String, allowRootEntity: Boolean): Try[PipelineQuery] = {
    parse(expression, outputAttributeExpression(allowRootEntity))
  }

  def parseEntityExpr(expression: String): Try[PipelineQuery] = {
    parse(expression, entityExpression)
  }

  private def parse(expression: String, parser: Parser[PipelineQuery] ) = {
    parseAll(parser, expression) match {
      case Success(result, _) => scala.util.Success(result)
      case NoSuccess(msg, next) => scala.util.Failure(new RuntimeException("Failed at line %s, column %s: %s".format(next.pos.line, next.pos.column, msg)))
    }
  }

  /** functions against pipes **/

  // the root function starts the pipeline at some root entity type in the workspace
  protected def entityRootFunc(context: ExpressionContext): PipeType

  // final func that gets an attribute off a workspace
  protected def workspaceAttributeFinalFunc(attrName: AttributeName)(context: ExpressionContext, shouldBeNone: Option[PipeType]): ExpressionOutputType

  // root func that gets an entity reference off a workspace
  protected def workspaceEntityRefRootFunc(attrName: AttributeName)(context: ExpressionContext): PipeType

  // add pipe to an entity referenced by the current entity
  protected def entityNameAttributePipeFunc(attrName: AttributeName)(context: ExpressionContext, queryPipeline: PipeType): PipeType

  // filter attributes to only the given attributeName and convert to attribute
  // Return a map from the entity names to the list of attribute values for each entity
  protected def entityAttributeFinalFunc(attrName: AttributeName)(context: ExpressionContext, queryPipeline: Option[PipeType]): ExpressionOutputType

  // final func that handles reserved attributes on an entity
  protected def entityReservedAttributeFinalFunc(attributeName: String)(context: ExpressionContext, queryPipeline: Option[PipeType]): ExpressionOutputType

  // final func that handles reserved attributes on a workspace
  protected def workspaceReservedAttributeFinalFunc(attributeName: String)(context: ExpressionContext, shouldBeNone: Option[PipeType]): ExpressionOutputType

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  protected def entityFinalFunc(context: ExpressionContext, queryPipeline: Option[PipeType]): ExpressionOutputType

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  protected def workspaceEntityFinalFunc(attrName: AttributeName)(context: ExpressionContext, shouldBeNone: Option[PipeType]): ExpressionOutputType
}
