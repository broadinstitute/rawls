package org.broadinstitute.dsde.rawls.entities.base

import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, ParsedMCExpressions}

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


  // Syntax checker
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

  protected def parseInputExpr(allowRootEntity: Boolean)(expression: String): Try[Unit]

  protected def parseOutputExpr(allowRootEntity: Boolean)(expression: String): Try[Unit]

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
