package org.broadinstitute.dsde.rawls.expressions

import _root_.slick.dbio
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.entities.local.LocalEntityExpressionContext
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.AntlrTerraExpressionParser.toAttributeName
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser._
import org.broadinstitute.dsde.rawls.model._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

// accessible only via ExpressionEvaluator

private[expressions] object SlickExpressionEvaluator {
  def withNewExpressionEvaluator[R](dataAccess: DataAccess, rootEntities: Option[Seq[EntityRecord]])(
    op: SlickExpressionEvaluator => ReadWriteAction[R]
  )(implicit executionContext: ExecutionContext): ReadWriteAction[R] = {
    val evaluator = new SlickExpressionEvaluator(dataAccess, rootEntities)

    evaluator.populateExprEvalScratchTable() andThen
      op(evaluator) andFinally
      evaluator.clearExprEvalScratchTable()
  }

  def withNewExpressionEvaluator[R](dataAccess: DataAccess,
                                    workspaceContext: Workspace,
                                    rootType: String,
                                    rootName: String
  )(
    op: SlickExpressionEvaluator => ReadWriteAction[R]
  )(implicit executionContext: ExecutionContext): ReadWriteAction[R] = {
    import dataAccess.driver.api._

    // Find the root entity for the expression
    val dbRootEntityRec =
      dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceIdAsUUID, rootType, rootName).result

    // Sanity check that we've only got one, and then pass upwards
    dbRootEntityRec flatMap { rootEntityRec =>
      if (rootEntityRec.size != 1) {
        DBIO.failed(
          new RawlsException(
            s"Expected 1 root entity type, found ${rootEntityRec.size} when searching for $rootType/$rootName"
          )
        )
      } else {
        withNewExpressionEvaluator(dataAccess, Some(rootEntityRec))(op)
      }
    }
  }
}

private[expressions] class SlickExpressionEvaluator protected (val dataAccess: DataAccess,
                                                               val rootEntities: Option[Seq[EntityRecord]]
)(implicit executionContext: ExecutionContext) {
  import dataAccess.driver.api._

  val transactionId = UUID.randomUUID().toString

  private def populateExprEvalScratchTable() = {
    val exprEvalBatches = rootEntities
      .getOrElse(Seq.empty[EntityRecord])
      .map(e => ExprEvalRecord(e.id, e.name, transactionId))
      .grouped(dataAccess.batchSize)

    DBIO.sequence(exprEvalBatches.toSeq.map(batch => dataAccess.exprEvalQuery ++= batch))
  }

  private def clearExprEvalScratchTable() =
    dataAccess.exprEvalQuery.filter(_.transactionId === transactionId).delete

  // A parsed expression will result in a ExpressionEvaluationPipeline. Each step traverses from entity to
  // entity following references. The final step takes the last entity, producing a result dependent on the query
  // (e.g. loading the entity itself, getting an attribute). The root step produces an entity to start the pipeline.
  // If rootStep is None, steps should also be empty and finalStep does all the work.
  case class ExpressionEvaluationPipeline(rootStep: Option[RootFunc],
                                          relationSteps: List[RelationFunc],
                                          finalStep: FinalFunc
  )

  // starting with just an expression context produces a PipelineStepQuery
  type RootFunc = (LocalEntityExpressionContext) => dataAccess.PipelineStepQuery

  // extends the input PipelineStepQuery producing a PipelineStepQuery that traverses further down the chain
  type RelationFunc = (LocalEntityExpressionContext, dataAccess.PipelineStepQuery) => dataAccess.PipelineStepQuery

  // converts the incoming PipelineStepQuery into the appropriate db action
  // PipelineStepQuery may be None when there is no pipeline (e.g. workspace attributes)
  // Returns a map of entity names to an iterable of the expression result
  type FinalFunc = (LocalEntityExpressionContext, Option[dataAccess.PipelineStepQuery]) => ExpressionOutputType

  // key is the entity name, value is the result of the expression for said entity
  type ExpressionOutputType = ReadAction[Map[String, Iterable[Any]]]

  /** Final attribute lookup functions */
  // the basic case: this.(ref.)*.attribute
  def evalEntityLookupFinalAttribute(workspaceContext: Workspace,
                                     entityLookupContext: EntityLookupContext
  ): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] =
    evalFinalAttribute(workspaceContext, buildAttributeQueryForEntity(entityLookupContext))

  // attributes at the end of a reference chain starting at a workspace: workspace.ref.(ref.)*.attribute
  def evalWorkspaceEntityLookupFinalAttribute(workspaceContext: Workspace,
                                              workspaceEntityLookupContext: WorkspaceEntityLookupContext
  ): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] =
    evalFinalAttribute(workspaceContext, buildAttributeQueryForWorkspaceEntity(workspaceEntityLookupContext))

  // attributes directly on a workspace: workspace.attribute
  def evalWorkspaceAttributeLookupFinalAttribute(workspaceContext: Workspace,
                                                 workspaceAttributeLookupContext: WorkspaceAttributeLookupContext
  ): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] =
    evalFinalAttribute(workspaceContext, buildAttributeQueryForWorkspaceAttribute(workspaceAttributeLookupContext))

  /** Final entity lookup functions */
  // reference chain starting with an entity: this.(ref.)*ref OR reference IS the entity: this
  def evalEntityLookupFinalEntity(workspace: Workspace,
                                  entityLookupContext: EntityLookupContext
  ): ReadWriteAction[Iterable[EntityRecord]] =
    evalFinalEntity(workspace, buildEntityReferenceQueryForEntity(entityLookupContext))

  // reference chain starting with the workspace: workspace.(ref.)*ref
  def evalWorkspaceEntityLookupFinalEntity(workspace: Workspace,
                                           workspaceEntityLookupContext: WorkspaceEntityLookupContext
  ): ReadWriteAction[Iterable[EntityRecord]] =
    evalFinalEntity(workspace, buildEntityReferenceQueryForWorkspaceEntity(workspaceEntityLookupContext))

  // reference directly off the workspace: workspace.ref
  def evalWorkspaceAttributeLookupFinalEntity(workspace: Workspace,
                                              workspaceAttributeLookupContext: WorkspaceAttributeLookupContext
  ): ReadWriteAction[Iterable[EntityRecord]] =
    evalFinalEntity(workspace, buildEntityReferenceQueryForWorkspaceAttribute(workspaceAttributeLookupContext))

  /** PipelineQuery build functions for various lookups */

  private def buildAttributeQueryForEntity(context: EntityLookupContext): ExpressionEvaluationPipeline =
    buildPipelineQueryForRelations(
      Option(dataAccess.entityExpressionQuery.entityRootQuery),
      context.relation,
      entityAttributeFinalFunc(context.attributeName())
    )

  private def buildAttributeQueryForWorkspaceEntity(
    context: WorkspaceEntityLookupContext
  ): ExpressionEvaluationPipeline =
    buildPipelineQueryForRelations(
      Option(
        dataAccess.entityExpressionQuery.workspaceEntityRefRootQuery(
          toAttributeName(context.workspaceEntity.relation.attributeName())
        )
      ),
      context.relation(),
      entityAttributeFinalFunc(context.attributeName())
    )

  private def buildAttributeQueryForWorkspaceAttribute(
    context: WorkspaceAttributeLookupContext
  ): ExpressionEvaluationPipeline =
    ExpressionEvaluationPipeline(None, List.empty, workspaceAttributeFinalFunc(context.attributeName()))

  private def buildEntityReferenceQueryForEntity(context: EntityLookupContext): ExpressionEvaluationPipeline =
    buildPipelineQueryForRelations(
      Option(dataAccess.entityExpressionQuery.entityRootQuery),
      context.relation,
      dataAccess.entityExpressionQuery.entityFinalQuery,
      Option(context.attributeName())
    )

  private def buildEntityReferenceQueryForWorkspaceEntity(
    context: WorkspaceEntityLookupContext
  ): ExpressionEvaluationPipeline =
    buildPipelineQueryForRelations(
      Option(
        dataAccess.entityExpressionQuery.workspaceEntityRefRootQuery(
          toAttributeName(context.workspaceEntity.relation.attributeName())
        )
      ),
      context.relation,
      dataAccess.entityExpressionQuery.entityFinalQuery,
      Option(context.attributeName())
    )

  private def buildEntityReferenceQueryForWorkspaceAttribute(
    context: WorkspaceAttributeLookupContext
  ): ExpressionEvaluationPipeline =
    ExpressionEvaluationPipeline(
      None,
      List.empty,
      dataAccess.entityExpressionQuery.workspaceEntityFinalQuery(toAttributeName(context.attributeName))
    )

  private def buildPipelineQueryForRelations(rootStep: Option[RootFunc],
                                             relations: java.util.List[RelationContext],
                                             finalStep: FinalFunc,
                                             finalEntityOpt: Option[AttributeNameContext] = None
  ): ExpressionEvaluationPipeline = {
    import scala.jdk.CollectionConverters._

    val steps: List[RelationFunc] = (relations.asScala.toList.map(_.attributeName) ++ finalEntityOpt).map {
      attributeNameCtx =>
        dataAccess.entityExpressionQuery.entityNameAttributeRelationQuery(toAttributeName(attributeNameCtx)) _
    }

    ExpressionEvaluationPipeline(rootStep, steps, finalStep)
  }

  /** Helper functions */
  // Determine which FinalFunc to use by checking if entity attribute is a reserved attribute
  private def entityAttributeFinalFunc(attrNameContext: AttributeNameContext): FinalFunc = {
    val attrName = toAttributeName(attrNameContext)
    if (Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(attrName))) {
      dataAccess.entityExpressionQuery.entityReservedAttributeFinalQuery(attrName.name) _
    } else {
      dataAccess.entityExpressionQuery.entityAttributeFinalQuery(attrName) _
    }
  }

  // Determine which FinalFunc to use by checking if workspace attribute is a reserved attribute
  private def workspaceAttributeFinalFunc(attrNameContext: AttributeNameContext): FinalFunc = {
    val attrName = toAttributeName(attrNameContext)
    if (Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(attrName))) {
      dataAccess.entityExpressionQuery.workspaceReservedAttributeFinalQuery(attrName.name) _
    } else {
      dataAccess.entityExpressionQuery.workspaceAttributeFinalQuery(attrName) _
    }
  }

  /** Functions that run given PipelineQuery and return resulting AttributeValue or EntityRecord */

  private def evalFinalAttribute(workspaceContext: Workspace,
                                 pipeline: ExpressionEvaluationPipeline
  ): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] =
    runPipe(LocalEntityExpressionContext(workspaceContext, rootEntities, transactionId), pipeline) map { exprResults =>
      val results = exprResults map { case (key, attrVals) =>
        key -> Try(attrVals.collect {
          case AttributeNull           => Seq.empty
          case AttributeValueEmptyList => Seq.empty
          case av: AttributeValue      => Seq(av)
          case avl: AttributeValueList => avl.list
          case ae: AttributeEntityReference =>
            throw new RawlsException("Attribute expression returned a reference to an entity.")
          case ael: AttributeEntityReferenceList =>
            throw new RawlsException("Attribute expression returned a list of entities.")
          case AttributeEntityReferenceEmptyList =>
            throw new RawlsException("Attribute expression returned a list of entities.")
          case badType =>
            val message = s"unsupported type resulting from attribute expression: $badType: ${badType.getClass}"
            val MAX_ERROR_SIZE = 997
            val trimmed = if (message.length > MAX_ERROR_SIZE) {
              message.take(MAX_ERROR_SIZE) + "..."
            } else {
              message
            }
            throw new RawlsException(trimmed)
        }.flatten)
      }
      // add any missing entities (i.e. those missing the attribute) back into the result map
      results ++ rootEntities.getOrElse(Seq.empty[EntityRecord]).map(_.name).filterNot(results.keySet.contains).map {
        missingKey => missingKey -> Success(Seq())
      }
    }

  private def evalFinalEntity(workspaceContext: Workspace,
                              pipelineQuery: ExpressionEvaluationPipeline
  ): ReadWriteAction[Iterable[EntityRecord]] =
    if (rootEntities.isEmpty || rootEntities.get.isEmpty) {
      DBIO.failed(
        new RawlsException(s"ExpressionEvaluator has no entities passed to evalFinalEntity")
      ) // todo: move these checks elsewhere?
    } else if (rootEntities.get.size > 1) {
      DBIO.failed(
        new RawlsException(
          s"ExpressionEvaluator has been set up with ${rootEntities.get.size} entities for evalFinalEntity, can only accept 1."
        )
      )
    } else {
      // If parsing succeeded, evaluate the expression using the given root entities and retype back to EntityRecord
      runPipe(LocalEntityExpressionContext(workspaceContext, rootEntities, transactionId), pipelineQuery).map {
        resultMap =>
          // NOTE: As per the DBIO.failed a few lines up, resultMap should only have one key, the same root elem.
          val (rootElem, elems) = resultMap.head
          elems.collect { case e: EntityRecord => e }
      }
    }

  private def runPipe(expressionContext: LocalEntityExpressionContext,
                      pipe: ExpressionEvaluationPipeline
  ): ReadAction[Map[String, Iterable[Any]]] = {
    val builtPipe = pipe.rootStep.map(rootStep =>
      pipe.relationSteps.foldLeft(rootStep(expressionContext)) { (queryPipeline, func) =>
        func(expressionContext, queryPipeline)
      }
    )

    // Run the final step. This executes the pipeline and returns its output.
    Try {
      pipe.finalStep(expressionContext, builtPipe)
    } match {
      case Success(finalResult) => finalResult
      case Failure(regret)      => dbio.DBIO.failed(regret)
    }
  }
}
