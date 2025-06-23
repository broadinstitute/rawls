package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.ExpressionAndResult
import org.broadinstitute.dsde.rawls.entities.base.{
  ExpressionEvaluationContext,
  ExpressionEvaluationSupport,
  InputExpressionReassembler
}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, CompactEvaluateVisitor}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.ExpressionLookup
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeValue,
  AttributeValueList,
  ErrorReport,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue
}
import org.broadinstitute.dsde.rawls.util.CollectionUtils

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class QueryPlan(
  relationChain: List[String],
  expressionMappings: Map[String, Set[String]] // expression -> attributes it needs from this query
)

class CompactExpressionEvaluator(repository: CompactEntityRepository) extends ExpressionEvaluationSupport {

  // TODO: if the end result is a reference attribute, we get blank.  i think that should be an error
  /**
   * Evaluates a single expression against a specific entity and returns the resulting attribute values.
   *
   * @param workspaceId The UUID of the workspace containing the entity to evaluate against
   * @param expression The expression string to evaluate (e.g., "this.samples.type",
   *                   "{id: this.sample_id, files: this.samples.files}")
   * @param entityType The type of the entity to start evaluation from (e.g., "sample_set", "sample")
   * @param entityName The name of the specific entity to start evaluation from
   * @return A Future containing a sequence of AttributeValue objects representing the final
   *         evaluated result of the expression
   *
   * @example
   * {{{
   * // Evaluate a simple attribute reference
   * evaluateExpression(workspaceId, "this.sample_id", "sample_set", "set1")
   * // Returns: Future(Seq(AttributeString("SAMPLE_123")))
   *
   * // Evaluate a relationship traversal
   * evaluateExpression(workspaceId, "this.samples.type", "sample_set", "set1")
   * // Returns: Future(Seq(AttributeString("tumor"), AttributeString("normal")))
   *
   * // Evaluate a complex structured expression
   * evaluateExpression(workspaceId, "{id: this.sample_id, count: this.samples.length}", "sample_set", "set1")
   * // Returns: Future(Seq(AttributeString("{\"id\": \"SAMPLE_123\", \"count\": 2}")))
   * }}}
   *
   * @throws RawlsExceptionWithErrorReport if the expression cannot be parsed or if the referenced
   *                                       entity or attributes do not exist
   *
   */
  def evaluateExpression(workspaceId: UUID, expression: String, entityType: String, entityName: String)(implicit
    executionContext: ExecutionContext
  ): Future[Seq[AttributeValue]] = {
    // We need to get the parse tree to use for final input
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new CompactEvaluateVisitor()
    val parsedTree = terraExpressionParser.root()
    val lookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
    val queryPlans = buildQueryPlans(lookups)
    val queryFutures: Seq[Future[Seq[ExpressionAndResult]]] = queryPlans map { plan =>
      // For a single expression, the root entity type is the same as the given entity
      executeQueryPlan(workspaceId, entityType, entityName, entityType, plan)
    }
    Future.sequence(queryFutures).map { allResults =>
      val combinedResults: Seq[ExpressionAndResult] = allResults.flatten

      InputExpressionReassembler
        .constructFinalInputValues(
          combinedResults,
          parsedTree,
          Some(Seq(entityName)),
          None
        )
        .getOrElse(entityName, Success(Seq.empty))
        .get
        .toSeq
    }
  }

  /**
   * Evaluates multiple expressions against entities in the context of a workflow submission,
   * handling entity type transitions and optimizing database queries across all expressions.
   *
   * This method is designed for workflow submission validation where multiple input expressions
   * need to be evaluated against potentially different entity types. It handles cases where
   * the input entity type differs from the target entity type (rootEntityType) by properly
   * traversing entity relationships. The method optimizes performance by consolidating all
   * expression lookups into efficient query plans and executing them in batch.
   *
   * @param workspaceId The UUID of the workspace containing the entities to evaluate against
   * @param expressionEvaluationContext Context containing:
   *                                   - entityType: The type of the input entity (e.g., "sample_set")
   *                                   - entityName: The name of the input entity
   *                                   - expression: Optional entity expression to determine target entities
   *                                   - rootEntityType: The expected entity type for final results (e.g., "sample")
   * @param gatherInputsResult The workflow inputs that need expression evaluation, containing
   *                          processableInputs with their expressions
   * @return A Future containing a LazyList of SubmissionValidationEntityInputs, where each
   *         entry represents the resolved input values for a specific entity of the rootEntityType
   *
   * @throws RawlsExceptionWithErrorReport if:
   *         - Required context fields (entityType, entityName, rootEntityType) are missing
   *         - Entity type doesn't match rootEntityType when no entity expression is provided
   *         - Expressions cannot be parsed or referenced entities/attributes don't exist
   *
   */
  def evaluateExpressions(workspaceId: UUID,
                          expressionEvaluationContext: ExpressionEvaluationContext,
                          gatherInputsResult: MethodConfigResolver.GatherInputsResult
  )(implicit executionContext: ExecutionContext): Future[LazyList[SubmissionValidationEntityInputs]] = {
    // First, verify that necessary entity information is present and consistent
    val entityType = expressionEvaluationContext.entityType.getOrElse(
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityType"))
    )
    val entityName = expressionEvaluationContext.entityName.getOrElse(
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityName"))
    )

    val rootEntityTypeOpt = expressionEvaluationContext.rootEntityType
    if (rootEntityTypeOpt.isEmpty) {
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing rootEntityType")))
    } else if (
      expressionEvaluationContext.expression.isEmpty &&
      entityType != rootEntityTypeOpt.get
    ) {
      val whatYouGaveUs =
        if (expressionEvaluationContext.entityType.isDefined)
          s"an entity of type ${expressionEvaluationContext.entityType.get}"
        else "no entity"
      Future.failed(
        new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(
            StatusCodes.BadRequest,
            s"Method configuration expects an entity of type ${rootEntityTypeOpt.get}, but you gave us $whatYouGaveUs."
          )
        )
      )
    } else {

      // Next, parse both the entity expression (if it exists) and the input expressions
      // To determine which entities to fetch from the database
      val rootEntityType = rootEntityTypeOpt.get

      val entityLookups: Seq[ExpressionLookup] = expressionEvaluationContext.expression match {
        case None             => Seq.empty
        case Some(expression) => parseLookups(expression)
      }

      // Parse all input expressions and collect their lookups and parsed trees
      val inputExpressionData = gatherInputsResult.processableInputs.toSeq.map { input =>
        val terraExpressionParser = AntlrTerraExpressionParser.getParser(input.expression)
        val visitor = new CompactEvaluateVisitor()
        val parsedTree = terraExpressionParser.root()
        val inputLookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
        (input, parsedTree, inputLookups)
      }

      // Gather all ExpressionLookups from all inputs
      val allLookups = inputExpressionData.flatMap(_._3)

      // Build query plans for all lookups combined
      val queryPlans = if (entityType != rootEntityType && entityLookups.nonEmpty) {
        // Use the entityLookups to determine the starting point for the relationChains in the query plans
        val entityRelationChain = entityLookups.flatMap(_.relations.map(_.attributeName().getText)).toList
        val baseQueryPlans = buildQueryPlans(allLookups)

        baseQueryPlans.map { plan =>
          plan.copy(relationChain = entityRelationChain ++ plan.relationChain)
        }
      } else {
        buildQueryPlans(allLookups)
      }

      // Execute all query plans
      val queryFutures: Seq[Future[Seq[ExpressionAndResult]]] = queryPlans.map { plan =>
        executeQueryPlan(workspaceId, entityType, entityName, rootEntityType, plan, entityLookups)
      }

      Future.sequence(queryFutures).flatMap { allQueryResults =>
        val combinedExpressionAndResults: Seq[ExpressionAndResult] = allQueryResults.flatten

        // Determine the correct root entity names based on rootEntityType
        val rootEntityNames = if (entityType == rootEntityType) {
          // Entity type matches root entity type, use the original entity name
          Seq(entityName)
        } else {
          // Entity type differs from root entity type, extract entity names from query results
          // The query results contain entities of the rootEntityType
          combinedExpressionAndResults.flatMap(_._2.keys).distinct
        }

        // Process each input separately using the combined query results
        val inputFutures: Seq[Future[Seq[(ExpressionEvaluationSupport.EntityName, SubmissionValidationValue)]]] =
          inputExpressionData.map { case (input, parsedTree, inputLookups) =>

            // Filter ExpressionAndResults relevant to this input
            val relevantResults = combinedExpressionAndResults.filter { case (expression, _) =>
              inputLookups.exists(_.expression == expression)
            }

            Future.successful {
              // Use InputExpressionReassembler to get the final result for this input
              val resultMap = InputExpressionReassembler.constructFinalInputValues(
                relevantResults,
                parsedTree,
                Some(rootEntityNames),
                Some(input)
              )
              convertToSubmissionValidationValues(resultMap, input)
            }
          }

        Future.sequence(inputFutures).map { resultsSeq =>
          CollectionUtils
            .groupByTuples(resultsSeq.flatten)
            .map { case (entityName: ExpressionEvaluationSupport.EntityName, values) =>
              SubmissionValidationEntityInputs(
                entityName = entityName,
                inputResolutions = values.toSet
              )
            }
            .to(LazyList)
        }
      }
    }
  }

  // TODO this is now only used in one case; do we need a separate method anymore?
  // It's useful for testing the visitor
  def parseLookups(expression: String): Seq[ExpressionLookup] = {
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new CompactEvaluateVisitor()
    Try(terraExpressionParser.root()) match {
      case Success(parsedTree) =>
        visitor.visit(parsedTree).map { result =>
          // result is Seq[AttributeLookup]
          result
        }
      case Failure(_) => Seq.empty
    }
  }

  /**
   * This is a recursive function that walks down the relations lists of ExpressionLookups. Each
   * relation should correspond to a relation between entity types. We start with
   * relationLevel 0. All ExpressionLookups that have the same number of relations as the relation
   * level (none to start) are queries on the given entity type. Collect those lookups for an empty list,
   * i.e. no relations. For all lookups that have more relations than
   * relation level, group them by the relation at that level (the first relation to start) and call
   * this method recursively for each grouping, prepending the relation to the result to build the relation chain.
   * Keep track of the attributes needed for each relation level and the expression that generated it in order to collate
   * results later.
   *
   * @param lookups The ExpressionLookups to process, all of which must have the same value
   *     up to relationLevel
   * @param relationLevel The current relation level starting with 0 and incrementing with each
   *     recursive call
   * @return A seq of QueryPlans, which contain: List of string representing the chain of relations,
   *         map of expression to list of strings representing the attributes to get from the entities at the end of the chain
   */
  def buildQueryPlans(
    lookups: Seq[ExpressionLookup],
    relationLevel: Int = 0
  ): Seq[QueryPlan] = {
    // Group lookups by the relation at the current level
    val nextLookupByRelation = lookups
      .filter(_.relations.size > relationLevel)
      .groupBy(_.relations(relationLevel).attributeName().getText)

    // Recursively process each group, incrementing the relation level
    val nextPlans = nextLookupByRelation.toSeq.flatMap { case (relation, groupedLookups) =>
      buildQueryPlans(groupedLookups, relationLevel + 1).map { plan =>
        plan.copy(relationChain = relation :: plan.relationChain)
      }
    }

    // Collect lookups at the current level (those with no more relations)
    val currentLevelLookups = lookups.filter(_.relations.size == relationLevel)
    val currentPlan = if (currentLevelLookups.nonEmpty) {
      val expressionMappings = currentLevelLookups.groupBy(_.expression).map { case (expr, exprLookups) =>
        expr -> exprLookups.flatMap(_.attributeName).toSet
      }
      Seq(QueryPlan(Nil, expressionMappings))
    } else {
      Seq.empty
    }

    currentPlan ++ nextPlans
  }

  /**
   * Executes a single query plan to retrieve entity records and extract the required attributes.
   * Processes the attained attributes into ExpressionAndResult tuples that can be consumed
   * by the InputExpressionReassembler.
   *
   * @param workspaceId The UUID of the workspace containing the entities to query
   * @param entityType The type of the starting entity (e.g., "sample_set", "sample")
   * @param entityName The name of the specific entity to start the query from
   * @param rootEntityType The expected entity type for the final results
   * @param plan The QueryPlan containing:
   *             - relationChain: List of relation names to traverse (e.g., ["samples", "participants"])
   *             - expressionMappings: Map from expression strings to the set of attribute names
   *               that expression needs from the entities at the end of the relation chain
   * @return A Future containing a sequence of ExpressionAndResult tuples, where each tuple contains:
   *         - The original expression string as the key
   *         - A Map from entity name to Try[Iterable[AttributeValue] containing the extracted
   *           attribute values for that expression
   */
  def executeQueryPlan(workspaceId: UUID,
                       entityType: String,
                       entityName: String,
                       rootEntityType: String,
                       plan: QueryPlan,
                       entityLookups: Seq[ExpressionLookup] = Seq.empty
  )(implicit
    executionContext: ExecutionContext
  ): Future[Seq[ExpressionAndResult]] =
    repository.dataSource
      .inTransaction { _ =>
        if (plan.relationChain.isEmpty && entityLookups.isEmpty) {
          repository.queries.getEntity(workspaceId, entityType, entityName).map {
            case Some(entity) => Map(entity.name -> entity)
            case None =>
              throw new RawlsExceptionWithErrorReport(
                ErrorReport(StatusCodes.NotFound, s"Entity of type $entityType with name $entityName not found.")
              )
          }
        } else {
          repository.queries.queryRelatedRecordsWithArray(workspaceId, entityType, entityName, plan.relationChain)
        }
      }
      .map { entityRecords =>
        // Validate entity types if we have entityLookups
        if (entityLookups.nonEmpty && entityRecords.nonEmpty) {
          val actualEntityTypes = entityRecords.values.map(_.entityType).toSet
          if (actualEntityTypes.nonEmpty && !actualEntityTypes.contains(rootEntityType)) {
            val actualTypesStr = actualEntityTypes.mkString(", ")
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(
                StatusCodes.BadRequest,
                s"The expression in your SubmissionRequest matched only entities of the wrong type. " +
                  s"(Expected type $rootEntityType, but got $actualTypesStr.)"
              )
            )
          }
        }

        // For each expression in this query plan, create an ExpressionAndResult
        plan.expressionMappings.toSeq.flatMap { case (expression, attributeNames) =>
          attributeNames.map { attrName =>
            val attributeName = AttributeName.fromDelimitedName(attrName)

            if (entityType == rootEntityType) {
              // Entity type is the root entity type: aggregate all attributes and map to the original entity name
              val attrs: Seq[AttributeValue] =
                entityRecords.values.toSeq.flatMap(_.toEntity.attributes.get(attributeName)).flatMap {
                  case avl: AttributeValueList => avl.list
                  case av: AttributeValue      => Seq(av)
                  case _                       => Seq.empty
                }
              (expression, Map(entityName -> Success(attrs)))
            } else {
              // Entity type is not root entity type, e.g. we're dealing with a set: map to actual entity names from the query results
              val entityToAttributeValues = entityRecords.map { case (actualEntityName, record) =>
                val attrs: Seq[AttributeValue] = record.toEntity.attributes
                  .get(attributeName) match {
                  case Some(avl: AttributeValueList) => avl.list
                  case Some(av: AttributeValue)      => Seq(av)
                  case _                             => Seq.empty
                }
                actualEntityName -> Success(attrs)
              }
              (expression, entityToAttributeValues)
            }
          }
        }
      }
}
