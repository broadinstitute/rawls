package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadAction
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
  Attributable,
  AttributeName,
  AttributeString,
  AttributeValue,
  AttributeValueList,
  ErrorReport,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue
}
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import slick.dbio.DBIO

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
    val queryActions: Seq[ReadAction[Seq[ExpressionAndResult]]] = queryPlans.map { plan =>
      executeQueryPlan(workspaceId, entityType, entityName, entityType, plan)
    }

    val combinedAction = DBIO.sequence(queryActions)

    repository.dataSource
      .inTransaction { _ =>
        combinedAction
      }
      .map { allResults =>
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
    val entityInfoFuture =
      (expressionEvaluationContext.entityType,
       expressionEvaluationContext.entityName,
       expressionEvaluationContext.rootEntityType
      ) match {
        case (Some(entityType), Some(entityName), Some(rootEntityType)) =>
          // All three exist, assign their values
          if (
            expressionEvaluationContext.expression.isEmpty &&
            entityType != rootEntityType
          ) {
            val whatYouGaveUs =
              if (expressionEvaluationContext.entityType.isDefined)
                s"an entity of type ${expressionEvaluationContext.entityType.get}"
              else "no entity"
            Future.failed(
              new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(
                  StatusCodes.BadRequest,
                  s"Method configuration expects an entity of type ${rootEntityType}, but you gave us $whatYouGaveUs."
                )
              )
            )
          } else {
            val entityLookups: Seq[ExpressionLookup] = expressionEvaluationContext.expression match {
              case None             => Seq.empty
              case Some(expression) => parseLookups(expression)
            }

            val inputExpressionData = gatherInputsResult.processableInputs.toSeq.map { input =>
              val terraExpressionParser = AntlrTerraExpressionParser.getParser(input.expression)
              val visitor = new CompactEvaluateVisitor()
              val parsedTree = terraExpressionParser.root()
              val inputLookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
              (input, parsedTree, inputLookups)
            }

            val allLookups = inputExpressionData.flatMap { case (_, _, lookups) => lookups }
            val queryPlans = if (entityType != rootEntityType && entityLookups.nonEmpty) {
              val entityRelationChain = entityLookups.flatMap(_.attributeName).toList
              val baseQueryPlans = buildQueryPlans(allLookups)

              baseQueryPlans.map { plan =>
                plan.copy(relationChain = entityRelationChain ++ plan.relationChain)
              }
            } else {
              buildQueryPlans(allLookups)
            }

            val queryActions: Seq[ReadAction[Seq[ExpressionAndResult]]] = queryPlans.map { plan =>
              executeQueryPlan(workspaceId, entityType, entityName, rootEntityType, plan, entityLookups)
            }

            val combinedAction = DBIO.sequence(queryActions)
            repository.dataSource
              .inTransaction { _ =>
                combinedAction
              }
              .map { allQueryResults =>
                val combinedExpressionAndResults: Seq[ExpressionAndResult] = allQueryResults.flatten

                val rootEntityNames = if (entityType == rootEntityType) {
                  Seq(entityName)
                } else {
                  combinedExpressionAndResults.flatMap { case (_, resultMap) => resultMap.keys }.distinct
                }

                // Pre-compute a map from expression -> ExpressionAndResult for O(1) lookup
                val expressionToResultMap: Map[String, Seq[ExpressionAndResult]] =
                  combinedExpressionAndResults.groupBy { case (expression, _) => expression }

                inputExpressionData.flatMap { case (input, parsedTree, inputLookups) =>
                  // Lookup ExpressionAndResults relevant to this input
                  val relevantResults = inputLookups.flatMap { lookup =>
                    expressionToResultMap.getOrElse(lookup.expression, Seq.empty)
                  }
                  val resultMap = InputExpressionReassembler.constructFinalInputValues(
                    relevantResults,
                    parsedTree,
                    Some(rootEntityNames),
                    Some(input)
                  )
                  convertToSubmissionValidationValues(resultMap, input)
                }
              }
          }

        case (None, None, None) =>
          // None exist, proceed without assignment
          val inputExpressionData = gatherInputsResult.processableInputs.toSeq.map { input =>
            val terraExpressionParser = AntlrTerraExpressionParser.getParser(input.expression)
            val visitor = new CompactEvaluateVisitor()
            val parsedTree = terraExpressionParser.root()
            val inputLookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
            (input, parsedTree, inputLookups)
          }

          // Repackage the parsed expressions into the correct form without querying the database
          Future.successful {
            inputExpressionData.flatMap { case (input, parsedTree, _) =>
              val resultMap = InputExpressionReassembler.constructFinalInputValues(
                Seq.empty, // No database results since no entities are queried
                parsedTree,
                None, // No root entity names since no entities are queried
                Some(input)
              )
              convertToSubmissionValidationValues(resultMap, input)
            }
          }

        case (Some(et), None, _) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, s"Missing entityName")
            )
          )

        case (None, Some(en), _) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, s"Missing entityType")
            )
          )

        case (None, None, Some(ret)) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, s"Missing entityType and entityName")
            )
          )

        case (Some(et), Some(en), None) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, s"Missing rootEntityType")
            )
          )
      }

    entityInfoFuture.map { resultsSeq =>
      CollectionUtils
        .groupByTuples(resultsSeq)
        .map { case (entityName: ExpressionEvaluationSupport.EntityName, values) =>
          SubmissionValidationEntityInputs(
            entityName = entityName,
            inputResolutions = values.toSet
          )
        }
        .to(LazyList)
    }
  }

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
  ): ReadAction[Seq[ExpressionAndResult]] = {
    val queryAction =
      if (plan.relationChain.isEmpty && entityLookups.isEmpty) { // TODO not sure if entityLookups.isEmpty is needed or not
        repository.queries.getEntity(workspaceId, entityType, entityName).map {
          case Some(entity) => Map(entity.name -> Seq(entity)) // Wrap the entity in a Seq to match the expected type
          case None =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.NotFound, s"Entity of type $entityType with name $entityName not found.")
            )
        }
      } else {
        repository.queries.queryRelatedRecordsWithRelationChain(workspaceId, entityType, entityName, plan.relationChain)
      }

    queryAction.map { entityRecords =>
      // Validate entity types if we have entityLookups
      // TODO correct validation
      if (entityLookups.nonEmpty && entityRecords.nonEmpty) {
        val actualEntityTypes = entityRecords.values.flatten.map(_.entityType).toSet
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

          val entityToAttributeValues = if (entityType == rootEntityType) {
            // Group all results under the original entityName since we want results grouped by the starting entity type
            val allAttrs: Seq[AttributeValue] = entityRecords.values.flatten.toSeq.flatMap { record =>
              val attributeNameToCheck =
                AttributeName.withDefaultNS(record.entityType + Attributable.entityIdAttributeSuffix)
              record.toEntity.attributes.get(attributeName) match {
                case Some(avl: AttributeValueList) =>
                  avl.list.toSeq
                case Some(av: AttributeValue) =>
                  Seq(av)
                case _ if attributeName == attributeNameToCheck =>
                  Seq(AttributeString(record.name))
                case _ =>
                  Seq.empty
              }
            }
            Map(entityName -> Success(allAttrs))
          } else {
            entityRecords.map { case (actualEntityName, records) =>
              val attrs: Seq[AttributeValue] = records.flatMap { record =>
                val attributeNameToCheck =
                  AttributeName.withDefaultNS(record.entityType + Attributable.entityIdAttributeSuffix)
                record.toEntity.attributes.get(attributeName) match {
                  case _ if attributeName == attributeNameToCheck =>
                    Seq(
                      AttributeString(record.name)
                    ) // Return the record's name as an AttributeString wrapped in a Seq
                  case Some(avl: AttributeValueList) =>
                    avl.list // Return the list of values directly
                  case Some(av: AttributeValue) =>
                    Seq(av) // Wrap the single value in a Seq
                  case _ =>
                    Seq.empty // Return an empty Seq for unmatched cases
                }
              }
              actualEntityName -> Success(attrs)
            }
          }
          (expression, entityToAttributeValues)
        }
      }
    }
  }
}
