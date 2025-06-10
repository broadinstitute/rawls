package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityRecord
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{
  EntityName,
  ExpressionAndResult,
  LookupExpression
}
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
  Attribute,
  AttributeName,
  AttributeValue,
  AttributeValueList,
  EntityName,
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
      executeQueryPlan(workspaceId, entityType, entityName, plan)
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

  def evaluateExpressions(workspaceId: UUID,
                          expressionEvaluationContext: ExpressionEvaluationContext,
                          gatherInputsResult: MethodConfigResolver.GatherInputsResult
  )(implicit executionContext: ExecutionContext): Future[LazyList[SubmissionValidationEntityInputs]] =
//    Future.successful(LazyList.empty)
    {
      // TODO is this necessarily an error?  when isn't it?
      val entityType = expressionEvaluationContext.entityType.getOrElse(
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityType"))
      )
      val entityName = expressionEvaluationContext.entityName.getOrElse(
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityName"))
      )

      val rootEntityTypeOpt = expressionEvaluationContext.rootEntityType
      if (rootEntityTypeOpt.isEmpty) { // TODO Is this an error or what?
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
        // If there's an expression, evaluate it to get the list of entities to run this job on.
        // Otherwise, use the entity given in the submission.
        val entityLookups: Seq[ExpressionLookup] = expressionEvaluationContext.expression match {
          case None             => Seq.empty
          case Some(expression) => parseLookups(expression)
        }
        // TODO use entitylookups?? surely it shouldn't work without it
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
        val queryPlans = buildQueryPlans(allLookups)
        println("queryPlans: ", queryPlans)

        // Execute all query plans
        val queryFutures: Seq[Future[Seq[ExpressionAndResult]]] = queryPlans.map { plan =>
          executeQueryPlan(workspaceId, entityType, entityName, plan)
        }

        Future.sequence(queryFutures).flatMap { allQueryResults =>
          val combinedExpressionAndResults: Seq[ExpressionAndResult] = allQueryResults.flatten

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
                  Some(Seq(entityName)), // Use the original entityName as root
                  None
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
   * @param expressionLookups The ExpressionLookups to process, all of which must have the same value
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
   * @param plan The QueryPlan containing:
   *             - relationChain: List of relation names to traverse (e.g., ["samples", "participants"])
   *             - expressionMappings: Map from expression strings to the set of attribute names
   *               that expression needs from the entities at the end of the relation chain
   * @return A Future containing a sequence of ExpressionAndResult tuples, where each tuple contains:
   *         - The original expression string as the key
   *         - A Map from entity name to Try[Iterable[AttributeValue]] containing the extracted
   *           attribute values for that expression
   */
  def executeQueryPlan(workspaceId: UUID, entityType: String, entityName: String, plan: QueryPlan)(implicit
    executionContext: ExecutionContext
  ): Future[Seq[ExpressionAndResult]] =
    repository.dataSource
      .inTransaction { _ =>
        repository.queries.queryRelatedRecordsWithArray(workspaceId, entityType, entityName, plan.relationChain)
      }
      .map { entityRecords =>
        val allRecords = entityRecords.values.flatten.toSeq.map(_.toEntity)

        // For each expression in this query plan, create an ExpressionAndResult
        plan.expressionMappings.toSeq.flatMap { case (expression, attributeNames) =>
          attributeNames.map { attrName =>
            val attributeName = AttributeName.fromDelimitedName(attrName)
            val attrs: Seq[AttributeValue] = allRecords.flatMap(_.attributes.get(attributeName)).flatMap {
              case avl: AttributeValueList => avl.list
              case av: AttributeValue      => Seq(av)
              case _                       => Seq.empty
            }
            (expression, Map(entityName -> Success(attrs)))
          }
        }
      }

}
