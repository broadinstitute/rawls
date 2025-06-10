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
    val queryPlans = buildPlans(lookups)
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
        expressionEvaluationContext.expression match {
          case None =>
            repository.dataSource
              .inTransaction { _ =>
                repository.queries
                  .getEntity(workspaceId, entityType, entityName)
              } flatMap {
              case Some(record) =>
                // Convert the single CompactEntityRecord into a Map for buildValidationInputs
                val entityRecords = Map(entityName -> Seq(record))
                val inputFutures: Seq[Future[SubmissionValidationValue]] =
                  gatherInputsResult.processableInputs.toSeq.map { input =>
                    val terraExpressionParser = AntlrTerraExpressionParser.getParser(input.expression)
                    val visitor = new CompactEvaluateVisitor()
                    val parsedTree = terraExpressionParser.root()
                    val inputLookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
                    val exprAndResults = inputLookups
                      .map { lookup =>
                        val attrNameOpt = lookup.attributeName
                        val entityValueMap: Map[String, Try[Iterable[AttributeValue]]] =
                          entityRecords.map { case (eName, records) =>
                            val values: Iterable[AttributeValue] = attrNameOpt match {
                              case Some(attrName) =>
                                records
                                  .map(_.toEntity)
                                  .flatMap(_.attributes.get(AttributeName.fromDelimitedName(attrName)))
                                  .collect { case av: AttributeValue => av }
                              case None =>
                                lookup.values
                            }
                            eName -> Success(values)
                          }
                        (lookup.expression, entityValueMap)
                      }
                    // Use InputExpressionReassembler to get the final result
                    val resultMap = InputExpressionReassembler.constructFinalInputValues(
                      exprAndResults,
                      parsedTree,
                      Some(Seq(entityName)),
                      None
                    )
                    // Build SubmissionValidationEntityInputs for each entity
                    Future.successful(
                      resultMap.get(entityName).flatMap(_.toOption).flatMap(_.headOption) match {
                        case Some(value) =>
                          SubmissionValidationValue(
                            value = Some(value),
                            error = None,
                            inputName = input.workflowInput.getName
                          )
                        case None =>
                          SubmissionValidationValue(
                            value = None,
                            error = Some("No value found"),
                            inputName = input.workflowInput.getName
                          )
                      }
                    )
                  }
                Future.sequence(inputFutures).map { inputResolutions =>
                  LazyList(
                    SubmissionValidationEntityInputs(
                      entityName = entityName,
                      inputResolutions = inputResolutions.toSet
                    )
                  )
                }
              case None =>
                Future.successful(LazyList.empty[SubmissionValidationEntityInputs])
            }
          case Some(expression) =>
            val entityLookups = parseLookups(expression) // Determines what the root entity is
            println("entityLookups: ", entityLookups)

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
            val queryPlans = buildPlans(allLookups)
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
    }

//          val entityLookups = parseLookups(expression) //Determines what the root entity is
//          val inputFutures: Seq[Future[Seq[(ExpressionEvaluationSupport.EntityName, SubmissionValidationValue)]]] =
//            gatherInputsResult.processableInputs.toSeq.map { input =>
//              val terraExpressionParser = AntlrTerraExpressionParser.getParser(input.expression)
//              val visitor = new CompactEvaluateVisitor()
//              val parsedTree = terraExpressionParser.root()
//              val inputLookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
//              // TODO currently runs a query for each input.  should presumably only run one query
//              repository.dataSource
//                .inTransaction { _ =>
//                  repository.queries.queryRelatedRecordsWithArray(workspaceId,
//                                                                  entityType,
//                                                                  entityName,
//                                                                  entityLookups,
//                                                                  inputLookups
//                  )
//                }
//                .map { entityRecords =>
//                  val rootEntityType = rootEntityTypeOpt.get
//                  val allRecords = entityRecords.values.flatten.toSeq
//                  val recordsMatchRootEntity = allRecords.forall(_.entityType == rootEntityType)
//                  val exprAndResults = inputLookups.map { lookup =>
//                    val attrNameOpt = lookup.attributeName
//                    // If the rootEntityType isn't the same as the entityRecords, then we need to map all the different records' values to the rootEntity
//                    val entityValueMap: Map[String, Try[Iterable[AttributeValue]]] =
//                      if (recordsMatchRootEntity) {
//                        // Each entity gets its own values as before
//                        entityRecords.map { case (eName, records) =>
//                          val values: Iterable[AttributeValue] = attrNameOpt match {
//                            case Some(attrName) =>
//                              records
//                                .map(_.toEntity)
//                                .flatMap(_.attributes.get(AttributeName.fromDelimitedName(attrName)))
//                                .flatMap {
//                                  case avl: AttributeValueList => avl.list
//                                  case av: AttributeValue      => Seq(av)
//                                  case _                       => Seq.empty
//                                }
//                            case None =>
//                              lookup.values
//                          }
//                          eName -> Success(values)
//                        }
//                      } else {
//                        val values: Iterable[AttributeValue] = attrNameOpt match {
//                          case Some(attrName) =>
//                            allRecords
//                              .map(_.toEntity)
//                              .flatMap(_.attributes.get(AttributeName.fromDelimitedName(attrName)))
//                              .flatMap {
//                                case avl: AttributeValueList => avl.list
//                                case av: AttributeValue      => Seq(av)
//                                case _                       => Seq.empty
//                              }
//                          case None =>
//                            lookup.values
//                        }
//                        Map(entityName -> Success(values))
//                      }
//                    (lookup.expression, entityValueMap)
//                  }
//                  // Use InputExpressionReassembler to get the final result
//                  val rootEntityNames = if (recordsMatchRootEntity) entityRecords.keys.toSeq else Seq(entityName)
//                  val resultMap = InputExpressionReassembler.constructFinalInputValues(
//                    exprAndResults,
//                    parsedTree,
//                    Some(rootEntityNames),
//                    None
//                  )
//                  convertToSubmissionValidationValues(resultMap, input)
//                }
//            }
//          Future.sequence(inputFutures).map { resultsSeq =>
//            CollectionUtils
//              .groupByTuples(resultsSeq.flatten)
//              .map { case (entityName: ExpressionEvaluationSupport.EntityName, values) =>
//                SubmissionValidationEntityInputs(
//                  entityName = entityName,
//                  inputResolutions = values.toSet
//                )
//              }
//              .to(LazyList)
//          }
//      }
//    }
//  }

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

  // TODO update javadoc
  /**
   * This is a recursive function that walks down the relations lists of ExpressionLookups. Each
   * relation should correspond to a relation between entity types. We start with
   * relationLevel 0. All ExpressionLookups that have the same number of relations as the relation
   * level (none to start) are queries on the given entity type. Collect those lookups for an empty list,
   * i.e. no relations. For all lookups that have more relations than
   * relation level, group them by the relation at that level (the first relation to start) and call
   * this method recursively for each grouping, prepending the relation to the result to build the relation chain.
   *
   * @param expressionLookups The ExpressionLookups to process, all of which must have the same value
   *     up to relationLevel
   * @param relationLevel The current relation level starting with 0 and incrementing with each
   *     recursive call
   * @return A set of tuples containing: List of string representing the chain of relations,
   *         list of strings representing the attributes to get from the entities at the end of the chain
   */
  def buildPlans(
    lookups: Seq[ExpressionLookup],
    relationLevel: Int = 0
  ): Seq[QueryPlan] = {
    // Group lookups by the relation at the current level
    val nextLookupByRelation = lookups
      .filter(_.relations.size > relationLevel)
      .groupBy(_.relations(relationLevel).attributeName().getText)

    // Recursively process each group, incrementing the relation level
    val nextPlans = nextLookupByRelation.toSeq.flatMap { case (relation, groupedLookups) =>
      buildPlans(groupedLookups, relationLevel + 1).map { plan =>
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
