package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadAction
import org.broadinstitute.dsde.rawls.entities.base.{ExpressionEvaluationContext, ExpressionEvaluationSupport}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, CompactEvaluateVisitor}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  AttributeValue,
  AttributeValueList,
  ErrorReport,
  SubmissionValidationEntityInputs,
  SubmissionValidationValue
}
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CompactExpressionEvaluator(repository: CompactEntityRepository) extends ExpressionEvaluationSupport {

  def evaluateExpression(workspaceId: UUID, expression: String, entityType: String, entityName: String)(implicit
    executionContext: ExecutionContext
  ): Future[Seq[AttributeValue]] = {
    val lookup = parseLookups(expression)

    repository.dataSource.inTransaction { _ =>
      lookUpToQuery(workspaceId, lookup, entityType, entityName)
    }
  }

  // TODO alllllll the error handling, correctly
  // TODO also everything else I might need to worry about from gatherInputsResult, although as far as I can tell, LocalEntityProvider just ignores everything else
  def evaluateExpressions(workspaceId: UUID,
                          expressionEvaluationContext: ExpressionEvaluationContext,
                          gatherInputsResult: MethodConfigResolver.GatherInputsResult
  )(implicit executionContext: ExecutionContext): Future[LazyList[SubmissionValidationEntityInputs]] = {
    // TODO is this necessarily an error?  when isn't it?
    val rootEntityType = expressionEvaluationContext.entityType.getOrElse(
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityType"))
    )
    val rootEntityName = expressionEvaluationContext.entityName.getOrElse(
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityName"))
    )

    val inputFutures: Seq[Future[(String, Seq[SubmissionValidationValue])]] = {
      gatherInputsResult.processableInputs.toSeq.map { input =>
        val lookups = parseLookups(input.expression)
        repository.dataSource
          .inTransaction { _ =>
            lookUpToQuery(workspaceId, lookups, rootEntityType, rootEntityName)
          }
          .map { values =>
            val attributeValueList = AttributeValueList(values)

            // Wrap each value in a SubmissionValidationValue
            val validationValues =
              Seq(SubmissionValidationValue(Some(attributeValueList), None, input.workflowInput.getName))
            rootEntityName -> validationValues // TODO this won't be rootentity, it'll come from elsewhere
          }(executionContext)
      }
      /*
            ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
        // Evaluate the results per input and return a seq of DBIO[ Map(entity -> value) ], one per input
        val resultsByInput = inputs.toSeq.map { input =>
          evaluator.evalFinalAttribute(workspaceContext, input.expression, Option(input)).asTry.map {
            tryAttribsByEntity =>
              val validationValuesByEntity: Seq[(EntityName, SubmissionValidationValue)] = tryAttribsByEntity match {
                case Failure(regret) =>
                  // The DBIOAction failed - this input expression was not evaluated. Make an error for each entity.
                  entityNames
                    .map((_, SubmissionValidationValue(None, Some(regret.getMessage), input.workflowInput.getName)))
                case Success(attributeMap) =>
                  convertToSubmissionValidationValues(attributeMap, input)
              }
              validationValuesByEntity
          }
        }

       */
    }

    // Combine all input results into a map, then wrap in SubmissionValidationEntityInputs
    Future.sequence(inputFutures).map { resultsByInput =>
      val valuesByEntity: Map[ExpressionEvaluationSupport.EntityName, Seq[SubmissionValidationValue]] =
        resultsByInput.groupBy(_._1).view.mapValues(_.flatMap(_._2)).toMap

      createSubmissionValidationEntityInputs(valuesByEntity)
    }
  }

  def parseLookups(expression: String): Seq[AttributeLookup] = {
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

  // TODO multiple lookups but in a smarter way
  // TODO do we need to care about workspace attributes?
  def lookUpToQuery(workspaceId: UUID, lookups: Seq[AttributeLookup], entityType: String, entityName: String)(implicit
    executionContext: ExecutionContext
  ): ReadAction[Seq[AttributeValue]] =
    // if there is one lookup with no relations
    DBIO
      .sequence(lookups.map { lookup =>
        if (lookup.relations.isEmpty)
          repository.queries
            .queryEntityForAttribute(workspaceId, lookup.attributeName, entityType, entityName)
            .map(Seq(_))
        else
          repository.queries.queryRelationsForAttribute(
            workspaceId,
            lookup.relations(0).getText, // TODO what happens if there are multiple relations
            lookup.attributeName,
            entityType,
            entityName
          )
      })
      .map(_.flatten)

}
