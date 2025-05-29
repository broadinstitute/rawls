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
    val entityType = expressionEvaluationContext.entityType.getOrElse(
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityType"))
    )
    val entityName = expressionEvaluationContext.entityName.getOrElse(
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Missing entityName"))
    )
    // TODO will parselookups work if expression is None
    val entityLookups = parseLookups(expressionEvaluationContext.expression.get)

    // TODO when entityType/Name is a set entity (i.e. different from rootEntity) then the expression needs to know to refer to
    // that entity type instead.  is that done in parseLookups or in lookUpToQuery?
    // TODO also parse exevcxt.expression
    // make a view??? that decidse if its an array or not, json type func in sql
    val inputFutures: Seq[Future[(String, Seq[SubmissionValidationValue])]] = {
      gatherInputsResult.processableInputs.toSeq.map { input =>
        val inputLookups = parseLookups(input.expression)
        // TODO lookupToQuery should use both inputLookups and entityLookups
        // we want the result to be records.  for each record we get any attributes out of it that are needed by the expressions parsed
        repository.dataSource
          .inTransaction { _ =>
            lookUpToQuery(workspaceId, inputLookups, entityType, entityName)
          }
          .map { values =>
            val attributeValueList = AttributeValueList(values)

            // Wrap each value in a SubmissionValidationValue
            val validationValues =
              Seq(SubmissionValidationValue(Some(attributeValueList), None, input.workflowInput.getName))
            entityName -> validationValues // TODO this won't be entityName if it's a set entity
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
  // group lookups according to root, add root to attributelookup
  //
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

  /*
  def queryFromLookup

import scala.collection.mutable.LinkedHashMap
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource

case class Relation(relationColName: String, relationEntityType: String)
case class Record(id: String, attributes: Map[String, Any])

class EntityDao(namedTemplate: NamedParameterJdbcTemplate) {

  def queryRelatedRecordsWithArray(
      collectionId: String,
      arrayEntityType: String,
      arrayEntityId: String,
      arrayRelations: List[Relation],
      relations: List[Relation],
      pageSize: Int,
      offset: Int
  ): LinkedHashMap[String, List[Record]] = {
    require(arrayRelations.nonEmpty, "Array relations must not be empty")

    val rootEntityType = arrayRelations.last.relationEntityType
    val queryEntityType = if (relations.isEmpty) rootEntityType else relations.last.relationEntityType

    val sql =
      s"""
         |WITH RECURSIVE entity_hierarchy AS (
         |  SELECT
         |    e.entity_id AS root_id,
         |    e.entity_id,
         |    e.entity_type,
         |    e.attributes
         |  FROM ENTITY e
         |  WHERE e.entity_id = :arrayEntityId AND e.entity_type = :arrayEntityType
         |
         |  UNION ALL
         |
         |  SELECT
         |    h.root_id,
         |    e.entity_id,
         |    e.entity_type,
         |    e.attributes
         |  FROM entity_hierarchy h
         |  JOIN ENTITY e
            ON (
              JSON_UNQUOTE(JSON_EXTRACT(h.attributes, CONCAT('$.attrs.', :relationColName, '.entity_id'))) = e.entity_id
              AND JSON_UNQUOTE(JSON_EXTRACT(h.attributes, CONCAT('$.attrs.', :relationColName, '.entity_type'))) = e.entity_type
            )
            OR (
              JSON_CONTAINS(
                JSON_EXTRACT(h.attributes, CONCAT('$.attrs.', :relationColName)),
                JSON_OBJECT('entity_id', e.entity_id, 'entity_type', e.entity_type)
              )
            )
         |)
         |SELECT root_id, entity_id, entity_type, attributes
         |FROM entity_hierarchy
         |WHERE entity_type = :queryEntityType
         |LIMIT :pageSize OFFSET :offset
       """.stripMargin

    val params = new MapSqlParameterSource()
      .addValue("arrayEntityId", arrayEntityId)
      .addValue("arrayEntityType", arrayEntityType)
      .addValue("relationColName", arrayRelations.head.relationColName) // Adjust for dynamic relations
      .addValue("queryEntityType", queryEntityType)
      .addValue("pageSize", pageSize)
      .addValue("offset", offset)

    val results = namedTemplate.query(sql, params, (rs, _) => {
      Record(
        id = rs.getString("entity_id"),
        attributes = Map(
          "entity_type" -> rs.getString("entity_type"),
          "attributes" -> rs.getString("attributes")
        )
      )
    })

    results
      .groupBy(_.id)
      .map { case (rootId, records) => rootId -> records.toList }
      .to(LinkedHashMap)
  }
}
   */
}
