package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
import io.circe.Json
import io.circe.parser.parse
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, ReadAction}
import org.broadinstitute.dsde.rawls.entities.base.{ExpressionEvaluationContext, ExpressionEvaluationSupport}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, CompactEvaluateVisitor}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  Attribute,
  AttributeName,
  AttributeNull,
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

  // TODO replace with queryRelatedRecordsWithArray
  def evaluateExpression(workspaceId: UUID, expression: String, entityType: String, entityName: String)(implicit
    executionContext: ExecutionContext
  ): Future[Seq[AttributeValue]] = {
    val lookup = parseLookups(expression)

    repository.dataSource.inTransaction { _ =>
      lookUpToQuery(workspaceId, lookup, entityType, entityName)
    }
  }

  // TODO alllllll the error handling, correctly
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

    val entityLookups = expressionEvaluationContext.expression
      .map(parseLookups)
      .getOrElse(Seq.empty)

    // TODO if there are multiple inputs, how does that affect the query?
    val inputFutures =
      gatherInputsResult.processableInputs.toSeq.map { input =>
        val inputLookups = parseLookups(input.expression)
        val inputMap = inputLookups.map(_ -> input.workflowInput.getName)

        if (entityLookups.isEmpty) {
          repository.dataSource
            .inTransaction { _ =>
              repository.queries
                .getEntity(workspaceId, entityType, entityName)
            }
            .map {
              case Some(record) =>
                // Convert the single CompactEntityRecord into a Map for buildValidationInputs
                val entityRecords = Map(entityName -> Seq(record))
                buildValidationInputs(entityRecords, inputMap)
              case None =>
                LazyList.empty[SubmissionValidationEntityInputs]
            }
        } else {
          repository.dataSource
            .inTransaction { _ =>
              repository.queries.queryRelatedRecordsWithArray(workspaceId,
                                                              entityType,
                                                              entityName,
                                                              entityLookups,
                                                              inputLookups
              )
            }
            .map { entityRecords =>
              buildValidationInputs(entityRecords, inputMap)
            }
        }
      }
//TODO it makes sense for the entityName to be that of the actual entities
    // But in the case of an entity set should be it be the name of the set entity??
    Future.sequence(inputFutures).map { results =>
      results.flatten
        .groupBy(_.entityName) // Group by entityName
        .map { case (entityName, inputs) =>
          SubmissionValidationEntityInputs(
            entityName = entityName,
            inputResolutions = inputs.flatMap(_.inputResolutions).toSet // Combine all SubmissionValidationValue sets
          )
        }
        .to(LazyList)
    }
  }

  def buildValidationInputs(
    entityRecords: Map[String, Seq[CompactEntityRecord]],
    lookupsWithInputNames: Seq[(AttributeLookup, String)]
  ): LazyList[SubmissionValidationEntityInputs] =
    // TODO why do i have a list of records for each entity again??
    entityRecords
      .map { case (entityName, records) =>
        // Use the first record for attribute extraction (or adjust as needed)
        val record = records.head.toEntity

        val validationValues: Set[SubmissionValidationValue] = lookupsWithInputNames.map { case (lookup, inputName) =>
          val attrName = AttributeName.fromDelimitedName(lookup.attributeName)
          val attrValue: Option[Attribute] = record.attributes.get(attrName)
          SubmissionValidationValue(
            value = attrValue.orElse(Some(AttributeNull)),
            error = None,
            inputName = inputName
          )
        }.toSet

        SubmissionValidationEntityInputs(
          entityName = entityName,
          inputResolutions = validationValues
        )
      }
      .to(LazyList)

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
