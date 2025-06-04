package org.broadinstitute.dsde.rawls.expressions

import akka.http.scaladsl.model.StatusCodes
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

  // TODO figure out examples and correct answers, then write tests
  def evaluateExpression(workspaceId: UUID, expression: String, entityType: String, entityName: String)(implicit
    executionContext: ExecutionContext
  ): Future[Seq[AttributeValue]] = {
    val lookups = parseLookups(expression)

    repository.dataSource
      .inTransaction { _ =>
        repository.queries.queryRelatedRecordsWithArray(workspaceId, entityType, entityName, lookups, Seq.empty)
      }
      .map { entityRecords =>
        // It's easier to pick out individual attributes from an Entity
        val allRecords = entityRecords.values.flatten.toSeq.map(_.toEntity)
        lookups.flatMap { lookup =>
          val attrName = AttributeName.fromDelimitedName(lookup.attributeName)
          val attrs: Seq[Attribute] = allRecords.flatMap(_.attributes.get(attrName))
          attrs.flatMap {
            case avl: AttributeValueList => avl.list
            case av: AttributeValue      => Seq(av)
            case _                       => Seq.empty
          }
        }
      }
  }

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
      val rootEntityType = rootEntityTypeOpt.get
      // TODO if there are multiple inputs, how does that affect the query?
      val inputFutures =
        gatherInputsResult.processableInputs.toSeq.map { input =>
          val inputLookups = parseLookups(input.expression)
          val inputMap = inputLookups.map(_ -> input.workflowInput.getName)
          // If there's an expression, evaluate it to get the list of entities to run this job on.
          // Otherwise, use the entity given in the submission.
          expressionEvaluationContext.expression match {
            case None =>
              repository.dataSource
                .inTransaction { _ =>
                  repository.queries
                    .getEntity(workspaceId, entityType, entityName)
                }
                .map {
                  case Some(record) =>
                    // Convert the single CompactEntityRecord into a Map for buildValidationInputs
                    val entityRecords = Map(entityName -> Seq(record))
                    buildValidationInputs(entityRecords, inputMap, rootEntityType, entityName)
                  case None =>
                    LazyList.empty[SubmissionValidationEntityInputs]
                }
            case Some(expression) =>
              val entityLookups = parseLookups(expression)
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
                  buildValidationInputs(entityRecords, inputMap, rootEntityType, entityName)
                }

          }
        }
      Future.sequence(inputFutures).map { results =>
        results.flatten
          .groupBy(_.entityName)
          .map { case (entityName, inputs) =>
            SubmissionValidationEntityInputs(
              entityName = entityName,
              inputResolutions = inputs.flatMap(_.inputResolutions).toSet
            )
          }
          .to(LazyList)
      }
    }
  }

  // TODO i'm really not sure i'm doing this rootEntityType thing correctly
  def buildValidationInputs(
    entityRecords: Map[String, Seq[CompactEntityRecord]],
    lookupsWithInputNames: Seq[(AttributeLookup, String)],
    rootEntityType: String,
    rootEntityName: String
  ): LazyList[SubmissionValidationEntityInputs] = {
    // Flatten all records to check their types
    val allRecords = entityRecords.values.flatten.toSeq

    // If all records are of the rootEntityType, use their names as the entityNames
    if (allRecords.forall(_.entityType == rootEntityType)) {

      // TODO why do i have a list of records for each entity again??
      entityRecords
        .map { case (entityName, records) =>
          val record =
            records.head.toEntity // TODO I think I only care about the first record, so I shouldn't need a list should I??

          val validationValues: Set[SubmissionValidationValue] = lookupsWithInputNames.map { case (lookup, inputName) =>
            val attrName = AttributeName.fromDelimitedName(lookup.attributeName)
            val attrValue: Option[Attribute] = record.attributes.get(attrName)
            // TODO What should the error message be?  When is it an error and not just null?
            val error =
              if (record.attributes.contains(attrName)) None else Some("This attribute does not exist on this entity")

            SubmissionValidationValue(
              value = attrValue,
              error = error,
              inputName = inputName
            )
          }.toSet

          SubmissionValidationEntityInputs(
            entityName = entityName,
            inputResolutions = validationValues
          )
        }
        .to(LazyList)
    } else {
      // Group all attribute values for each input into an AttributeValueList
      val validationValues: Set[SubmissionValidationValue] = lookupsWithInputNames.map { case (lookup, inputName) =>
        val attrName = AttributeName.fromDelimitedName(lookup.attributeName)
        val attrs: Seq[Attribute] = allRecords.map(_.toEntity).flatMap(_.attributes.get(attrName))
        val value: Option[Attribute] =
          if (attrs.isEmpty) None
          else if (attrs.length == 1) {
            attrs.head match {
              case avl: AttributeValueList => Some(avl)
              case av: AttributeValue      => Some(AttributeValueList(Seq(av)))
              case _                       => None
            }
          } else {
            val allValues: Seq[AttributeValue] = attrs.flatMap {
              case avl: AttributeValueList => avl.list
              case av: AttributeValue      => Seq(av)
              case _                       => Seq.empty
            }
            Some(AttributeValueList(allValues))
          }
        val error = if (value.nonEmpty) None else Some("No attributes found for this input")
        SubmissionValidationValue(
          value = value,
          error = error,
          inputName = inputName
        )
      }.toSet

      LazyList(
        SubmissionValidationEntityInputs(
          entityName = rootEntityName,
          inputResolutions = validationValues
        )
      )
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

  // TODO delete
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
            lookup.relations(0).getText,
            lookup.attributeName,
            entityType,
            entityName
          )
      })
      .map(_.flatten)

}
