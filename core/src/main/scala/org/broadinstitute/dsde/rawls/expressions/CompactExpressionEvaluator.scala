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

class CompactExpressionEvaluator(repository: CompactEntityRepository) extends ExpressionEvaluationSupport {

  def evaluateExpression(workspaceId: UUID, expression: String, entityType: String, entityName: String)(implicit
    executionContext: ExecutionContext
  ): Future[Seq[AttributeValue]] = {
    // We need to get the parse tree to use for final input; can we extract this to a separate method somehow?
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new CompactEvaluateVisitor()
    val parsedTree = terraExpressionParser.root()
    val lookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)

    repository.dataSource
      .inTransaction { _ =>
        repository.queries.queryRelatedRecordsWithArray(workspaceId, entityType, entityName, lookups, Seq.empty)
      }
      .map { entityRecords =>
        val allRecords = entityRecords.values.flatten.toSeq.map(_.toEntity)

        lookups.map { lookup =>
          val attrName = AttributeName.fromDelimitedName(lookup.attributeName.get)
          val attrs: Seq[AttributeValue] = allRecords.flatMap(_.attributes.get(attrName)).flatMap {
            case avl: AttributeValueList => avl.list
            case av: AttributeValue      => Seq(av)
            case _                       => Seq.empty
          }
          //  type ExpressionAndResult = (LookupExpression, Map[EntityName, Try[Iterable[AttributeValue]]])
          (lookup.expression, Map(entityName -> Success(attrs)))
        }
      }
      .map { exprAndResults =>
        // Use InputExpressionReassembler to get the final result
        InputExpressionReassembler.constructFinalInputValues(
          exprAndResults,
          parsedTree,
          Some(Seq(entityName)),
          None
        )
      }
      .map { resultMap =>
        resultMap.getOrElse(entityName, Success(Seq.empty)).get.toSeq

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
          val entityLookups = parseLookups(expression)
          val inputFutures: Seq[Future[Seq[(ExpressionEvaluationSupport.EntityName, SubmissionValidationValue)]]] =
            gatherInputsResult.processableInputs.toSeq.map { input =>
              val terraExpressionParser = AntlrTerraExpressionParser.getParser(input.expression)
              val visitor = new CompactEvaluateVisitor()
              val parsedTree = terraExpressionParser.root()
              val inputLookups: Seq[ExpressionLookup] = visitor.visit(parsedTree)
              // TODO currently runs a query for each input.  should presumably only run one query
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
                  val rootEntityType = rootEntityTypeOpt.get
                  val allRecords = entityRecords.values.flatten.toSeq
                  val recordsMatchRootEntity = allRecords.forall(_.entityType == rootEntityType)
                  val exprAndResults = inputLookups.map { lookup =>
                    val attrNameOpt = lookup.attributeName
                    // If the rootEntityType isn't the same as the entityRecords, then we need to map all the different records' values to the rootEntity
                    val entityValueMap: Map[String, Try[Iterable[AttributeValue]]] =
                      if (recordsMatchRootEntity) {
                        // Each entity gets its own values as before
                        entityRecords.map { case (eName, records) =>
                          val values: Iterable[AttributeValue] = attrNameOpt match {
                            case Some(attrName) =>
                              records
                                .map(_.toEntity)
                                .flatMap(_.attributes.get(AttributeName.fromDelimitedName(attrName)))
                                .flatMap {
                                  case avl: AttributeValueList => avl.list
                                  case av: AttributeValue      => Seq(av)
                                  case _                       => Seq.empty
                                }
                            case None =>
                              lookup.values
                          }
                          eName -> Success(values)
                        }
                      } else {
                        val values: Iterable[AttributeValue] = attrNameOpt match {
                          case Some(attrName) =>
                            allRecords
                              .map(_.toEntity)
                              .flatMap(_.attributes.get(AttributeName.fromDelimitedName(attrName)))
                              .flatMap {
                                case avl: AttributeValueList => avl.list
                                case av: AttributeValue      => Seq(av)
                                case _                       => Seq.empty
                              }
                          case None =>
                            lookup.values
                        }
                        Map(entityName -> Success(values))
                      }
                    (lookup.expression, entityValueMap)
                  }
                  // Use InputExpressionReassembler to get the final result
                  val rootEntityNames = if (recordsMatchRootEntity) entityRecords.keys.toSeq else Seq(entityName)
                  val resultMap = InputExpressionReassembler.constructFinalInputValues(
                    exprAndResults,
                    parsedTree,
                    Some(rootEntityNames),
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

  // TODO i'm really not sure i'm doing this rootEntityType thing correctly
//  def buildValidationInputs(
//    entityRecords: Map[String, Seq[CompactEntityRecord]],
//    lookupsWithInputNames: Seq[(ExpressionLookup, String)],
//    rootEntityType: String,
//    rootEntityName: String
//  ): LazyList[SubmissionValidationEntityInputs] = {
//    // Flatten all records to check their types
//    val allRecords = entityRecords.values.flatten.toSeq
//
//    // If all records are of the rootEntityType, use their names as the entityNames
//    if (allRecords.forall(_.entityType == rootEntityType)) {
//
//      // TODO why do i have a list of records for each entity again??
//      entityRecords
//        .map { case (entityName, records) =>
//          val record =
//            records.head.toEntity // TODO I think I only care about the first record, so I shouldn't need a list should I??
//
//          val validationValues: Set[SubmissionValidationValue] = lookupsWithInputNames.map { case (lookup, inputName) =>
//            val attrName = AttributeName.fromDelimitedName(lookup.attributeName.get)
//            val attrValue: Option[Attribute] = record.attributes.get(attrName)
//            // TODO What should the error message be?  When is it an error and not just null?
//            val error =
//              if (record.attributes.contains(attrName)) None else Some("This attribute does not exist on this entity")
//
//            SubmissionValidationValue(
//              value = attrValue,
//              error = error,
//              inputName = inputName
//            )
//          }.toSet
//
//          SubmissionValidationEntityInputs(
//            entityName = entityName,
//            inputResolutions = validationValues
//          )
//        }
//        .to(LazyList)
//    } else {
//      // Group all attribute values for each input into an AttributeValueList
//      val validationValues: Set[SubmissionValidationValue] = lookupsWithInputNames.map { case (lookup, inputName) =>
//        val attrName = AttributeName.fromDelimitedName(lookup.attributeName.get)
//        val attrs: Seq[Attribute] = allRecords.map(_.toEntity).flatMap(_.attributes.get(attrName))
//        val value: Option[Attribute] =
//          if (attrs.isEmpty) None
//          else if (attrs.length == 1) {
//            attrs.head match {
//              case avl: AttributeValueList => Some(avl)
//              case av: AttributeValue      => Some(AttributeValueList(Seq(av)))
//              case _                       => None
//            }
//          } else {
//            val allValues: Seq[AttributeValue] = attrs.flatMap {
//              case avl: AttributeValueList => avl.list
//              case av: AttributeValue      => Seq(av)
//              case _                       => Seq.empty
//            }
//            Some(AttributeValueList(allValues))
//          }
//        val error = if (value.nonEmpty) None else Some("No attributes found for this input")
//        SubmissionValidationValue(
//          value = value,
//          error = error,
//          inputName = inputName
//        )
//      }.toSet
//
//      LazyList(
//        SubmissionValidationEntityInputs(
//          entityName = rootEntityName,
//          inputResolutions = validationValues
//        )
//      )
//    }
//  }

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

}
