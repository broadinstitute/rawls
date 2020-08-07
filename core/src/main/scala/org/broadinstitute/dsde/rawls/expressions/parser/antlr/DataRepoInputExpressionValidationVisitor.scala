package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{RelationshipModel, SnapshotModel, TableModel}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{AttributeNameContext, EntityLookupContext, RelationContext}
import org.broadinstitute.dsde.rawls.model.ErrorReport

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/** rootEntityType is required to deal with TDR, but it's passed in as an option because enforcing this is a question
  * of validation which is the job of this ValidationVisitor. In other words, all validation rules should be
  * implemented in the ValidationVisitors */
class DataRepoInputExpressionValidationVisitor(rootEntityType: Option[String],
                                               snapshotModel: SnapshotModel)
  extends TerraExpressionBaseVisitor[Try[Unit]] {

  override def defaultResult() = Success(())

  override def aggregateResult(aggregate: Try[Unit], nextResult: Try[Unit]): Try[Unit] = aggregate.flatMap(_ => nextResult)

  // Entity lookup nodes are only allowed if a rootEntityType is specified
  override def visitEntityLookup(ctx: EntityLookupContext): Try[Unit] = {
    rootEntityType match {
      case Some(rootTableName) => validateEntityLookup(rootTableName, ctx)
      case None => Failure(new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, "Expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used.") // it seems like it would get here only if methodConfiguration.rootEntityType is not set, so this error message doesn't make sense to me. Shouldn't it be something like "Missing Root Entity Type. Please select one and try again."
      ))
    }
  }

  // Valid DataRepo EntityLookups mean that each of the relationships exist and the final attribute exists as a column
  // on the final table
  private def validateEntityLookup(rootTableName: String, entityLookupContext: EntityLookupContext): Try[Unit] = {
    (maybeFindTableInSnapshotModel(rootTableName), Option(entityLookupContext.attributeName().namespace())) match {
      case (Success(rootTableModel), None) =>
        val relations = entityLookupContext.relation().asScala.toList
        traverseRelationsAndGetFinalTable(rootTableModel, relations).flatMap(finalTable => checkForAttributeOnTable(finalTable, entityLookupContext.attributeName()))
      case (Failure(regrets), _) => Failure(regrets)
      case (_, Some(_)) => Failure(new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, "Expressions with \"namespace: name\" are not valid for BigQuery")
      ))
    }
  }

  private def checkForAttributeOnTable(tableModel: TableModel, attributeNameContext: AttributeNameContext): Try[Unit] = {
    val tableColumns = tableModel.getColumns.asScala.toList
    val attributeName = attributeNameContext.getText
    if (tableColumns.exists(_.getName == attributeName)) {
      Success()
    } else {
      Failure(new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, s"Missing attribute `${attributeName}` on table `${tableModel.getName}`")
      ))
    }
  }

  private def traverseRelationsAndGetFinalTable(currentTableModel: TableModel, relations: List[RelationContext]): Try[TableModel] = {
    relations match {
      case Nil => Success(currentTableModel)
      case nextRelationContext :: remainingRelations => {
        val nextRelationName = nextRelationContext.attributeName.name.getText
        maybeGetNextTableFromRelation(currentTableModel, nextRelationName).flatMap(traverseRelationsAndGetFinalTable(_, remainingRelations))
      }
    }
  }

  private def maybeFindTableInSnapshotModel(tableName: String): Try[TableModel] = {
    val snapshotTables = snapshotModel.getTables.asScala.toList
    snapshotTables.find(_.getName == tableName) match {
      case None => Failure(new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, s"Table $tableName does not exist in snapshot")
      ))
      case Some(tableModel) => Success(tableModel)
    }
  }

  private def maybeFindRelationshipInSnapshotModel(relationshipName: String): Try[RelationshipModel] = {
    val snapshotRelationships = snapshotModel.getRelationships.asScala.toList
    snapshotRelationships.find(_.getName == relationshipName) match {
      case None => Failure(new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, s"Relationship $relationshipName does not exist in snapshot")
      ))
      case Some(relationshipModel) => Success(relationshipModel)
    }
  }

  private def maybeGetNextTableFromRelation(fromTable: TableModel, relationshipName: String): Try[TableModel] = {
    for {
      relationshipModel <- maybeFindRelationshipInSnapshotModel(relationshipName)
      nextTableName <- fromTable.getName match {
        case forward if forward.equalsIgnoreCase(relationshipModel.getFrom.getTable) => Success(relationshipModel.getTo.getTable)
        case backward if backward.equalsIgnoreCase(relationshipModel.getTo.getTable) => Success(relationshipModel.getFrom.getTable)
        case _ => Failure(new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, s"Table $fromTable does not exist in relationship ${relationshipModel.getName}")
        ))
      }
      nextTable <- maybeFindTableInSnapshotModel(nextTableName)
    } yield {
      nextTable
    }
  }
}
