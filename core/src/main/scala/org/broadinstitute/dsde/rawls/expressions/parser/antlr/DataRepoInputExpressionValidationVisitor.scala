package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{SnapshotModel, TableModel}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{AttributeNameContext, EntityLookupContext, RelationContext}
import org.broadinstitute.dsde.rawls.model.ErrorReport

import scala.annotation.tailrec
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

  override def visitNamespace(ctx: TerraExpressionParser.NamespaceContext): Try[Unit] = {
    rootEntityType match {
      case Some(_) => {
        val maybeParent = Option(ctx.parent.parent.getChild(0).getText)
        maybeParent match {
          case Some(maybeWorkspace) => {
            if (maybeWorkspace == "workspace.")
              super.visitNamespace(ctx)
            else
              Failure(new RawlsExceptionWithErrorReport(
                ErrorReport(StatusCodes.BadRequest, "Expressions with \"namespace: name\" are not valid for BigQuery")
              ))
          }
          case None => Failure(new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, "Expressions with \"namespace: name\" are not valid for BigQuery")
          ))
        }

      }
      case None => super.visitNamespace(ctx)
    }
  }

  // Valid DataRepo EntityLookups mean that each of the relationships exist and the final attribute exists as a column
  // on the final table
  private def validateEntityLookup(rootTableName: String, entityLookupContext: EntityLookupContext): Try[Unit] = {
    maybeFindTableInSnapshotModel(rootTableName) match {
      case Some(rootTableModel) => {
        val relations = entityLookupContext.relation().asScala.toList
        traverseRelationsAndGetFinalTable(rootTableModel, relations).flatMap(finalTable => checkForAttributeOnTable(finalTable, entityLookupContext.attributeName()))
      }
      case None => Failure(new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadRequest, s"Root entity type [$rootTableName] is not a name of a table that exist within DataRepo Snapshot.")
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

  @tailrec
  private def traverseRelationsAndGetFinalTable(currentTableModel: TableModel, relations: List[RelationContext]): Try[TableModel] = {
    relations match {
      case Nil => Success(currentTableModel)
      case nextRelationContext :: remainingRelations => {
        val nextRelationName = nextRelationContext.attributeName.name.getText
        maybeGetNextTableFromRelation(currentTableModel, nextRelationName) match {
          case Some(nextTableModel) => traverseRelationsAndGetFinalTable(nextTableModel, remainingRelations)
          case None => Failure(new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.BadRequest, s"Relationship with name `${nextRelationName}` and from table `${currentTableModel}` could not be found.") // This would only happen if there is a bug in TDR code.
          ))
        }
      }
    }
  }

  private def maybeFindTableInSnapshotModel(tableName: String): Option[TableModel] = {
    val snapshotTables = snapshotModel.getTables.asScala.toList
    snapshotTables.find(_.getName == tableName)
  }

  private def maybeGetNextTableFromRelation(fromTable: TableModel, relationshipName: String): Option[TableModel] = {
    val relationships = snapshotModel.getRelationships.asScala.toList
    for {
      tableModel <- maybeFindTableInSnapshotModel(fromTable.getName)
      relationship <- relationships.find {relationship =>
        relationship.getFrom.getTable == tableModel.getName && relationship.getName == relationshipName}
      nextTableName = relationship.getTo.getTable
      nextTable <- maybeFindTableInSnapshotModel(nextTableName)
    } yield {
      nextTable
    }
  }
}
