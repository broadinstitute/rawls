package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import bio.terra.datarepo.model.{SnapshotModel, TableModel}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{AttributeNameContext, EntityLookupContext, RelationContext}

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
      case None => Failure(new RawlsException("Expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."))
    }
  }

  // Valid DataRepo EntityLookups mean that each of the relationships exist and the final attribute exists as a column
  // on the final table
  private def validateEntityLookup(rootTableName: String, entityLookupContext: EntityLookupContext): Try[Unit] = {
    maybeFindTableInSnapshotModel(rootTableName) match {
      case Some(rootTableModel) => {
        val relations = entityLookupContext.relation().asScala.toList
        traverseRelationsAndGetFinalTable(rootTableModel, relations).map(finalTable => checkForAttributeOnTable(finalTable, entityLookupContext.attributeName()))
      }
      case None => Failure(new RawlsException(s"DataRepo Snapshot must include a table with same name as Root Entity Type: ${rootTableName}"))
    }
  }

  private def checkForAttributeOnTable(tableModel: TableModel, attributeNameContext: AttributeNameContext): Try[Unit] = {
    val tableColumns = tableModel.getColumns.asScala.toList
    val attributeName = attributeNameContext.getText
    if (tableColumns.exists(_.getName == attributeName)) {
      Success()
    } else {
      Failure(new RawlsException(s"Missing attribute `${attributeName}` on table `${tableModel.getName}`"))
    }
  }

  @tailrec
  private def traverseRelationsAndGetFinalTable(currentTableModel: TableModel, relations: List[RelationContext]): Try[TableModel] = {
    relations match {
      case nextRelationContext :: remainingRelations => {
        val nextRelationName = nextRelationContext.getText
        maybeGetNextTableFromRelation(currentTableModel, nextRelationName) match {
          case Some(nextTableModel) => traverseRelationsAndGetFinalTable(nextTableModel, remainingRelations)
          case None => Failure(new RawlsException(s"Relationship with name `${nextRelationName}` and from table `${currentTableModel}` could not be found"))
        }
      }
      case Nil => Success(currentTableModel)
    }
  }

  // TODO: this is checking that the `fromTable` exists and that the relationship exists in one spot.  Not sure if we need a separate check for the table and another for the relationship
  // Maybe we don't need to do all that though...it feels like we're getting into the territory of validating the TDR
  // "relationship" schema at some point.  Maybe it's ok to just say, "If a relationship is defined with name X and
  // from_table FOO, then we're going to assume that the RelationshipModel object is valid for this SnapshotModel."
  private def maybeGetNextTableFromRelation(fromTable: TableModel, relationName: String): Option[TableModel] = {
    maybeFindTableInSnapshotModel(fromTable.getName)
//      .map { tableModel =>
      // TODO: Asked TDR team, they are currently implementing the logic for how to get List<RelationshipModel>
      // if (relationship exists with name `relationName` with from_table `fromTable`) then
      //     return Option(getRelationship(relationName).to_table())
      // else
      //     None
//      }
  }

  private def maybeFindTableInSnapshotModel(tableName: String): Option[TableModel] = {
    val snapshotTables = snapshotModel.getTables.asScala.toList
    snapshotTables.find(_.getName == tableName)
  }
}
