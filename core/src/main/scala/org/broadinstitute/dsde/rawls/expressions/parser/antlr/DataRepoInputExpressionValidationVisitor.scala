package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import bio.terra.datarepo.model.{SnapshotModel, TableModel}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.{AttributeNameContext, EntityLookupContext, RelationContext}

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
        traverseRelationsAndGetFinalTable(rootTableModel, relations).flatMap(finalTable => checkForAttributeOnTable(finalTable, entityLookupContext.attributeName()))
      }
      case None => Failure(new RawlsException(s"Root entity type [$rootTableName] is not a name of a table that exist within DataRepo Snapshot."))
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

  // TODO: CA-939 support relationships once that information is published by data repo
  private def traverseRelationsAndGetFinalTable(currentTableModel: TableModel, relations: List[RelationContext]): Try[TableModel] = {
    relations match {
      case Nil => Success(currentTableModel)
      case _ => Failure(new RawlsException(s"Relationships are not currently supported."))
    }
  }

  private def maybeFindTableInSnapshotModel(tableName: String): Option[TableModel] = {
    val snapshotTables = snapshotModel.getTables.asScala.toList
    snapshotTables.find(_.getName == tableName)
  }
}
