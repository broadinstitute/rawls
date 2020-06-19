package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.TableModel
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeBoolean, AttributeName, AttributeNumber, AttributeString, AttributeValue, AttributeValueEmptyList, AttributeValueList, Entity}

import scala.collection.JavaConverters._

trait DataRepoBigQuerySupport {

  def pkFromSnapshotTable(tableModel: TableModel): String = {
    // If data repo returns one and only one primary key, use it.
    // If data repo returns null or a compound PK, use the built-in rowid for pk instead.
    scala.Option(tableModel.getPrimaryKey) match {
      case Some(pk) if pk.size() == 1 => pk.asScala.head
      case _ => "datarepo_row_id" // default data repo value
    }
  }

  def fieldValueToAttributeValue(field: Field, fv: FieldValue): AttributeValue = {
    field.getType match {
      case LegacySQLTypeName.FLOAT | LegacySQLTypeName.INTEGER | LegacySQLTypeName.NUMERIC =>
        AttributeNumber(fv.getNumericValue)
      case LegacySQLTypeName.BOOLEAN =>
        AttributeBoolean(fv.getBooleanValue)
      case LegacySQLTypeName.STRING =>
        AttributeString(fv.getStringValue)
      case _ =>
        // DATE, DATETIME, TIME, TIMESTAMP
        // BYTES
        // GEOGRAPHY
        // RECORD
        // we don't necessarily support these data types, but we'll try:
        AttributeString(fv.getValue.toString)
    }
  }

  def fieldToAttribute(field: Field, row: FieldValueList): (AttributeName, Attribute) = {
    val attrName = field.getName
    // TODO: catch exceptions retrieving values from queryResults
    val fv = row.get(attrName)

    val attribute = field.getMode match {
      case Mode.REPEATED =>
        fv.getRepeatedValue.asScala.toList match {
          case x if x.isEmpty => AttributeValueEmptyList
          case members =>
            // TODO: can a list itself contain a list? Recursion here is difficult because of the Attribute/AttributeValue hierarchy
            val attrValues = members.map { member => fieldValueToAttributeValue(field, member) }
            AttributeValueList(attrValues)
        }
      case _ => fieldValueToAttributeValue(field, fv)
    }

    (AttributeName.withDefaultNS(attrName), attribute)
  }

  def queryResultsToEntities(queryResults:TableResult, entityType: String, entityName: String): List[Entity] = {
    val fieldDefs:List[Field] = queryResults.getSchema.getFields.iterator().asScala.toList

    queryResults.iterateAll().asScala.map { row =>
      val attrs = fieldDefs.map { field => fieldToAttribute(field, row) }.toMap
      Entity(entityName, entityType, attrs)
    }.toList
  }

  def queryResultsToEntity(queryResults:TableResult, entityType: String, entityName: String): Entity = {
    queryResults.getTotalRows match {
      case 1 =>
        queryResultsToEntities(queryResults, entityType, entityName).head
      case 0 =>
        // TODO: better error messages
        throw new DataEntityException("BQ succeeded, but returned zero rows") // error not found
      case _ =>
        // TODO: better error messages
        throw new DataEntityException(s"BQ succeeded, but returned ${queryResults.getTotalRows} rows") // unexpected error: too many found
    }
  }

}
