package org.broadinstitute.dsde.rawls.entities.datarepo

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * contains helper methods for working with BigQuery from an EntityProvider.
 */
trait DataRepoBigQuerySupport {

  /**
   * translate a single BigQuery FieldValue to a single Rawls AttributeValue
   *
   * @param field the BQ schema's field definition
   * @param fv the BQ field value
   * @return the Rawls attribute value, post-translation
   */
  def fieldValueToAttributeValue(field: Field, fv: FieldValue): AttributeValue = {
    field.getType match {
      case x if fv.isNull =>
        AttributeNull
      case LegacySQLTypeName.FLOAT | LegacySQLTypeName.INTEGER | LegacySQLTypeName.NUMERIC =>
        AttributeNumber(fv.getNumericValue)
      case LegacySQLTypeName.BOOLEAN =>
        AttributeBoolean(fv.getBooleanValue)
      case LegacySQLTypeName.STRING =>
        AttributeString(fv.getStringValue)
      case LegacySQLTypeName.RECORD =>
        // TODO: unclear what to do with RECORD types; they don't translate cleanly to the entity model
        AttributeString(fv.getValue.toString)
      case _ =>
        // "else" case that covers LegacySQLTypeNames of:
        // DATE, DATETIME, TIME, TIMESTAMP, BYTES, GEOGRAPHY
        // these types don't have strongly-typed equivalents in the entity model, so we treat them
        // as string values, and rely on the caller to parse the string
        AttributeString(fv.getValue.toString)
    }
  }

  /**
   * Given a BigQuery row and one named field from the BQ schema, return a Rawls AttributeName->Attribute pair
   * representing that field's value(s) inside the row.
   *
   * @param field the BQ schema's field definition
   * @param row all field values for a single BQ row
   * @return the Rawls attributename->attribute pair, post-translation
   */
  def fieldToAttribute(field: Field, row: FieldValueList): (AttributeName, Attribute) = {
    val attrName = field.getName
    val fv = Try(row.get(attrName)) match {
      case Success(fieldValue) => fieldValue
      case Failure(ex) =>
        throw new DataEntityException(s"field ${field.getName} not found in row ${asDelimitedString(row)}")
    }

    val attribute = field.getMode match {
      case Mode.REPEATED =>
        fv.getRepeatedValue.asScala.toList match {
          case x if x.isEmpty => AttributeValueEmptyList
          case members =>
            // can a list itself contain a list? Recursion here is difficult because of the Attribute/AttributeValue hierarchy
            val attrValues = members.map { member => fieldValueToAttributeValue(field, member) }
            AttributeValueList(attrValues)
        }
      case _ => fieldValueToAttributeValue(field, fv)
    }

    (AttributeName.withDefaultNS(attrName), attribute)
  }

  /**
   * Translates a BigQuery result set into a list of Rawls Entities. Returns an empty list if BigQuery returned zero rows.
   *
   * @param queryResults the BigQuery result set
   * @param entityType name of a BigQuery table in the result set, used as the Rawls entity type.
   * @param primaryKey what Rawls believes is the identifying column in the BigQuery table, used to generate entity names.
   * @return list of Rawls Entities
   */
  def queryResultsToEntities(queryResults:TableResult, entityType: String, primaryKey: String): List[Entity] = {
    // short-circuit if query results is empty
    if (queryResults.getTotalRows == 0) {
      List.empty[Entity]
    } else {

      val schemaFields = queryResults.getSchema.getFields

      // does primary key exist in the results?
      if (Try(schemaFields.getIndex(primaryKey)).isFailure) {
        throw new DataEntityException(s"could not find primary key column '$primaryKey' in query results: ${asDelimitedString(schemaFields)}")
      }

      // short-circuit if query results is empty
      if (queryResults.getTotalRows == 0) {
        List.empty[Entity]
      } else {
        val fieldDefs:List[Field] = schemaFields.iterator().asScala.toList
        queryResults.iterateAll().asScala.map { row =>
          val attrs = fieldDefs.map { field => fieldToAttribute(field, row) }.toMap
          val entityName = attrs.find(_._1.name == primaryKey) match {
            case Some((_, a: Attribute)) => AttributeStringifier.apply(a)
            case None =>
              // this shouldn't happen, since we validated the pk against the schema
              throw new DataEntityException(s"could not find primary key column '$primaryKey' in row: ${asDelimitedString(row)}")
          }
          Entity(entityName, entityType, attrs)
        }.toList
      }
    }
  }

  /**
   * Translates a BigQuery result set into a single Rawls Entity. Expects one and only one row from BigQuery
   * and will throw exceptions if less than or more than one row exists.
   *
   * @param queryResults the BigQuery result set
   * @param entityType name of a BigQuery table in the result set, used as the Rawls entity type.
   * @param primaryKey what Rawls believes is the identifying column in the BigQuery table, used to generate entity names.
   * @return a single Rawls Entity
   */
  def queryResultsToEntity(queryResults:TableResult, entityType: String, primaryKey: String): Entity = {
    queryResults.getTotalRows match {
      case 1 =>
        queryResultsToEntities(queryResults, entityType, primaryKey).head
      case 0 =>
        throw new EntityNotFoundException("Entity not found.")
      case _ =>
        throw new DataEntityException(s"Query succeeded, but returned ${queryResults.getTotalRows} rows; expected one row.")
    }
  }

  // create comma-delimited string of field names for use in error messages.
  // list is sorted alphabetically for determinism in unit tests.
  def asDelimitedString(fieldList: FieldList): String = {
    fieldList.asScala.toList.map(_.getName).sorted.mkString(",")
  }
  def asDelimitedString(fieldValueList: FieldValueList): String = {
    fieldValueList.asScala.toList.map(_.toString).sorted.mkString(",")
  }

}
