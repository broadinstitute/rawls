package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException, IllegalIdentifierException}
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

// object so we don't recompile this regex for each instance of DataRepoEntityProvider
object DataRepoBigQuerySupport {
  val illegalBQChars: Regex = """[^a-zA-Z0-9\-_]""".r
}

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
   * Given a BigQuery row and one named field from the BQ schema, return a Rawls Attribute
   * representing that field's value(s) inside the row.
   *
   * @param field the BQ schema's field definition
   * @param row all field values for a single BQ row
   * @return the Rawls attribute, post-translation
   */
  def fieldToAttribute(field: Field, row: FieldValueList): Attribute = {
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

    attribute
  }

  def fieldToAttributeName(field: Field): AttributeName = AttributeName.withDefaultNS(field.getName)

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
          val attrs = fieldDefs.map { field => fieldToAttributeName(field) -> fieldToAttribute(field, row) }.toMap
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

  /**
   * Translates a BigQuery result set into the pagination metadata that Rawls expects.
   * @param queryResults the BigQuery result set
   * @param entityQuery the query criteria supplied by the user, which includes page size
   * @return the Rawls-flavor pagination metadata
   */
  def queryResultsMetadata(queryResults:TableResult, entityQuery: EntityQuery): EntityQueryResultMetadata = {
    val totalRowCount = queryResults.getTotalRows

    val pageCount = Math.ceil(totalRowCount.toFloat / entityQuery.pageSize).toInt

    // we don't support filtering in BQ, so unfilteredCount and filteredCount are the same
    EntityQueryResultMetadata(totalRowCount.toInt, totalRowCount.toInt, pageCount)
  }

  /**
   * Given an EntityQuery, which contains page number and page size, generate the correct absolute pagination offset for use in BQ
   * @param entityQuery the query criteria supplied by the user
   * @return the offset value to use in an OFFSET sql clause
   */
  def translatePaginationOffset(entityQuery: EntityQuery): Int = {
    if (entityQuery.page < 1)
      throw new DataEntityException("page value must be at least 1.", code = StatusCodes.BadRequest)
    (entityQuery.page-1) * entityQuery.pageSize
  }

  /**
   * generates the SQL and config to send to BigQuery for use in the queryEntities() method
   * @param dataProject project containing the BQ data
   * @param viewName dataset/snapshot to query in BQ
   * @param entityType table to query in BQ
   * @param entityQuery user-supplied query criteria
   * @return the object to pass to BQ to execute a query
   */
  def queryConfigForQueryEntities(dataProject: String, viewName: String, entityType: String, entityQuery: EntityQuery): QueryJobConfiguration = {
    // generate BQ SQL for this entity
    val query = s"SELECT * FROM `${validateSql(dataProject)}.${validateSql(viewName)}.${validateSql(entityType)}` " +
      s"ORDER BY ${validateSql(entityQuery.sortField)} ${SortDirections.toSql(entityQuery.sortDirection)} " +
      s"LIMIT ${entityQuery.pageSize} " +
      s"OFFSET ${translatePaginationOffset(entityQuery).toLong};"

    // generate query config
    QueryJobConfiguration.newBuilder(query)
      .build
  }

  /**
   * Checks for illegal/undesired characters in a string so that it is safe to use inside
   * a BigQuery SQL statement. Throws an error if illegal characters exist.
   *
   * BQ does not support bind parameters everywhere we want to use them.
   * Per https://cloud.google.com/bigquery/docs/parameterized-queries,
   * "Parameters cannot be used as substitutes for identifiers, column names,
   * table names, or other parts of the query."
   *
   * Therefore we must validate any dynamic strings we want to use in those places.
   *
   * Google's doc on table naming (https://cloud.google.com/bigquery/docs/tables) says:
   *   - Contain up to 1,024 characters
   *   - Contain letters (upper or lower case), numbers, and underscores
   * Google's doc on dataset naming (https://cloud.google.com/bigquery/docs/datasets) has the exact same requirements
   * Google's doc on view naming (https://cloud.google.com/bigquery/docs/views) has the exact same requirements
   * Google's doc on project naming (https://cloud.google.com/resource-manager/docs/creating-managing-projects) says:
   *   - The project ID must be a unique string of 6 to 30 lowercase letters, digits, or hyphens. It must start with a letter, and cannot have a trailing hyphen.
   * Google's doc on column naming (https://cloud.google.com/bigquery/docs/schemas) says:
   *   - A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_), and it must start with a letter or underscore. The maximum column name length is 128 characters
   *
   * Given those naming requirements, this function allows ONLY: letters, numbers, underscores, hyphens.
   * It does not impose any length restrictions.
   *
   * @param input the string to be validated
   * @return the original string, if valid; throws an error if invalid.
   */
  def validateSql(input: String): String = {
    if (input == null || DataRepoBigQuerySupport.illegalBQChars.findFirstIn(input).isDefined) {
      val inputSubstring = scala.Option(input).getOrElse("null").take(64)
      val sanitizedForOutput = DataRepoBigQuerySupport.illegalBQChars.replaceAllIn(inputSubstring, "_")
      throw new IllegalIdentifierException(s"Illegal identifier used in BigQuery SQL. Original input was like [$sanitizedForOutput]")
    } else {
      input
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
