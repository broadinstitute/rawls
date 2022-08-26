package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{RelationshipModel, SnapshotModel, TableModel}
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.{Option => _, _}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.entities.datarepo.DataRepoBigQuerySupport._
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  EntityNotFoundException,
  EntityTypeNotFoundException,
  IllegalIdentifierException
}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ParsedEntityLookupExpression
import org.broadinstitute.dsde.rawls.model._

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object DataRepoBigQuerySupport {
  val illegalBQChars: Regex = """[^a-zA-Z0-9\-_]""".r
  val datarepoRowIdColumn = "datarepo_row_id"

  def tableNameInQuery(dataProject: String,
                       viewName: String,
                       tableName: String,
                       alias: Option[String] = None
  ): String = {
    val aliasString = alias.map(a => s" `${validateSql(a)}`").getOrElse("")
    s"`${validateSql(dataProject)}.${validateSql(viewName)}.${validateSql(tableName)}`$aliasString"
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
  def validateSql(input: String): String =
    if (input == null || DataRepoBigQuerySupport.illegalBQChars.findFirstIn(input).isDefined) {
      val inputSubstring = Option(input).getOrElse("null").take(64)
      val sanitizedForOutput = DataRepoBigQuerySupport.illegalBQChars.replaceAllIn(inputSubstring, "_")
      throw new IllegalIdentifierException(
        s"Illegal identifier used in BigQuery SQL. Original input was like [$sanitizedForOutput]"
      )
    } else {
      input
    }

  def getTableModel(snapshotModel: SnapshotModel, entityType: String): TableModel =
    snapshotModel.getTables.asScala.find(_.getName == entityType) match {
      case Some(table) => table
      case None        => throw new EntityTypeNotFoundException(requestedType = entityType)
    }

  def getRelationshipModel(snapshotModel: SnapshotModel, relationshipName: String): RelationshipModel =
    snapshotModel.getRelationships.asScala.find(_.getName == relationshipName) match {
      case Some(relationshipModel) => relationshipModel
      case None                    => throw new DataEntityException(s"relationship $relationshipName does not exist")
    }

  /*
  The following set of case classes are used as an intermediate representation derived from a set of entity lookup
  expressions. The intermediate representation is then used to construct a SQL query to submit to BigQuery.
   */
  object EntityTable {
    def apply(snapshotModel: SnapshotModel, name: String, alias: String): EntityTable =
      EntityTable(snapshotModel.getDataProject, snapshotModel.getName, name, alias)
  }
  case class EntityTable(dataProject: String, viewName: String, name: String, alias: String) {
    lazy val nameInQuery: String = tableNameInQuery(dataProject, viewName, name, Option(alias))
  }

  object EntityColumn {
    def apply(snapshotModel: SnapshotModel, table: EntityTable, column: String): EntityColumn = {
      val isArray =
        getTableModel(snapshotModel, table.name).getColumns.asScala.find(_.getName == column).exists(_.isArrayOf)
      EntityColumn(table, column, isArray)
    }
  }
  case class EntityColumn(table: EntityTable, column: String, isArray: Boolean) {
    lazy val qualifiedName = s"`${validateSql(table.alias)}`.`${validateSql(column)}`"
  }

  /**
    * Contains the data to construct a JOIN in SQL and identify the JOIN in the results
    * @param from left side of join
    * @param to right side of join
    * @param relationshipPath list of relationship names that resulted in this join, used to figure out how to map the
    *                         results back to the input expressions, see ParsedEntityLookupExpression.relationshipPath
    * @param alias alias used in the SQL and results
    * @param isArray joins on array columns require an extra join on the unnested array
    */
  case class EntityJoin(from: EntityColumn,
                        to: EntityColumn,
                        relationshipPath: Seq[String],
                        alias: String,
                        isArray: Boolean
  )

  /**
    * This class represents the parts of the query an expression may add to the BigQuery query
    * @param fromTable the table that the data comes from. If there is no relationship then this table would come after
    *                  the FROM statement. If there is a relationship this comes after a JOIN statement
    * @param join if this is a join, this specifies both sides of the ON statement
    * @param selectColumns the columns that are selected from fromTable. Order is important. The order in this list
    *                      is the order that the columns appear in the actual SQL query and the results. Due to the
    *                      use of STRUCTs we can't rely on the names of fields to extract values from the result and
    *                      must use indexes.
    */
  case class SelectAndFrom(fromTable: EntityTable, join: Option[EntityJoin], selectColumns: Seq[EntityColumn])
}

/**
 * contains helper methods for working with BigQuery from an EntityProvider.
 */
trait DataRepoBigQuerySupport extends LazyLogging {

  /**
   * translate a single BigQuery FieldValue (i.e. not a repeated/array type) to a single Rawls AttributeValue
   *
   * @param field the BQ schema's field definition
   * @param fv the BQ field value
   * @return the Rawls attribute value, post-translation
   */
  def singleFieldValueToAttributeValue(field: Field, fv: FieldValue): AttributeValue =
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
        // Data Repo does not support RECORD types, but we'll include a primitive case for them here
        // by simply toString-ing them
        AttributeString(fv.getValue.toString)
      case _ =>
        // "else" case that covers LegacySQLTypeNames of:
        // DATE, DATETIME, TIME, TIMESTAMP, BYTES, GEOGRAPHY
        // these types don't have strongly-typed equivalents in the entity model, so we treat them
        // as string values, and rely on the caller to parse the string
        AttributeString(fv.getValue.toString)
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

    fieldValueToAttribute(field, fv)
  }

  /**
    * translate a BigQuery FieldValue, possibly repeated/array, to a Rawls Attribute.
    *
    * @param field the BQ schema's field definition
    * @param fv the BQ field value
    * @return the Rawls attribute value, post-translation
    */
  def fieldValueToAttribute(field: Field, fv: FieldValue): Attribute = {
    val attribute = field.getMode match {
      case Mode.REPEATED =>
        fv.getRepeatedValue.asScala.toList match {
          case x if x.isEmpty => AttributeValueEmptyList
          case members        =>
            // can a list itself contain a list? Recursion here is difficult because of the Attribute/AttributeValue hierarchy
            val attrValues = members.map(member => singleFieldValueToAttributeValue(field, member))
            AttributeValueList(attrValues)
        }
      case _ => singleFieldValueToAttributeValue(field, fv)
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
  def queryResultsToEntities(queryResults: TableResult, entityType: String, primaryKey: String): List[Entity] =
    // short-circuit if query results is empty
    if (queryResults.getTotalRows == 0) {
      List.empty[Entity]
    } else {

      val schemaFields = queryResults.getSchema.getFields

      // check results for data we don't handle well. For now this is just RECORD types.
      if (schemaFields.asScala.exists(_.getType == LegacySQLTypeName.RECORD)) {
        logger.warn(
          s"query results for entity type $entityType contains one or more fields " +
            s"with ${LegacySQLTypeName.RECORD.toString} datatypes; these " +
            s"are unsupported and will be serialized to a string value."
        )
      }

      // does primary key exist in the results?
      if (Try(schemaFields.getIndex(primaryKey)).isFailure) {
        throw new DataEntityException(
          s"could not find primary key column '$primaryKey' in query results: ${asDelimitedString(schemaFields)}"
        )
      }

      // short-circuit if query results is empty
      if (queryResults.getTotalRows == 0) {
        List.empty[Entity]
      } else {
        val fieldDefs: List[Field] = schemaFields.iterator().asScala.toList
        queryResults
          .iterateAll()
          .asScala
          .map { row =>
            val attrs = fieldDefs.map(field => fieldToAttributeName(field) -> fieldToAttribute(field, row)).toMap
            val entityName = attrs.find(_._1.name == primaryKey) match {
              case Some((_, a: Attribute)) => AttributeStringifier.apply(a)
              case None                    =>
                // this shouldn't happen, since we validated the pk against the schema
                throw new DataEntityException(
                  s"could not find primary key column '$primaryKey' in row: ${asDelimitedString(row)}"
                )
            }
            Entity(entityName, entityType, attrs)
          }
          .toList
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
  def queryResultsToEntity(queryResults: TableResult, entityType: String, primaryKey: String): Entity =
    queryResults.getTotalRows match {
      case 1 =>
        queryResultsToEntities(queryResults, entityType, primaryKey).head
      case 0 =>
        throw new EntityNotFoundException("Entity not found.")
      case _ =>
        throw new DataEntityException(
          s"Query succeeded, but returned ${queryResults.getTotalRows} rows; expected one row."
        )
    }

  /**
   * Translates a BigQuery result set into the pagination metadata that Rawls expects.
   * @param entityQuery the query criteria supplied by the user, which includes page size
   * @return the Rawls-flavor pagination metadata
   */
  def queryResultsMetadata(totalRowCount: Int, entityQuery: EntityQuery): EntityQueryResultMetadata = {
    // calculate page count, ensuring a min of 1. We always return at least one page,
    // even if that single page contains zero entities.
    val pageCount = Math.max(Math.ceil(totalRowCount.toFloat / entityQuery.pageSize).toInt, 1)
    // we don't support filtering in BQ, so unfilteredCount and filteredCount are the same
    EntityQueryResultMetadata(totalRowCount, totalRowCount, pageCount)
  }

  /**
   * Given an EntityQuery, which contains page number and page size, generate the correct absolute pagination offset for use in BQ
   * @param entityQuery the query criteria supplied by the user
   * @return the offset value to use in an OFFSET sql clause
   */
  def translatePaginationOffset(entityQuery: EntityQuery): Int = {
    if (entityQuery.page < 1)
      throw new DataEntityException("page value must be at least 1.", code = StatusCodes.BadRequest)
    (entityQuery.page - 1) * entityQuery.pageSize
  }

  /**
   * generates the SQL and config to send to BigQuery for use in the queryEntities() method
   * @param dataProject project containing the BQ data
   * @param viewName dataset/snapshot to query in BQ
   * @param entityType table to query in BQ
   * @param entityQuery user-supplied query criteria
   * @return the object to pass to BQ to execute a query
   */
  def queryConfigForQueryEntities(dataProject: String,
                                  viewName: String,
                                  entityType: String,
                                  entityQuery: EntityQuery
  ): QueryJobConfiguration.Builder = {
    // generate BQ SQL for this entity
    val query = s"SELECT * FROM ${tableNameInQuery(dataProject, viewName, entityType)} " +
      s"ORDER BY `${validateSql(entityQuery.sortField)}` ${SortDirections.toSql(entityQuery.sortDirection)} " +
      s"LIMIT ${entityQuery.pageSize} " +
      s"OFFSET ${translatePaginationOffset(entityQuery).toLong};"

    // generate query config
    QueryJobConfiguration.newBuilder(query)
  }

  // create comma-delimited string of field names for use in error messages.
  // list is sorted alphabetically for determinism in unit tests.
  def asDelimitedString(fieldList: FieldList): String =
    fieldList.asScala.toList.map(_.getName).sorted.mkString(",")
  def asDelimitedString(fieldValueList: FieldValueList): String =
    fieldValueList.asScala.toList.map(_.toString).sorted.mkString(",")

  /**
    * Generate the BQ job for all of the entity lookup expression.
    * @param parsedExpressions incoming expressions
    * @param tableModel model object for base table
    * @param entityNameColumn name of column used for the entity name
    * @return structural information about the query (aliases and column ordering are important)
    *         and the BQ job config containing the sql
    */
  protected[datarepo] def queryConfigForExpressions(snapshotModel: SnapshotModel,
                                                    parsedExpressions: Set[ParsedEntityLookupExpression],
                                                    tableModel: TableModel,
                                                    entityNameColumn: String
  ): (Seq[SelectAndFrom], QueryJobConfiguration.Builder) = {
    val rootEntityTable = EntityTable(snapshotModel, tableModel.getName, nextAlias("root"))

    val selectAndFroms =
      figureOutQueryStructureForExpressions(snapshotModel, rootEntityTable, parsedExpressions, entityNameColumn)

    val query: String = generateExpressionSQL(selectAndFroms)

    (selectAndFroms, QueryJobConfiguration.newBuilder(query))
  }

  /**
    * Makes the sql. The first element in selectAndFroms determines the start of the query. There should be
    * no relationship defined in the first selectAndFrom. Its columns are added directly to the SELECT and
    * its table added directly to the FROM. So, if there is only selectAndFrom the query is trivial:
    *   SELECT {selectAndFroms[0].selectColumns}
    *   FROM {selectAndFroms[0].fromTable}
    *
    * Each additional selectAndFrom may add an ARRAY of STRUCT to the SELECT clause
    * (if there are no selectColumns nothing is added) and does add a JOIN to the FROM clause.
    *
    * The STRUCT looks like
    *   dedup(ARRAY_AGG(STRUCT(selectColumns)))
    * See dedupFunction below for what dedup is. This means that each related entity is represented by an array of
    * structures. If it is a many to one relationship the array may have 0 or 1 elements. If it is a one to many
    * relationship the array may have 0 or more elements.
    *
    * The JOIN looks like a typical left join. Unless the from column is an array in which case there is first
    * a LEFT JOIN UNNEST on the array column.
    *
    * ARRAY_AGG is an aggregate function so we also need a GROUP BY clause which is the column list defined by the
    * first selectAndFrom.
    *
    * A full example looks like (with no selected array columns)
    * CREATE TEMP FUNCTION dedup_5(val ANY TYPE) AS ((
    *   SELECT ARRAY_AGG(t)
    *   FROM (SELECT DISTINCT * FROM UNNEST(val) v) t
    * ));
    * SELECT root.donor_id, root.string-field, root.datarepo_row_id,
    *   dedup_5(ARRAY_AGG(STRUCT(entity_3.path, entity_3.datarepo_row_id, entity_3.sample_id))) rel_4
    * FROM `unittest-dataproject.unittest-name.donor` root
    *   LEFT JOIN `unittest-dataproject.unittest-name.sample` entity_1 ON root.donor_id = entity_1.donor_id
    *   LEFT JOIN `unittest-dataproject.unittest-name.file` entity_3 ON entity_1.sample_id = entity_3.sample_id
    * GROUP BY root.donor_id, root.string-field, root.datarepo_row_id;
    *
    * The collection of expressions that would make the above are
    * this.donor_id
    * this.my_donor.sample.path
    * this.my_donor.sample.sample_id
    * this.string-field
    * (also notice that datarepo_row_id columns are automatically added if not otherwise requested, necessary for proper grouping)
    *
    * see behavior of "DataEntityProvider.generateExpressionSQL()" tests in DataRepoEntityProviderSpec
    *
    * @param selectAndFroms
    * @return
    */
  private[datarepo] def generateExpressionSQL(selectAndFroms: Seq[SelectAndFrom]) = {
    // the head of selectAndFroms is special, it is the starting point of the query
    // all elements of the tail eventually join back to the head (like that snake thing)
    val rootTableJoin = selectAndFroms.head

    if (rootTableJoin.join.isDefined) {
      throw new DataEntityException(s"expected rootTableJoin.join to be None")
    }

    val rootSelectFragment = rootTableJoin.selectColumns.map(_.qualifiedName).mkString(", ")
    val rootFromFragment = rootTableJoin.fromTable.nameInQuery

    val (selectFragments, fromFragments, dedupFunctionDefs) = selectAndFroms.tail.map { selectAndFrom =>
      val relationship = selectAndFrom.join.getOrElse(
        throw new DataEntityException("there should only be 1 join without a relationship")
      )
      val (selectFragment, dedupFunctionDef) = if (selectAndFrom.selectColumns.isEmpty) {
        // there are no selected columns related to this join so we don't include anything in the select clause
        // but the join will still exist in the from clause as it is probably a part of a relationship path
        (None, None)
      } else {
        val dedupFunctionName = nextAlias("dedup")
        val dedupFunctionDef = dedupFunction(dedupFunctionName, selectAndFrom)
        (
          // important that selectAndFrom.selectColumns added in order so that the indexes in the result set are as expected
          Option(
            s"$dedupFunctionName(ARRAY_AGG(STRUCT(${selectAndFrom.selectColumns.map(_.qualifiedName).mkString(", ")}))) `${relationship.alias}`"
          ),
          Option(dedupFunctionDef)
        )
      }
      val fromFragment = joinClause(relationship)
      (selectFragment, fromFragment, dedupFunctionDef)
    }.unzip3

    val query = if (selectFragments.isEmpty) {
      // there are no joins
      s"""SELECT $rootSelectFragment FROM $rootFromFragment;"""
    } else {
      s"""${dedupFunctionDefs.flatten.mkString("\n")}
         |SELECT $rootSelectFragment,
         |${selectFragments.flatten.mkString(",\n")}
         |FROM $rootFromFragment
         |${fromFragments.mkString("\n")}
         |GROUP BY $rootSelectFragment;""".stripMargin
    }
    query
  }

  /**
    * A dedup function is used in the BQ query to remove duplicate structs when there is a cartesian product caused
    * by diverging joins. For example expressions this.foo.bar and this.baz.splat would lead to a join to the foo
    * and baz tables and a cartesian product.
    *
    * When arrays are not involved a generic function can be used. However when arrays are in the structs we need a
    * more specialized function that unnests the arrays and reaggregates them.
    * @param selectAndFrom
    * @return
    */
  private[datarepo] def dedupFunction(dedupFunctionName: String, selectAndFrom: SelectAndFrom): String = {
    val (arrayColumns, scalarColumns) = selectAndFrom.selectColumns.partition(_.isArray)
    val dedupFunctionDef = if (arrayColumns.nonEmpty) {
      val arrayColumnNames = arrayColumns.map(c => s"`${validateSql(c.column)}`")
      val arrayColumnNameWithAliases = arrayColumnNames.zip(Seq.fill(arrayColumnNames.size)(s"`${nextAlias("ac")}`"))
      val scalarColumnNames = scalarColumns.map(c => s"`${validateSql(c.column)}`")

      val scalarColumnList = scalarColumnNames.mkString(", ")
      val arrayAliasList = arrayColumnNameWithAliases.map(_._2).mkString(", ")
      val reAggregateList = arrayColumnNameWithAliases
        .map { case (arrayName, arrayAlias) =>
          s"ARRAY_AGG(DISTINCT $arrayAlias) $arrayName"
        }
        .mkString(", ")
      val unnestList = arrayColumnNameWithAliases
        .map { case (arrayName, arrayAlias) =>
          s"UNNEST(v.$arrayName) $arrayAlias"
        }
        .mkString(", ")

      s"""CREATE TEMP FUNCTION ${dedupFunctionName}(val ANY TYPE) AS ((
         |  SELECT ARRAY_AGG(STRUCT(${(scalarColumnNames ++ arrayColumnNames).mkString(", ")})) FROM (
         |    SELECT $scalarColumnList, $reAggregateList
         |    FROM (SELECT DISTINCT $scalarColumnList, $arrayAliasList FROM UNNEST(val) v, $unnestList)
         |    GROUP BY $scalarColumnList
         |  )
         |));""".stripMargin
    } else {
      s"""CREATE TEMP FUNCTION ${dedupFunctionName}(val ANY TYPE) AS ((
         |  SELECT ARRAY_AGG(t)
         |  FROM (SELECT DISTINCT * FROM UNNEST(val) v) t
         |));""".stripMargin
    }

    dedupFunctionDef
  }

  /**
    * This is the main work of converting parsedExpressions to a sql query. At this time there is no WHERE clause
    * in our sql queries, we are getting all the data. This function figures out what columns go in the SELECT clause
    * and what tables and joins go in the FROM clause. The result is a Seq[SelectAndFrom]. This will never be emtpy.
    * The first will never have a relationship (i.e. it is not a join) and always have fromTable as the table. Each
    * subsequent SelectAndFrom will have a relationship to one of the tables referenced by a previous SelectAndFrom
    * in the Seq. Order matters so the sql can be constructed correctly.
    *
    * This function is recursive.
    *
    * @param snapshotModel
    * @param fromTable
    * @param parsedExpressions
    * @param traversedRelationships used to capture the relationship path as this function recurses
    * @return
    */
  private[datarepo] def figureOutQueryStructureForExpressions(snapshotModel: SnapshotModel,
                                                              fromTable: EntityTable,
                                                              parsedExpressions: Set[ParsedEntityLookupExpression],
                                                              entityNameColumn: String,
                                                              traversedRelationships: Seq[String] = Seq.empty
  ): Seq[SelectAndFrom] = {
    def groupAndOrderByRelationshipHead(
      parsedExpressions: Set[ParsedEntityLookupExpression]
    ): Seq[(Option[String], Set[ParsedEntityLookupExpression])] =
      parsedExpressions.groupBy(_.relationshipPath.headOption).toSeq.sortBy { case (name, _) => name.getOrElse("") }

    // ordering is important here because it determines the order in which the joins are added in the final SQL
    val selectAndFroms = groupAndOrderByRelationshipHead(parsedExpressions).flatMap {
      case (None, baseTableExpressions) =>
        val entityColumns = baseTableExpressions.map(expr => EntityColumn(snapshotModel, fromTable, expr.columnName)) +
          EntityColumn(fromTable, entityNameColumn, false)
        // sort columns by name for consistency in tests
        Seq(SelectAndFrom(fromTable, None, entityColumns.toSeq.sortBy(_.column)))

      case (Some(relationshipName), baseTableExpressions) =>
        val currentRelationshipPath = traversedRelationships :+ relationshipName
        val relationshipModel = getRelationshipModel(snapshotModel, relationshipName)
        // the given fromTable may be either the from or the to in the relationship model
        // depending on the direction the relationship is being traversed
        val entityJoin = fromTable.name match {
          case forward if forward.equalsIgnoreCase(relationshipModel.getFrom.getTable) =>
            val fromColumn = EntityColumn(snapshotModel, fromTable, relationshipModel.getFrom.getColumn)
            val toColumn =
              EntityColumn(snapshotModel,
                           EntityTable(snapshotModel, relationshipModel.getTo.getTable, nextAlias("entity")),
                           relationshipModel.getTo.getColumn
              )
            createEntityJoin(snapshotModel, fromTable, relationshipModel, fromColumn, toColumn, currentRelationshipPath)

          case backward if backward.equalsIgnoreCase(relationshipModel.getTo.getTable) =>
            val fromColumn = EntityColumn(snapshotModel, fromTable, relationshipModel.getTo.getColumn)
            val toColumn =
              EntityColumn(snapshotModel,
                           EntityTable(snapshotModel, relationshipModel.getFrom.getTable, nextAlias("entity")),
                           relationshipModel.getFrom.getColumn
              )
            createEntityJoin(snapshotModel, fromTable, relationshipModel, fromColumn, toColumn, currentRelationshipPath)

          case _ =>
            throw new DataEntityException(s"$fromTable does not exist in relationship ${relationshipModel.getName}")
        }

        val popOffTheRelationshipHead =
          baseTableExpressions.map(ex => ex.copy(relationshipPath = ex.relationshipPath.tail))
        val (noMoreRelationships, continueRecursing) = popOffTheRelationshipHead.partition(_.relationshipPath.isEmpty)

        // Construct a SelectAndFrom with all columns in noMoreRelationships.
        // Note that noMoreRelationships may actually be empty, this is the case where no columns are selected from
        // the relation but it is still part of the path to get to a further related entity so still needs a join.
        // Append results of figureOutQueryStructureForExpressions called with continueRecursing
        // to continue walking the relationship path.
        val columns =
          noMoreRelationships.map(expr => EntityColumn(snapshotModel, entityJoin.to.table, expr.columnName)) match {
            case empty if empty.isEmpty => Set.empty
            case someColumns            =>
              // only add the datarepo id column if other columns exist
              someColumns + EntityColumn(entityJoin.to.table, datarepoRowIdColumn, false)
          }

        // sort columns by name for consistency in tests
        Seq(SelectAndFrom(fromTable, Option(entityJoin), columns.toSeq.sortBy(_.column))) ++
          figureOutQueryStructureForExpressions(snapshotModel,
                                                entityJoin.to.table,
                                                continueRecursing,
                                                entityNameColumn,
                                                currentRelationshipPath
          )
    }

    if (
      traversedRelationships.isEmpty && (selectAndFroms.isEmpty || selectAndFroms.headOption.exists(_.join.isDefined))
    ) {
      // traversedRelationships is empty so this is the end of the top level recursive call.
      // selectAndFroms is empty OR
      // the head of selectAndFroms has a join which means there is no expression that selects from the root table.
      // However the query structure requires that there is at least 1 SelectAndFrom and the first be from the root table
      // so prepend one selecting just the entityNameColumn.
      SelectAndFrom(fromTable, None, Seq(EntityColumn(fromTable, entityNameColumn, false))) +: selectAndFroms
    } else {
      selectAndFroms
    }
  }

  private def createEntityJoin(snapshotModel: SnapshotModel,
                               fromTable: EntityTable,
                               relationshipModel: RelationshipModel,
                               fromColumn: EntityColumn,
                               toColumn: EntityColumn,
                               relationshipPath: Seq[String]
  ) = {
    val isArray = isColumnArray(snapshotModel, fromTable, fromColumn)
    EntityJoin(fromColumn, toColumn, relationshipPath, nextAlias("rel"), isArray)
  }

  private def isColumnArray(snapshotModel: SnapshotModel, fromTable: EntityTable, fromColumn: EntityColumn) =
    getTableModel(snapshotModel, fromTable.name).getColumns.asScala
      .find(_.getName == fromColumn.column)
      .exists(_.isArrayOf)

  private def joinClause(relationship: EntityJoin): String =
    if (relationship.isArray) {
      // in this case the "from" column is an array so the join needs more fancy
      // note alias must not start with a number
      val unnestJoinAlias = nextAlias("unnest")
      s"""LEFT JOIN UNNEST(${relationship.from.qualifiedName}) `$unnestJoinAlias`
         |LEFT JOIN ${relationship.to.table.nameInQuery} ON `$unnestJoinAlias` = ${relationship.to.qualifiedName}""".stripMargin
    } else {
      s"LEFT JOIN ${relationship.to.table.nameInQuery} ON ${relationship.from.qualifiedName} = ${relationship.to.qualifiedName}"
    }

  /**
    * nextAliasIndex is used as a counter within nextAlias. It is an AtomicInteger just in case there is
    * multi-threaded access. This is mutable state but no code should expect particular values.
    */
  private val nextAliasIndex = new AtomicInteger(1)

  /**
    * Returns a unique (within this instance) string for use as alias within SQL statements. Use this function
    * instead of aliases derived from external input to ensure SQL safety.
    * @param prefix
    * @return
    */
  private def nextAlias(prefix: String): String = s"${prefix}_${nextAliasIndex.getAndIncrement()}"
}
