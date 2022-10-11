package org.broadinstitute.dsde.rawls.entities.datarepo

import com.google.cloud.PageImpl
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory.createKeyList
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.model.{
  AttributeBoolean,
  AttributeName,
  AttributeNull,
  AttributeNumber,
  AttributeString,
  Entity,
  EntityQuery,
  EntityQueryResultMetadata,
  SortDirections
}
import org.scalatest.freespec.AnyFreeSpec

import scala.jdk.CollectionConverters._

/* see also the unit tests in DataRepoEntityProviderSpec
 */
class DataRepoBigQuerySupportSpec extends AnyFreeSpec with DataRepoBigQuerySupport {

  "DataRepoBigQuerySupport, when translating BQ results to Terra entities, should" - {

    //
    // create fixture data using BigQuery java client classes
    //
    val dateNow = new java.util.Date()
    val timeNow = java.util.Calendar.getInstance().getTime
    val byteArray = "text for bytes".getBytes

    val F_FLOAT = Field.of("float-field", LegacySQLTypeName.FLOAT)
    val F_INTEGER = Field.of("integer-field", LegacySQLTypeName.INTEGER)
    val F_NUMERIC = Field.of("numeric-field", LegacySQLTypeName.NUMERIC)
    val F_BOOLEAN = Field.of("boolean-field", LegacySQLTypeName.BOOLEAN)
    val F_STRING = Field.of("string-field", LegacySQLTypeName.STRING)
    val F_DATE = Field.of("date-field", LegacySQLTypeName.DATE)
    val F_DATETIME = Field.of("datetime-field", LegacySQLTypeName.DATETIME)
    val F_TIME = Field.of("time-field", LegacySQLTypeName.TIME)
    val F_TIMESTAMP = Field.of("timestamp-field", LegacySQLTypeName.TIMESTAMP)
    val F_BYTES = Field.of("bytes-field", LegacySQLTypeName.BYTES)
    val F_GEOGRAPHY = Field.of("geography-field", LegacySQLTypeName.GEOGRAPHY)
    // TODO: tests for RECORD types and repeated-value types

    // Google's FieldValue class documentation says:
    //  https://github.com/googleapis/java-bigquery/blob/master/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/FieldValue.java#L257
    /*
     * Creates an instance of {@code FieldValue}, useful for testing.
     *
     * <p>If the {@code attribute} is {@link Attribute#PRIMITIVE}, the {@code value} should be the
     * string representation of the underlying value, eg {@code "123"} for number {@code 123}.
     *
     * <p>If the {@code attribute} is {@link Attribute#REPEATED} or {@link Attribute#RECORD}, the
     * {@code value} should be {@code List} of {@link FieldValue}s or {@link FieldValueList},
     * respectively.
     * */
    val FV_FLOAT = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "123.456")
    val FV_INTEGER = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "42")
    val FV_NUMERIC = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "3.14")
    val FV_BOOLEAN = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "true")
    val FV_STRING = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "hello world")
    val FV_DATE = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, dateNow.toString)
    val FV_DATETIME = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, dateNow.toString)
    val FV_TIME = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, timeNow.toString)
    // // timestamps are encoded in the format 1408452095.22 where the integer part is seconds since
    // epoch (e.g. 1408452095.22 == 2014-08-19 07:41:35.220 -05:00)
    val FV_TIMESTAMP = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "1408452095.22")
    val FV_BYTES = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, byteArray.toString)
    val FV_GEOGRAPHY = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "[-54, 32]")

    "throw an error if requested field is not present in field values" in {
      val row: FieldValueList =
        FieldValueList.of(List(FV_FLOAT, FV_BOOLEAN, FV_STRING).asJava, F_FLOAT, F_BOOLEAN, F_STRING)
      val ex = intercept[DataEntityException] {
        fieldToAttribute(F_INTEGER, row)
      }
      assertResult(s"field ${F_INTEGER.getName} not found in row ${asDelimitedString(row)}") {
        ex.getMessage
      }
    }

    "translate BQ Float to AttributeNumber" in {
      val row: FieldValueList = FieldValueList.of(List(FV_FLOAT).asJava, F_FLOAT)
      val expected = (AttributeName.withDefaultNS("float-field"), AttributeNumber(123.456))
      assertResult(expected) {
        fieldToAttributeName(F_FLOAT) -> fieldToAttribute(F_FLOAT, row)
      }
    }

    "translate BQ Integer to AttributeNumber" in {
      val row: FieldValueList = FieldValueList.of(List(FV_INTEGER).asJava, F_INTEGER)
      val expected = (AttributeName.withDefaultNS("integer-field"), AttributeNumber(42))
      assertResult(expected) {
        fieldToAttributeName(F_INTEGER) -> fieldToAttribute(F_INTEGER, row)
      }
    }
    "translate BQ Numeric to AttributeNumber" in {
      val row: FieldValueList = FieldValueList.of(List(FV_NUMERIC).asJava, F_NUMERIC)
      val expected = (AttributeName.withDefaultNS("numeric-field"), AttributeNumber(3.14))
      assertResult(expected) {
        fieldToAttributeName(F_NUMERIC) -> fieldToAttribute(F_NUMERIC, row)
      }
    }
    "translate BQ Boolean to AttributeBoolean" in {
      val row: FieldValueList = FieldValueList.of(List(FV_BOOLEAN).asJava, F_BOOLEAN)
      val expected = (AttributeName.withDefaultNS("boolean-field"), AttributeBoolean(true))
      assertResult(expected) {
        fieldToAttributeName(F_BOOLEAN) -> fieldToAttribute(F_BOOLEAN, row)
      }
    }

    "translate BQ String to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_STRING).asJava, F_STRING)
      val expected = (AttributeName.withDefaultNS("string-field"), AttributeString("hello world"))
      assertResult(expected) {
        fieldToAttributeName(F_STRING) -> fieldToAttribute(F_STRING, row)
      }
    }

    "translate BQ Date to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_DATE).asJava, F_DATE)
      val expected = (AttributeName.withDefaultNS("date-field"), AttributeString(dateNow.toString))
      assertResult(expected) {
        fieldToAttributeName(F_DATE) -> fieldToAttribute(F_DATE, row)
      }
    }

    "translate BQ Datetime to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_DATETIME).asJava, F_DATETIME)
      val expected = (AttributeName.withDefaultNS("datetime-field"), AttributeString(dateNow.toString))
      assertResult(expected) {
        fieldToAttributeName(F_DATETIME) -> fieldToAttribute(F_DATETIME, row)
      }
    }

    "translate BQ Time to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_TIME).asJava, F_TIME)
      val expected = (AttributeName.withDefaultNS("time-field"), AttributeString(timeNow.toString))
      assertResult(expected) {
        fieldToAttributeName(F_TIME) -> fieldToAttribute(F_TIME, row)
      }
    }

    "translate BQ Timestamp to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_TIMESTAMP).asJava, F_TIMESTAMP)
      val expected = (AttributeName.withDefaultNS("timestamp-field"), AttributeString("1408452095.22"))
      assertResult(expected) {
        fieldToAttributeName(F_TIMESTAMP) -> fieldToAttribute(F_TIMESTAMP, row)
      }
    }

    "translate BQ Bytes to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_BYTES).asJava, F_BYTES)
      val expected = (AttributeName.withDefaultNS("bytes-field"), AttributeString(byteArray.toString))
      assertResult(expected) {
        fieldToAttributeName(F_BYTES) -> fieldToAttribute(F_BYTES, row)
      }
    }

    "translate BQ Geography to AttributeString" in {
      val row: FieldValueList = FieldValueList.of(List(FV_GEOGRAPHY).asJava, F_GEOGRAPHY)
      val expected = (AttributeName.withDefaultNS("geography-field"), AttributeString("[-54, 32]"))
      assertResult(expected) {
        fieldToAttributeName(F_GEOGRAPHY) -> fieldToAttribute(F_GEOGRAPHY, row)
      }
    }

    // LegacySQLTypeName.values() does not work here, unfortunately
    // skip RECORD types, which require definition of sub-fields
    List(
      LegacySQLTypeName.BOOLEAN,
      LegacySQLTypeName.BYTES,
      LegacySQLTypeName.DATE,
      LegacySQLTypeName.DATETIME,
      LegacySQLTypeName.FLOAT,
      LegacySQLTypeName.GEOGRAPHY,
      LegacySQLTypeName.INTEGER,
      LegacySQLTypeName.NUMERIC,
      LegacySQLTypeName.STRING,
      LegacySQLTypeName.TIME,
      LegacySQLTypeName.TIMESTAMP
    ).foreach { typename =>
      s"translate nulls of BQ ${typename.toString} to AttributeNull" in {
        val fld = Field.of(s"${typename.toString}-field", typename)

        val row: FieldValueList =
          FieldValueList.of(List(
                              FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, null)
                            ).asJava,
                            fld
          )

        assertResult((AttributeName.withDefaultNS(s"${typename.toString}-field"), AttributeNull)) {
          fieldToAttributeName(fld) -> fieldToAttribute(fld, row)
        }
      }
    }

    "throw error if queryResultsToEntity is given zero rows" in {
      val schema: Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
      val row: FieldValueList = FieldValueList.of(List().asJava)
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
      val tableResult: TableResult = new TableResult(schema, 0, page)

      val ex = intercept[EntityNotFoundException] {
        queryResultsToEntity(tableResult, "entityType", "entityName")
      }

      assertResult("Entity not found.") {
        ex.getMessage
      }
    }

    "throw error if queryResultsToEntity is given more than one row" in {
      val schema: Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
      val row: FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
                                                  F_INTEGER,
                                                  F_BOOLEAN,
                                                  F_STRING,
                                                  F_TIMESTAMP
      )
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row, row).asJava)
      val tableResult: TableResult = new TableResult(schema, 2, page)

      val ex = intercept[DataEntityException] {
        queryResultsToEntity(tableResult, "entityType", "entityName")
      }

      assertResult("Query succeeded, but returned 2 rows; expected one row.") {
        ex.getMessage
      }
    }

    "return the Entity if queryResultsToEntity is given one and only one row" in {
      val schema: Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
      val row: FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
                                                  F_INTEGER,
                                                  F_BOOLEAN,
                                                  F_STRING,
                                                  F_TIMESTAMP
      )
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
      val tableResult: TableResult = new TableResult(schema, 1, page)

      val expected = Entity(
        "hello world",
        "entityType",
        Map(
          AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
          AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("string-field") -> AttributeString("hello world"),
          AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
        )
      )

      assertResult(expected) {
        queryResultsToEntity(tableResult, "entityType", "string-field")
      }
    }

    "throw error if requested primary key does not exist in query results" in {
      val schema: Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
      val row: FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
                                                  F_INTEGER,
                                                  F_BOOLEAN,
                                                  F_STRING,
                                                  F_TIMESTAMP
      )
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
      val tableResult: TableResult = new TableResult(schema, 1, page)

      val expected = Entity(
        "hello world",
        "entityType",
        Map(
          AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
          AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("string-field") -> AttributeString("hello world"),
          AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
        )
      )

      val ex = intercept[DataEntityException] {
        queryResultsToEntity(tableResult, "entityType", "invalid-pk")
      }

      assertResult(
        "could not find primary key column 'invalid-pk' in query results: boolean-field,integer-field,string-field,timestamp-field"
      ) {
        ex.getMessage
      }
    }

    "return empty array if queryResultsToEntities is given zero rows" in {
      val schema: Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
      val row: FieldValueList = FieldValueList.of(List().asJava)
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
      val tableResult: TableResult = new TableResult(schema, 0, page)

      assertResult(List()) {
        queryResultsToEntities(tableResult, "entityType", "irrelevant")
      }
    }

    "return one-element array if queryResultsToEntities is given one row" in {
      val schema: Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
      val row: FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
                                                  F_INTEGER,
                                                  F_BOOLEAN,
                                                  F_STRING,
                                                  F_TIMESTAMP
      )
      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
      val tableResult: TableResult = new TableResult(schema, 1, page)

      val expected = Entity(
        "hello world",
        "entityType",
        Map(
          AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
          AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("string-field") -> AttributeString("hello world"),
          AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
        )
      )

      assertResult(List(expected)) {
        queryResultsToEntities(tableResult, "entityType", "string-field")
      }
    }

    "return multiple-element array if queryResultsToEntities is given more than one row" in {
      val schema: Schema = Schema.of(F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)

      val stringKeys = createKeyList(3)

      val results = stringKeys map { stringKey =>
        FieldValueList.of(
          List(FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey),
               FV_INTEGER,
               FV_BOOLEAN,
               FV_TIMESTAMP
          ).asJava,
          F_STRING,
          F_INTEGER,
          F_BOOLEAN,
          F_TIMESTAMP
        )
      }

      val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
      val tableResult: TableResult = new TableResult(schema, 3, page)

      val expected = stringKeys.map { stringKey =>
        Entity(
          stringKey,
          "entityType",
          Map(
            AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
            AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
            AttributeName.withDefaultNS("string-field") -> AttributeString(stringKey),
            AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
          )
        )
      }

      assertResult(expected) {
        queryResultsToEntities(tableResult, "entityType", "string-field")
      }
    }

    // (input pageSize, result set size) -> expected number of pages in result set
    val pagination = Map(
      (10, 0) -> 1,
      (10, 1) -> 1,
      (10, 9) -> 1,
      (10, 10) -> 1,
      (10, 11) -> 2,
      (10, 10000) -> 1000,
      (10, 10001) -> 1001,
      (20, 10000) -> 500,
      (20, 10001) -> 501
    )

    pagination foreach { case ((inputPageSize, resultSetSize), expectedPages) =>
      s"compute correct pagination metadata from BQ results for pageSize $inputPageSize and result set size $resultSetSize (expect $expectedPages)" in {
        val entityQuery = EntityQuery(page = 4,
                                      pageSize = inputPageSize,
                                      sortField = "ignored",
                                      sortDirection = SortDirections.Ascending,
                                      filterTerms = None
        )
        val actual = queryResultsMetadata(resultSetSize, entityQuery)

        assertResult(EntityQueryResultMetadata(resultSetSize, resultSetSize, expectedPages))(actual)
      }
    }

  }

  "DataRepoBigQuerySupport, when generating query config for queryEntities, should" - {

    List(SortDirections.Ascending, SortDirections.Descending) foreach { sortDirection =>
      s"include sort column and sort direction ${SortDirections.toSql(sortDirection)} in BigQuery SQL" in {
        val entityQuery = EntityQuery(page = 1,
                                      pageSize = 20,
                                      sortField = "mySortField",
                                      sortDirection = sortDirection,
                                      filterTerms = None
        )
        val actual = queryConfigForQueryEntities("dataProject", "viewName", "entityType", entityQuery)

        assert(
          actual.build.getQuery.contains(s"ORDER BY `mySortField` ${SortDirections.toSql(sortDirection)}"),
          "generated BQ SQL does not contain correct ORDER BY clause"
        )

      }
    }

    // map of input (page, pageSize) and expected (offset, limit)
    val pagination: Map[(Int, Int), (Int, Int)] = Map(
      (1, 10) -> (0, 10),
      (1, 20) -> (0, 20),
      (5, 50) -> (200, 50),
      (100, 5) -> (495, 5)
    )

    pagination.foreach { case ((page, pageSize), (offset, _)) =>
      s"calculate correct pagination offset for input page=$page, pageSize=$pageSize" in {
        val actual = translatePaginationOffset(
          EntityQuery(page = page,
                      pageSize = pageSize,
                      sortField = "ignored",
                      sortDirection = SortDirections.Ascending,
                      filterTerms = None
          )
        )
        assertResult(offset)(actual)
      }
    }

    pagination.foreach { case ((page, pageSize), (offset, limit)) =>
      s"include correct pagination offset and limit in BigQuery SQL for input page=$page, pageSize=$pageSize" in {
        val entityQuery = EntityQuery(page = page,
                                      pageSize = pageSize,
                                      sortField = "ignored",
                                      sortDirection = SortDirections.Ascending,
                                      filterTerms = None
        )
        val actual = queryConfigForQueryEntities("dataProject", "viewName", "entityType", entityQuery)

        assert(actual.build.getQuery.contains(s"OFFSET $offset"),
               "generated BQ SQL does not contain correct OFFSET clause"
        )

        assert(actual.build.getQuery.contains(s"LIMIT $limit"),
               "generated BQ SQL does not contain correct LIMIT clause"
        )
      }
    }

    List(0, -1, Int.MinValue) foreach { page =>
      s"throw error if input page value is less than 1 for input $page" in {
        val entityQuery = EntityQuery(page = page,
                                      pageSize = 10,
                                      sortField = "ignored",
                                      sortDirection = SortDirections.Ascending,
                                      filterTerms = None
        )

        val ex = intercept[DataEntityException] {
          queryConfigForQueryEntities("dataProject", "viewName", "entityType", entityQuery)
        }
        assertResult("page value must be at least 1.")(ex.getMessage)

      }
    }

  }

}
