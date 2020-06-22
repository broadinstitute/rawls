package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.TableModel
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Field, FieldList, FieldValue, FieldValueList, LegacySQLTypeName, Schema, TableResult}
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeName, AttributeNumber, AttributeString, Entity}
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class DataRepoBigQuerySupportSpec extends FlatSpec with DataRepoBigQuerySupport {

  behavior of "DataRepoBigQuerySupport, when finding the primary key for a table"

  it should "use primary key of `datarepo_row_id` if snapshot has null primary key" in {
    val input = new TableModel()
    input.setPrimaryKey(null)
    assertResult("datarepo_row_id") { pkFromSnapshotTable(input) }
  }

  it should "use primary key of `datarepo_row_id` if snapshot has empty-array primary key" in {
    val input = new TableModel()
    input.setPrimaryKey(List.empty[String].asJava)
    assertResult("datarepo_row_id") { pkFromSnapshotTable(input) }
  }

  it should "use primary key of `datarepo_row_id` if snapshot has multiple primary keys" in {
    val input = new TableModel()
    input.setPrimaryKey(List("one", "two", "three").asJava)
    assertResult("datarepo_row_id") { pkFromSnapshotTable(input) }
  }

  it should "use primary key from snapshot if one and only one returned" in {
    val input = new TableModel()
    input.setPrimaryKey(List("singlekey").asJava)
    assertResult("singlekey") { pkFromSnapshotTable(input) }
  }

  behavior of "DataRepoBigQuerySupport, when translating BQ results to Terra entities"

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
  // TODO: support RECORD types (repeated values/structs)

  val schema = FieldList.of(
    F_FLOAT, F_INTEGER, F_NUMERIC, F_BOOLEAN, F_STRING,
    F_DATE, F_DATETIME, F_TIME, F_TIMESTAMP,
    F_BYTES, F_GEOGRAPHY
  )

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

  it should "translate BQ Float to AttributeNumber" in {
    val row:FieldValueList = FieldValueList.of(List(FV_FLOAT).asJava, F_FLOAT)
    val expected = (AttributeName.withDefaultNS("float-field"), AttributeNumber(123.456))
    assertResult (expected) { fieldToAttribute(F_FLOAT, row) }
  }

  it should "translate BQ Integer to AttributeNumber" in {
    val row:FieldValueList = FieldValueList.of(List(FV_INTEGER).asJava, F_INTEGER)
    val expected = (AttributeName.withDefaultNS("integer-field"), AttributeNumber(42))
    assertResult (expected) { fieldToAttribute(F_INTEGER, row) }
  }
  it should "translate BQ Numeric to AttributeNumber" in {
    val row:FieldValueList = FieldValueList.of(List(FV_NUMERIC).asJava, F_NUMERIC)
    val expected = (AttributeName.withDefaultNS("numeric-field"), AttributeNumber(3.14))
    assertResult (expected) { fieldToAttribute(F_NUMERIC, row) }
  }
  it should "translate BQ Boolean to AttributeBoolean" in {
    val row:FieldValueList = FieldValueList.of(List(FV_BOOLEAN).asJava, F_BOOLEAN)
    val expected = (AttributeName.withDefaultNS("boolean-field"), AttributeBoolean(true))
    assertResult (expected) { fieldToAttribute(F_BOOLEAN, row) }
  }

  it should "translate BQ String to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_STRING).asJava, F_STRING)
    val expected = (AttributeName.withDefaultNS("string-field"), AttributeString("hello world"))
    assertResult (expected) { fieldToAttribute(F_STRING, row) }
  }

  it should "translate BQ Date to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_DATE).asJava, F_DATE)
    val expected = (AttributeName.withDefaultNS("date-field"), AttributeString(dateNow.toString))
    assertResult (expected) { fieldToAttribute(F_DATE, row) }
  }

  it should "translate BQ Datetime to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_DATETIME).asJava, F_DATETIME)
    val expected = (AttributeName.withDefaultNS("datetime-field"), AttributeString(dateNow.toString))
    assertResult (expected) { fieldToAttribute(F_DATETIME, row) }
  }

  it should "translate BQ Time to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_TIME).asJava, F_TIME)
    val expected = (AttributeName.withDefaultNS("time-field"), AttributeString(timeNow.toString))
    assertResult (expected) { fieldToAttribute(F_TIME, row) }
  }

  it should "translate BQ Timestamp to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_TIMESTAMP).asJava, F_TIMESTAMP)
    val expected = (AttributeName.withDefaultNS("timestamp-field"), AttributeString("1408452095.22"))
    assertResult (expected) { fieldToAttribute(F_TIMESTAMP, row) }
  }

  it should "translate BQ Bytes to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_BYTES).asJava, F_BYTES)
    val expected = (AttributeName.withDefaultNS("bytes-field"), AttributeString(byteArray.toString))
    assertResult (expected) { fieldToAttribute(F_BYTES, row) }
  }

  it should "translate BQ Geography to AttributeString" in {
    val row:FieldValueList = FieldValueList.of(List(FV_GEOGRAPHY).asJava, F_GEOGRAPHY)
    val expected = (AttributeName.withDefaultNS("geography-field"), AttributeString("[-54, 32]"))
    assertResult (expected) { fieldToAttribute(F_GEOGRAPHY, row) }
  }

  it should "throw error if queryResultsToEntity is given zero rows" in {
    val schema:Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val row:FieldValueList = FieldValueList.of(List().asJava)
    val page:PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
    val tableResult:TableResult = new TableResult(schema, 0, page)

    val ex = intercept[DataEntityException] {
      queryResultsToEntity(tableResult, "entityType", "entityName")
    }

    assertResult("BQ succeeded, but returned zero rows") { ex.getMessage }
  }

  it should "throw error if queryResultsToEntity is given more than one row" in {
    val schema:Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val row:FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
      F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val page:PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row, row).asJava)
    val tableResult:TableResult = new TableResult(schema, 2, page)

    val ex = intercept[DataEntityException] {
      queryResultsToEntity(tableResult, "entityType", "entityName")
    }

    assertResult("BQ succeeded, but returned 2 rows") { ex.getMessage }
  }

  it should "return the Entity if queryResultsToEntity is given one and only one row" in {
    val schema:Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val row:FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
      F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val page:PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
    val tableResult:TableResult = new TableResult(schema, 1, page)

    val expected = Entity("entityName", "entityType", Map(
        AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
        AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("string-field") -> AttributeString("hello world"),
        AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
      ))

    assertResult (expected) { queryResultsToEntity(tableResult, "entityType", "entityName") }
  }

  it should "return empty array if queryResultsToEntities is given zero rows" in {
    val schema:Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val row:FieldValueList = FieldValueList.of(List().asJava)
    val page:PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
    val tableResult:TableResult = new TableResult(schema, 0, page)

    assertResult(List()) { queryResultsToEntities(tableResult, "entityType", "entityName") }
  }

  it should "return one-element array if queryResultsToEntities is given one row" in {
    val schema:Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val row:FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
      F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val page:PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row).asJava)
    val tableResult:TableResult = new TableResult(schema, 1, page)

    val expected = Entity("entityName", "entityType", Map(
      AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
      AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
      AttributeName.withDefaultNS("string-field") -> AttributeString("hello world"),
      AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
    ))

    assertResult (List(expected)) { queryResultsToEntities(tableResult, "entityType", "entityName") }
  }

  it should "return multiple-element array if queryResultsToEntities is given more than one row" in {
    val schema:Schema = Schema.of(F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val row:FieldValueList = FieldValueList.of(List(FV_INTEGER, FV_BOOLEAN, FV_STRING, FV_TIMESTAMP).asJava,
      F_INTEGER, F_BOOLEAN, F_STRING, F_TIMESTAMP)
    val page:PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List(row, row, row).asJava)
    val tableResult:TableResult = new TableResult(schema, 3, page)

    val expected = Entity("entityName", "entityType", Map(
      AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
      AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
      AttributeName.withDefaultNS("string-field") -> AttributeString("hello world"),
      AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
    ))

    // TODO: use different values for each record!
    assertResult (List(expected, expected, expected)) { queryResultsToEntities(tableResult, "entityType", "entityName") }
  }

}
