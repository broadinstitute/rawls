package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.{ColumnModel, TableModel}
import com.google.cloud.PageImpl
import com.google.cloud.bigquery._
import cromwell.client.model.{ToolInputParameter, ValueType}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException, EntityTypeNotFoundException}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeName, AttributeNumber, AttributeString, AttributeValueRawJson, Entity, EntityTypeMetadata, SubmissionValidationEntityInputs, SubmissionValidationValue}
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Success

class DataRepoEntityProviderSpec extends AsyncFlatSpec with DataRepoEntityProviderSpecSupport with TestDriverComponent with Matchers {

  override implicit val executionContext = TestExecutionContext.testExecutionContext

  behavior of "DataEntityProvider.entityTypeMetadata()"

  it should "return entity type metadata in the golden path" in {
    // N.B. due to the DataRepoEntityProviderSpecSupport.defaultTables fixture data, this test also asserts on:
    // - empty list returned for columns on a table
    // - null PK returned for table, defaults to datarepo_row_id
    // - compound PK returned for table, defaults to datarepo_row_id
    // - single PK returned for table is honored
    // - row counts returned for table are honored

    val provider = createTestProvider()

    provider.entityTypeMetadata() map { metadata: Map[String, EntityTypeMetadata] =>
      // this is the default expected value, should it move to the support trait?
      val expected = Map(
        ("table1", EntityTypeMetadata(0, "datarepo_row_id", Seq("datarepo_row_id", "integer-field", "boolean-field", "timestamp-field"))),
        ("table2", EntityTypeMetadata(123, "table2PK", Seq("col2a", "col2b"))),
        ("table3", EntityTypeMetadata(456, "datarepo_row_id", Seq("col3.1", "col3.2"))))
      assertResult(expected) { metadata }
    }
  }

  it should "return an empty Map if data repo snapshot has no tables" in {
    val provider = createTestProvider(
      snapshotModel = createSnapshotModel( List.empty[TableModel] ) )

    provider.entityTypeMetadata() map { metadata: Map[String, EntityTypeMetadata] =>
      assert(metadata.isEmpty, "expected response data to be the empty map")
    }
  }

  behavior of "DataRepoBigQuerySupport, when finding the primary key for a table"

  it should "use primary key of `datarepo_row_id` if snapshot has null primary key" in {
    val input = new TableModel()
    input.setPrimaryKey(null)
    assertResult("datarepo_row_id") { createTestProvider().pkFromSnapshotTable(input) }
  }

  it should "use primary key of `datarepo_row_id` if snapshot has empty-array primary key" in {
    val input = new TableModel()
    input.setPrimaryKey(List.empty[String].asJava)
    assertResult("datarepo_row_id") { createTestProvider().pkFromSnapshotTable(input) }
  }

  it should "use primary key of `datarepo_row_id` if snapshot has multiple primary keys" in {
    val input = new TableModel()
    input.setPrimaryKey(List("one", "two", "three").asJava)
    assertResult("datarepo_row_id") { createTestProvider().pkFromSnapshotTable(input) }
  }

  it should "use primary key from snapshot if one and only one returned" in {
    val input = new TableModel()
    input.setPrimaryKey(List("singlekey").asJava)
    assertResult("singlekey") { createTestProvider().pkFromSnapshotTable(input) }
  }

  // to-do: tests for entity/row counts returned by data repo, once TDR supports this (see DR-1003)

  behavior of "DataEntityProvider.getEntity()"

  it should "return exactly one entity if all OK" in {

    // set up a provider with a mock that returns exactly one BQ row
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.take(1).asJava)
    val tableResult: TableResult = new TableResult(schema, 1, page)
    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    provider.getEntity("table1", "the first row") map { entity: Entity =>
      // this is the default expected value, should it move to the support trait?
      val expected = Entity("the first row", "table1", Map(
        AttributeName.withDefaultNS("datarepo_row_id") -> AttributeString("the first row"),
        AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
        AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
      ))
      assertResult(expected) { entity }
    }
  }

  it should "fail if pet credentials not available from Sam" in {
    val provider = createTestProvider(
      samDAO = new SpecSamDAO(petKeyForUserResponse = Left(new Exception("sam error"))))

    val futureEx = recoverToExceptionIf[Exception] {
      provider.getEntity("table1", "the first row")
    }
    futureEx map { ex =>
      assertResult("sam error") { ex.getMessage }
    }
  }

  ignore should "fail if user is a workspace Reader but did not specify a billing project (canCompute?)" in {
    // we haven't implemented the runtime logic for this because we don't have PO input,
    // so we don't know exactly what to unit test
    fail("not implemented in runtime code yet")
  }

  it should "fail if snapshot table not found in data repo's response" in {
    val provider = createTestProvider() // default behavior returns three rows

    val ex = intercept[EntityTypeNotFoundException] {
      provider.getEntity("this_table_is_unknown", "the first row")
    }
    assertResult("this_table_is_unknown") { ex.requestedType }
  }

  it should "bubble up error if BigQuery errors" in {
    val provider = createTestProvider(
      bqFactory = MockBigQueryServiceFactory.ioFactory(Left(new BigQueryException(555, "unit test exception message"))))

    val futureEx = recoverToExceptionIf[BigQueryException] {
      provider.getEntity("table1", "the first row")
    }
    futureEx map { ex =>
      assertResult("unit test exception message") { ex.getMessage }
    }
  }

  it should "fail if BigQuery returns zero rows" in {
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List.empty[FieldValueList].asJava)
    val tableResult: TableResult = new TableResult(Schema.of(List.empty[Field].asJava), 0, page)

    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    val futureEx = recoverToExceptionIf[EntityNotFoundException] {
      provider.getEntity("table1", "the first row")
    }
    futureEx map { ex =>
      assertResult("Entity not found.") { ex.getMessage }
    }
  }

  it should "fail if BigQuery returns more than one" in {
    val provider = createTestProvider() // default behavior returns three rows

    val futureEx = recoverToExceptionIf[DataEntityException] {
      provider.getEntity("table1", "the first row")
    }
    futureEx map { ex =>
      assertResult("Query succeeded, but returned 3 rows; expected one row.") { ex.getMessage }
    }
  }


  behavior of "DataEntityProvider.evaluateExpressions()"

  it should "do the happy path for basic expressions" in {

    val F_INTEGER = Field.of("integer-field", LegacySQLTypeName.INTEGER)
    val F_BOOLEAN = Field.of("boolean-field", LegacySQLTypeName.BOOLEAN)
    val F_STRING = Field.of("datarepo_row_id", LegacySQLTypeName.STRING)
    val F_TIMESTAMP = Field.of("timestamp-field", LegacySQLTypeName.TIMESTAMP)

    val schema: Schema = Schema.of(F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)

    val results = stringKeys map { stringKey  =>
      FieldValueList.of(List(
        FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey),
        FV_INTEGER, FV_BOOLEAN, FV_TIMESTAMP).asJava,
        F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, 3, page)


    // set up a provider with a mock that returns ..
    val provider = createTestProvider(snapshotModel = createSnapshotModel(List(new TableModel().name("table1").primaryKey(null).rowCount(3)
      .columns(List("integer-field", "boolean-field", "timestamp-field").map(new ColumnModel().name(_)).asJava))), bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))
    val expressionEvaluationContext = ExpressionEvaluationContext(None, None, None, Some("table1"))
    val gatherInputsResult = GatherInputsResult(Set(
      MethodInput(new ToolInputParameter().name("name1").valueType(new ValueType().typeName(ValueType.TypeNameEnum.INT)), "this.integer-field"),
      MethodInput(new ToolInputParameter().name("name2").valueType(new ValueType().typeName(ValueType.TypeNameEnum.BOOLEAN)), "this.boolean-field"),
      MethodInput(new ToolInputParameter().name("workspace1").valueType(new ValueType().typeName(ValueType.TypeNameEnum.STRING)), "workspace.string"),
      MethodInput(new ToolInputParameter().name("name3").valueType(new ValueType().typeName(ValueType.TypeNameEnum.OBJECT)), """{"foo": this.boolean-field, "bar": this.timestamp-field, "workspace": workspace.string}""")
    ), Set.empty, Set.empty, Set.empty)

    provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map("workspace.string" -> Success(List(AttributeString("workspaceValue"))))) map { submissionValidationEntityInputs =>
      val expectedResults = (stringKeys map { stringKey =>
        SubmissionValidationEntityInputs(stringKey, Set(
          SubmissionValidationValue(Some(AttributeNumber(MockBigQueryServiceFactory.FV_INTEGER.getNumericValue)), None, "name1"),
          SubmissionValidationValue(Some(AttributeBoolean(MockBigQueryServiceFactory.FV_BOOLEAN.getBooleanValue)), None, "name2"),
          SubmissionValidationValue(Some(AttributeString("workspaceValue")), None, "workspace1"),
          SubmissionValidationValue(Some(AttributeValueRawJson(s"""{"foo": ${MockBigQueryServiceFactory.FV_BOOLEAN.getBooleanValue}, "bar": "${MockBigQueryServiceFactory.FV_TIMESTAMP.getStringValue}", "workspace": "workspaceValue"}""")), None, "name3")
        ))
      })
      submissionValidationEntityInputs should contain theSameElementsAs expectedResults
    }
  }

  val table2Result = {
    val table2RowCount = 123
    val schema: Schema = Schema.of(F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)

    val stringKeys = List.tabulate(table2RowCount)(i => "Row" + i)

    val results = stringKeys map { stringKey  =>
      FieldValueList.of(List(
        FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey),
        FV_INTEGER, FV_BOOLEAN, FV_TIMESTAMP).asJava,
        F_STRING, F_INTEGER, F_BOOLEAN, F_TIMESTAMP)
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, table2RowCount, page)
    tableResult
  }

  it should "fail if the query results in more rows than are allowed by config" in {
    val smallMaxRowsPerQuery = 100
    val table2RowCount = 123

    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(table2Result)), config = DataRepoEntityProviderConfig(maxInputsPerSubmission, smallMaxRowsPerQuery))
    val expressionEvaluationContext = ExpressionEvaluationContext(None, None, None, Some("table2"))

    val gatherInputsResult = GatherInputsResult(Set(
      MethodInput(new ToolInputParameter().name("col2a").valueType(new ValueType().typeName(ValueType.TypeNameEnum.INT)), "this.col2a"),
      MethodInput(new ToolInputParameter().name("col2b").valueType(new ValueType().typeName(ValueType.TypeNameEnum.BOOLEAN)), "this.col2b"),
    ), Set.empty, Set.empty, Set.empty)

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map("workspace.string" -> Success(List(AttributeString("workspaceValue"))))), Duration.Inf)
    }.errorReport.message should be(s"Too many results. Results size ${table2RowCount} cannot exceed ${smallMaxRowsPerQuery}. Expression(s): [this.col2a, this.col2b].")
  }


  it should "fail if the submission has more inputs than are allowed by config" in {
    val smallMaxInputsPerSubmission = 200

    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(table2Result)), config = DataRepoEntityProviderConfig(smallMaxInputsPerSubmission, maxRowsPerQuery))
    val expressionEvaluationContext = ExpressionEvaluationContext(None, None, None, Some("table2"))

    val gatherInputsResult = GatherInputsResult(Set(
      MethodInput(new ToolInputParameter().name("col2a").valueType(new ValueType().typeName(ValueType.TypeNameEnum.INT)), "this.col2a"),
      MethodInput(new ToolInputParameter().name("col2b").valueType(new ValueType().typeName(ValueType.TypeNameEnum.BOOLEAN)), "this.col2b"),
    ), Set.empty, Set.empty, Set.empty)

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(provider.evaluateExpressions(expressionEvaluationContext, gatherInputsResult, Map("workspace.string" -> Success(List(AttributeString("workspaceValue"))))), Duration.Inf)
    }.errorReport.message should be(s"Too many results. Snapshot row count * number of entity expressions cannot exceed ${smallMaxInputsPerSubmission}.")
  }


}



