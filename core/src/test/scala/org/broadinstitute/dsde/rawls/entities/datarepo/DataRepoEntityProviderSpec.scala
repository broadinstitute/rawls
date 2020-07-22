package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.TableModel
import com.google.cloud.PageImpl
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory.{results, schema}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException, EntityTypeNotFoundException}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.JavaConverters._

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
        ("table2", EntityTypeMetadata(123, "table2PK", Seq("col2.1", "col2.2"))),
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

}



