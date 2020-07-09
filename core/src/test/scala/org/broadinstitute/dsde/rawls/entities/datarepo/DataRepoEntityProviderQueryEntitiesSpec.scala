package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.TableModel
import com.google.cloud.PageImpl
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory.{results, schema}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityTypeNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.collection.JavaConverters._

class DataRepoEntityProviderQueryEntitiesSpec extends AsyncFlatSpec with DataRepoEntityProviderSpecSupport with TestDriverComponent with Matchers {

  override implicit val executionContext = TestExecutionContext.testExecutionContext

  behavior of "DataEntityProvider.queryEntities()"

  val defaultEntityQuery: EntityQuery = EntityQuery(page = 1, pageSize = 10, sortField = "datarepo_row_id", sortDirection = SortDirections.Ascending, filterTerms = None)

  it should "return one entity if all OK and BQ returned one" in {

    // set up a provider with a mock that returns exactly one BQ row
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.take(1).asJava)
    val tableResult: TableResult = new TableResult(schema, 1, page)
    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = Seq(Entity("the first row", "table1", Map(
        AttributeName.withDefaultNS("datarepo_row_id") -> AttributeString("the first row"),
        AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
        AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
        AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
      )))
      assertResult(defaultEntityQuery) { entityQueryResponse.parameters }
      assertResult(EntityQueryResultMetadata(unfilteredCount = 1, filteredCount = 1, filteredPageCount = 1)) { entityQueryResponse.resultMetadata }
      assertResult(expected) { entityQueryResponse.results }
    }
  }

  it should "return three entities if all OK and BQ returned three" in {

    val provider = createTestProvider() // default behavior returns three rows

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = Seq("the first row", "the second row", "the third row") map { stringKey  =>
        Entity(stringKey, "table1", Map(
          AttributeName.withDefaultNS("datarepo_row_id") -> AttributeString(stringKey),
          AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
          AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
          AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
        ))
      }
      assertResult(defaultEntityQuery) { entityQueryResponse.parameters }
      assertResult(EntityQueryResultMetadata(unfilteredCount = 3, filteredCount = 3, filteredPageCount = 1)) { entityQueryResponse.resultMetadata }
      assertResult(expected) { entityQueryResponse.results }
    }
  }

  it should "return empty Seq and appropriate metadata if BigQuery returns zero rows" in {
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List.empty[FieldValueList].asJava)
    val tableResult: TableResult = new TableResult(Schema.of(List.empty[Field].asJava), 0, page)

    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = Seq.empty[Entity]
      assertResult(defaultEntityQuery) { entityQueryResponse.parameters }
      assertResult(EntityQueryResultMetadata(unfilteredCount = 0, filteredCount = 0, filteredPageCount = 0)) { entityQueryResponse.resultMetadata }
      assertResult(expected) { entityQueryResponse.results }
    }
  }

  it should "bubble up error if workspace manager errors (includes reference not found)" in {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Left(new bio.terra.workspace.client.ApiException("whoops 1"))))

    val ex = intercept[bio.terra.workspace.client.ApiException] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    assertResult("whoops 1") { ex.getMessage }
  }

  it should "fail if pet credentials not available from Sam" in {
    val provider = createTestProvider(
      samDAO = new SpecSamDAO(petKeyForUserResponse = Left(new Exception("sam error"))))

    val futureEx = recoverToExceptionIf[Exception] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    futureEx map { ex =>
      assertResult("sam error") { ex.getMessage }
    }
  }

  it should "throw bad request if a filter is supplied" in {
    val provider = createTestProvider()

    val ex = intercept[UnsupportedEntityOperationException] {
      provider.queryEntities("table1", defaultEntityQuery.copy(filterTerms = Some("my filter terms")))
    }
    assertResult("term filtering not supported by this provider.") { ex.getMessage }
  }

  ignore should "fail if user is a workspace Reader but did not specify a billing project (canCompute?)" in {
    // we haven't implemented the runtime logic for this because we don't have PO input,
    // so we don't know exactly what to unit test
    fail("not implemented in runtime code yet")
  }

  it should "bubble up error if data repo errors (includes snapshot not found/not allowed)" in {
    val provider = createTestProvider(
      dataRepoDAO = new SpecDataRepoDAO(Left(new bio.terra.datarepo.client.ApiException("whoops 2"))))

    val ex = intercept[bio.terra.datarepo.client.ApiException] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    assertResult("whoops 2") { ex.getMessage }
  }

  it should "fail if snapshot has no tables in data repo" in {
    val provider = createTestProvider(
      dataRepoDAO = new SpecDataRepoDAO(Right( createSnapshotModel( List.empty[TableModel] ) )))

    val ex = intercept[EntityTypeNotFoundException] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    assertResult("table1") { ex.requestedType }
  }

  it should "fail if snapshot table not found in data repo's response" in {
    val provider = createTestProvider() // default behavior returns three rows

    val ex = intercept[EntityTypeNotFoundException] {
      provider.queryEntities("this_table_is_unknown", defaultEntityQuery)
    }
    assertResult("this_table_is_unknown") { ex.requestedType }
  }

  it should "bubble up error if BigQuery errors" in {
    val provider = createTestProvider(
      bqFactory = MockBigQueryServiceFactory.ioFactory(Left(new BigQueryException(555, "unit test exception message"))))

    val futureEx = recoverToExceptionIf[BigQueryException] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    futureEx map { ex =>
      assertResult("unit test exception message") { ex.getMessage }
    }
  }


}



