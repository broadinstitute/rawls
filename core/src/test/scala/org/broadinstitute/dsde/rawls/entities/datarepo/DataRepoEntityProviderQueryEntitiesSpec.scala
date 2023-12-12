package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.{ColumnModel, TableModel}
import com.google.cloud.PageImpl
import com.google.cloud.bigquery._
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory.{createKeyList, createTestTableResult}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityTypeNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class DataRepoEntityProviderQueryEntitiesSpec
    extends AsyncFlatSpec
    with DataRepoEntityProviderSpecSupport
    with TestDriverComponent
    with Matchers {

  implicit override val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val defaultEntityRequestArguments =
    EntityRequestArguments(workspace, testContext, Some(DataReferenceName("referenceName")))

  behavior of "DataEntityProvider.queryEntities()"

  val defaultEntityQuery: EntityQuery = EntityQuery(page = 1,
                                                    pageSize = 10,
                                                    sortField = "datarepo_row_id",
                                                    sortDirection = SortDirections.Ascending,
                                                    filterTerms = None
  )

  it should "return one entity if all OK and BQ returned one" in {

    // set up a provider with a mock that returns exactly one BQ row
    val tableResult: TableResult = createTestTableResult(1)
    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = Seq(
        Entity(
          "Row0",
          "table1",
          Map(
            AttributeName.withDefaultNS("datarepo_row_id") -> AttributeString("Row0"),
            AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
            AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
            AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
          )
        )
      )
      assertResult(defaultEntityQuery)(entityQueryResponse.parameters)
      assertResult(EntityQueryResultMetadata(unfilteredCount = 10, filteredCount = 10, filteredPageCount = 1)) {
        entityQueryResponse.resultMetadata
      }
      assertResult(expected)(entityQueryResponse.results)
    }
  }

  it should "return three entities if all OK and BQ returned three" in {

    val provider = createTestProvider() // default behavior returns three rows

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = createKeyList(3) map { stringKey =>
        Entity(
          stringKey,
          "table1",
          Map(
            AttributeName.withDefaultNS("datarepo_row_id") -> AttributeString(stringKey),
            AttributeName.withDefaultNS("integer-field") -> AttributeNumber(42),
            AttributeName.withDefaultNS("boolean-field") -> AttributeBoolean(true),
            AttributeName.withDefaultNS("timestamp-field") -> AttributeString("1408452095.22")
          )
        )
      }
      assertResult(defaultEntityQuery)(entityQueryResponse.parameters)
      assertResult(EntityQueryResultMetadata(unfilteredCount = 10, filteredCount = 10, filteredPageCount = 1)) {
        entityQueryResponse.resultMetadata
      }
      assertResult(expected)(entityQueryResponse.results)
    }
  }

  val magicSortFields = List("datarepo_row_id", "name")

  magicSortFields foreach { magic =>
    it should s"allow sorting by '$magic'" in {

      val provider = createTestProvider() // default behavior returns three rows

      val query = defaultEntityQuery.copy(sortField = magic)

      // as long as this doesn't throw an error, we're good.  This test case is covered by other test cases,
      // but we make it explicit here in case those other test cases change.
      provider.queryEntities("table1", query) map { _ =>
        succeed
      }
    }
  }

  it should "return empty Seq and appropriate metadata if BigQuery returns zero rows" in {
    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, List.empty[FieldValueList].asJava)
    val tableResult: TableResult = new TableResult(Schema.of(List.empty[Field].asJava), 0, page)

    val provider = createTestProvider(bqFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)))

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = Seq.empty[Entity]
      assertResult(defaultEntityQuery)(entityQueryResponse.parameters)
      assertResult(EntityQueryResultMetadata(unfilteredCount = 10, filteredCount = 10, filteredPageCount = 1)) {
        entityQueryResponse.resultMetadata
      }
      assertResult(expected)(entityQueryResponse.results)
    }
  }

  it should "return empty Seq and appropriate metadata if Data Repo indicates zero rows" in {
    val emptyTables: List[TableModel] = List(
      new TableModel()
        .name("table1")
        .primaryKey(null)
        .rowCount(0)
        .columns(List("integer-field", "boolean-field", "timestamp-field").map(new ColumnModel().name(_)).asJava),
      new TableModel()
        .name("table2")
        .primaryKey(List("table2PK").asJava)
        .rowCount(123)
        .columns(List("col2a", "col2b").map(new ColumnModel().name(_)).asJava)
    )

    val provider = createTestProvider(snapshotModel = createSnapshotModel(tables = emptyTables))

    provider.queryEntities("table1", defaultEntityQuery) map { entityQueryResponse: EntityQueryResponse =>
      // this is the default expected value, should it move to the support trait?
      val expected = Seq.empty[Entity]
      assertResult(defaultEntityQuery)(entityQueryResponse.parameters)
      assertResult(EntityQueryResultMetadata(unfilteredCount = 0, filteredCount = 0, filteredPageCount = 1)) {
        entityQueryResponse.resultMetadata
      }
      assertResult(expected)(entityQueryResponse.results)
    }
  }

  List(2, 10, 42, Integer.MAX_VALUE) foreach { x =>
    it should s"throw bad request if Data Repo indicates zero rows and user requested page > 1 (requested page: $x)" in {
      val emptyTables: List[TableModel] = List(
        new TableModel()
          .name("table1")
          .primaryKey(null)
          .rowCount(0)
          .columns(List("integer-field", "boolean-field", "timestamp-field").map(new ColumnModel().name(_)).asJava),
        new TableModel()
          .name("table2")
          .primaryKey(List("table2PK").asJava)
          .rowCount(123)
          .columns(List("col2a", "col2b").map(new ColumnModel().name(_)).asJava)
      )

      val provider = createTestProvider(snapshotModel = createSnapshotModel(tables = emptyTables))

      val ex = intercept[DataEntityException] {
        provider.queryEntities("table1", defaultEntityQuery.copy(page = x))
      }
      assertResult(s"requested page $x is greater than the number of pages 1") {
        ex.getMessage
      }
    }
  }

  it should "fail if pet credentials not available from Sam" in {
    val provider = createTestProvider(samDAO = new SpecSamDAO(petKeyForUserResponse = Left(new Exception("sam error"))))

    val futureEx = recoverToExceptionIf[Exception] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    futureEx map { ex =>
      assertResult(
        s"Error attempting to use project ${provider.googleProject}. The project does not exist or you do not have permission to use it: sam error"
      )(ex.getMessage)
    }
  }

  it should "throw bad request if the supplied sort field does not exist in the target table" in {
    val provider = createTestProvider()

    val ex = intercept[DataEntityException] {
      provider.queryEntities("table1", defaultEntityQuery.copy(sortField = "unknownColumn"))
    }
    assertResult("sortField not valid for this entity type")(ex.getMessage)
  }

  List(2, 10, 42, Integer.MAX_VALUE) foreach { x =>
    it should s"throw bad request if the requested page is greater than actual pages (requested page: $x)" in {
      val provider = createTestProvider()

      val ex = intercept[DataEntityException] {
        provider.queryEntities("table1", defaultEntityQuery.copy(page = x))
      }
      assertResult(s"requested page $x is greater than the number of pages 1") {
        ex.getMessage
      }
    }
  }

  List(0, -1, -42, Integer.MIN_VALUE) foreach { x =>
    it should s"throw bad request if the requested page is less than 1 (requested page: $x)" in {
      val provider = createTestProvider()

      val ex = intercept[DataEntityException] {
        provider.queryEntities("table1", defaultEntityQuery.copy(page = x))
      }
      assertResult("page value must be at least 1.")(ex.getMessage)
    }
  }

  it should "throw bad request if a filter is supplied" in {
    val provider = createTestProvider()

    val ex = intercept[UnsupportedEntityOperationException] {
      provider.queryEntities("table1", defaultEntityQuery.copy(filterTerms = Some("my filter terms")))
    }
    assertResult("term filtering not supported by this provider.")(ex.getMessage)
  }

  ignore should "fail if user is a workspace Reader but did not specify a billing project (canCompute?)" in {
    // we haven't implemented the runtime logic for this because we don't have PO input,
    // so we don't know exactly what to unit test
    fail("not implemented in runtime code yet")
  }

  it should "fail if snapshot has no tables in data repo" in {
    val provider = createTestProvider(snapshotModel = createSnapshotModel(List.empty[TableModel]))

    val ex = intercept[EntityTypeNotFoundException] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    assertResult("table1")(ex.requestedType)
  }

  it should "fail if snapshot table not found in data repo's response" in {
    val provider = createTestProvider() // default behavior returns three rows

    val ex = intercept[EntityTypeNotFoundException] {
      provider.queryEntities("this_table_is_unknown", defaultEntityQuery)
    }
    assertResult("this_table_is_unknown")(ex.requestedType)
  }

  it should "bubble up error if BigQuery errors" in {
    val provider = createTestProvider(
      bqFactory = MockBigQueryServiceFactory.ioFactory(Left(new BigQueryException(555, "unit test exception message")))
    )

    val futureEx = recoverToExceptionIf[BigQueryException] {
      provider.queryEntities("table1", defaultEntityQuery)
    }
    futureEx map { ex =>
      assertResult("unit test exception message")(ex.getMessage)
    }
  }

}
