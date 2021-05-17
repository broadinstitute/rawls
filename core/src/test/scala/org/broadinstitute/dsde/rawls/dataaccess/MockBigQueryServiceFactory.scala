package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID
import cats.effect._
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.Acl.Entity
import com.google.cloud.bigquery.Dataset.Builder
import com.google.cloud.bigquery.{Acl, BigQuery, Dataset, DatasetId, DatasetInfo, Field, FieldValue, FieldValueList, JobId, LegacySQLTypeName, QueryJobConfiguration, Schema, Table, TableInfo, TableResult}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/*
 * Mocks for GoogleBigQueryServiceFactory and GoogleBigQueryService for use in unit tests.
 *
 * These mocks allow unit-test callers to specify the BigQuery result payload, and/or
 * specify that the query() method throws an exception.
 *
 * This file also contains the default fixture data returned by MockGoogleBigQueryService.query()
 * in the case where a caller did not override that result.
 */

object MockBigQueryServiceFactory {
  val defaultRowCount = 3

  // default fixture data returned by the underlying MockGoogleBigQueryService, unless a caller overrides it
  val F_INTEGER = Field.of("integer-field", LegacySQLTypeName.INTEGER)
  val F_BOOLEAN = Field.of("boolean-field", LegacySQLTypeName.BOOLEAN)
  val F_STRING = Field.of("datarepo_row_id", LegacySQLTypeName.STRING)
  val F_TIMESTAMP = Field.of("timestamp-field", LegacySQLTypeName.TIMESTAMP)

  val FV_INTEGER = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "42")
  val FV_BOOLEAN = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "true")
  val FV_TIMESTAMP = FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, "1408452095.22")

  val tableResult: TableResult = createTestTableResult(defaultRowCount)

  def createKeyList(n: Int): List[String] = {
    List.tabulate(n)(i => "Row" + i)
  }

  def createTestTableResult(tableRowCount: Int): TableResult = {
    val schema: Schema = Schema.of(F_BOOLEAN, F_STRING, F_INTEGER, F_TIMESTAMP)

    val stringKeys = createKeyList(tableRowCount)

    val results = stringKeys map { stringKey  =>
      FieldValueList.of(List(FV_BOOLEAN, FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey), FV_INTEGER, FV_TIMESTAMP).asJava,
        F_BOOLEAN, F_STRING, F_INTEGER, F_TIMESTAMP)
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, tableRowCount, page)
    tableResult
  }

  def createTestTableResultWithNestedStruct(tableRowCount: Int, nestedFieldName: String): TableResult = {
    val nestedField = Field.newBuilder(nestedFieldName, LegacySQLTypeName.RECORD, F_BOOLEAN, F_STRING, F_INTEGER).setMode(Field.Mode.REPEATED).build()
    val schema: Schema = Schema.of(F_STRING, nestedField)

    val stringKeys = createKeyList(tableRowCount)

    val results = stringKeys map { stringKey  =>
      val subKeyList = createKeyList(tableRowCount)
      val nestedRecord = subKeyList.map { subKey =>
        FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.RECORD, FieldValueList.of(
          List(FV_BOOLEAN, FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, subKey), FV_INTEGER).asJava,
          F_BOOLEAN, F_STRING, F_INTEGER))
      }.asJava

      FieldValueList.of(
        List(
          FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, stringKey),
          FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.REPEATED, nestedRecord)
        ).asJava,
        F_STRING, nestedField)
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, tableRowCount, page)
    tableResult
  }

  def ioFactory(queryResponse: Either[Throwable, TableResult] = Right(tableResult)): MockBigQueryServiceFactory = {
    lazy val blocker = Blocker.liftExecutionContext(TestExecutionContext.testExecutionContext)
    implicit val ec = TestExecutionContext.testExecutionContext

    new MockBigQueryServiceFactory(blocker, queryResponse)
  }

}

class MockBigQueryServiceFactory(blocker: Blocker, queryResponse: Either[Throwable, TableResult])(implicit val executionContext: ExecutionContext)
  extends GoogleBigQueryServiceFactory(blocker: Blocker)(executionContext: ExecutionContext) {

  override def getServiceForPet(petKey: String, projectId: GoogleProject): Resource[IO, GoogleBigQueryService[IO]] = {
    Resource.pure[IO, GoogleBigQueryService[IO]](new MockGoogleBigQueryService(queryResponse))
  }

  override def getServiceFromCredentialPath(credentialPath: String, projectId: GoogleProject): Resource[IO, GoogleBigQueryService[IO]] = {
    Resource.pure[IO, GoogleBigQueryService[IO]](new MockGoogleBigQueryService(queryResponse))
  }
}

class MockGoogleBigQueryService(queryResponse: Either[Throwable, TableResult]) extends GoogleBigQueryService[IO] {
  override def query(queryJobConfiguration: QueryJobConfiguration, options: BigQuery.JobOption*): IO[TableResult] =
    query(queryJobConfiguration, JobId.newBuilder().setJob(UUID.randomUUID().toString).build(), options: _*)

  override def query(queryJobConfiguration: QueryJobConfiguration, jobId: JobId, options: BigQuery.JobOption*): IO[TableResult] = {
    queryResponse match {
      case Left(t) => throw t
      case Right(results) => IO.pure(results)
    }
  }

  override def createDataset(datasetName: String, labels: Map[String, String], aclBindings: Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]): IO[DatasetId] = {
    IO.pure(DatasetInfo.newBuilder(datasetName).build().getDatasetId)
  }

  override def deleteDataset(datasetName: String): IO[Boolean] = IO.pure(true)

  override def getTable(datasetName: String, tableName: String): IO[Option[Table]] = {
    if(tableName.equals("gcp_billing_export_v1_table_does_not_exist")) IO.none
    else IO.pure(Some(null))
  }

  override def getDataset(datasetName: String): IO[Option[Dataset]] = {
    if(datasetName.equals("dataset_does_not_exist")) IO.none
    else IO.pure(Some(null))
  }
}
