package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import bio.terra.datarepo.model.TableModel
import bio.terra.workspace.model.DataReferenceDescription.ReferenceTypeEnum
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{FieldValueList, TableResult}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory
import org.broadinstitute.dsde.rawls.dataaccess.MockBigQueryServiceFactory.{results, schema}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{AsyncFlatSpec, Matchers}
import spray.json.{JsArray, JsNumber, JsObject, JsString}

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
        ("table1", EntityTypeMetadata(0, "datarepo_row_id", Seq())),
        ("table2", EntityTypeMetadata(123, "table2PK", Seq("col2.1", "col2.2"))),
        ("table3", EntityTypeMetadata(456, "datarepo_row_id", Seq("col3.1", "col3.2"))))
      assertResult(expected) { metadata }
    }
  }

  it should "return an empty Map if data repo snapshot has no tables" in {
    val provider = createTestProvider(
      dataRepoDAO = new SpecDataRepoDAO(Right( createSnapshotModel( List.empty[TableModel] ) )))

    provider.entityTypeMetadata() map { metadata: Map[String, EntityTypeMetadata] =>
      assert(metadata.isEmpty, "expected response data to be the empty map")
    }
  }

  it should "bubble up error if workspace manager errors" in {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Left(new bio.terra.workspace.client.ApiException("whoops 1"))))

    val ex = intercept[bio.terra.workspace.client.ApiException] { provider.entityTypeMetadata() }
    assertResult("whoops 1") { ex.getMessage }
  }

  it should "bubble up error if data repo errors" in {
    val provider = createTestProvider(
      dataRepoDAO = new SpecDataRepoDAO(Left(new bio.terra.datarepo.client.ApiException("whoops 2"))))

    val ex = intercept[bio.terra.datarepo.client.ApiException] { provider.entityTypeMetadata() }
    assertResult("whoops 2") { ex.getMessage }
  }

  // to-do: tests for different primary key values returned by data repo, if TDR ever supports this.
  // currently, TDR returns null for PKs in snapshots, and this is expected according to the TDR team,
  // with no concrete plans to change.

  // to-do: tests for entity/row counts returned by data repo, once TDR supports this (see DR-1003)

  behavior of "DataEntityProvider.lookupSnapshotForName()"

  it should "return snapshot id in the golden path" in {
    val provider = createTestProvider()
    val actual = provider.lookupSnapshotForName("foo")
    assertResult(UUID.fromString(snapshot)) { actual }
  }

  it should "bubble up error if workspace manager errors" in  {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Left(new bio.terra.workspace.client.ApiException("whoops 1"))))

    val ex = intercept[bio.terra.workspace.client.ApiException] { provider.lookupSnapshotForName("foo") }
    assertResult("whoops 1") { ex.getMessage }
  }

  it should "error if workspace manager returns a non-snapshot reference type" in {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(referenceType = null)))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult( s"Reference type value for foo is not of type ${ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue}" ) { ex.getMessage }
  }

  it should "error if workspace manager returns something other than serialized json object in reference payload" in  {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refString = Some("not }{ json"))))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assert {
      // the remainder of the message is generated by the json parser and could change, we should not assert on it
      ex.getMessage.startsWith("Could not parse reference value for foo:")
    }
  }

  // warning: test could fail if spray-json changes the wording of their error messages
  it should "error if workspace manager reference json does not contain `instanceName` key" in  {
    // we have to drop to raw JsObjects to test malformed responses, since these shouldn't happen normally
    // given type safety
    val badRefPayload = JsObject.apply(("snapshot", JsString(snapshot)))

    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refString = Some(badRefPayload.compactPrint))))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult("Could not parse reference value for foo: Object is missing required member 'instanceName'") { ex.getMessage }
  }

  // warning: test could fail if spray-json changes the wording of their error messages
  it should "error if workspace manager reference json `instanceName` key is not a string" in  {
    // we have to drop to raw JsObjects to test malformed responses, since these shouldn't happen normally
    // given type safety
    val badRefPayload = JsObject.apply(("instanceName", JsArray(JsNumber(1), JsNumber(2))), ("snapshot", JsString(snapshot)))

    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refString = Some(badRefPayload.compactPrint))))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult("Could not parse reference value for foo: Expected String as JsString, but got [1,2]") { ex.getMessage }
  }

  // warning: test could fail if spray-json changes the wording of their error messages
  it should "error if workspace manager reference json does not contain `snapshot` key" in  {
    // we have to drop to raw JsObjects to test malformed responses, since these shouldn't happen normally
    // given type safety
    val badRefPayload = JsObject.apply(("instanceName", JsString(dataRepoInstanceName)))

    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refString = Some(badRefPayload.compactPrint))))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult("Could not parse reference value for foo: Object is missing required member 'snapshot'") { ex.getMessage }
  }

  // warning: test could fail if spray-json changes the wording of their error messages
  it should "error if workspace manager reference json `snapshot` key is not a string" in {
    // we have to drop to raw JsObjects to test malformed responses, since these shouldn't happen normally
    // given type safety
    val badRefPayload = JsObject.apply(("instanceName", JsString(dataRepoInstanceName)), ("snapshot", JsArray(JsNumber(1), JsNumber(2))))

    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refString = Some(badRefPayload.compactPrint))))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult("Could not parse reference value for foo: Expected String as JsString, but got [1,2]") { ex.getMessage }
  }

  it should "error if workspace manager reference json `instanceName` value does not match DataRepoDAO's base url" in {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refInstanceName = "this is wrong")))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult("Reference value for foo contains an unexpected instance name value") { ex.getMessage }
  }

  it should "error if workspace manager reference json `snapshot` value is not a valid UUID" in {
    val provider = createTestProvider(
      workspaceManagerDAO = new SpecWorkspaceManagerDAO(Right(createDataRefDescription(refSnapshot = "this is not a uuid")))
    )

    val ex = intercept[DataEntityException] { provider.lookupSnapshotForName("foo") }
    assertResult("Reference value for foo contains an unexpected snapshot value") { ex.getMessage }
  }

  behavior of "DataEntityProvider.getEntity()"

  it should "return exactly one entity if all OK" in {

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

  ignore should "bubble up error if workspace manager errors (includes reference not found?)" in {
    fail("not implemented")
  }

  ignore should "fail if data reference not found in workspace manager" in {
    fail("not implemented")
  }

  ignore should "fail if user is a workspace Reader but did not specify a billing project (canCompute?)" in {
    fail("not implemented")
  }

  ignore should "bubble up error if data repo errors (includes snapshot not found/not allowed?)" in {
    fail("not implemented")
  }

  ignore should "fail if snapshot not found in data repo" in {
    fail("not implemented")
  }

  ignore should "fail if snapshot has no tables in data repo" in {
    fail("not implemented")
  }

  ignore should "fail if snapshot table not found in data repo's response" in {
    fail("not implemented")
  }

  ignore should "fail if pet credentials not available from Sam" in {
    fail("not implemented")
  }

  ignore should "bubble up error if BigQuery errors" in {
    fail("not implemented")
  }

  ignore should "fail if BigQuery returns zero rows" in {
    fail("not implemented")
  }


}



