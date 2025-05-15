package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{ColumnModel, RelationshipModel, SnapshotModel, TableModel}
import com.fasterxml.jackson.databind.ObjectMapper
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.Header
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class HttpDataRepoDAOSpec extends AnyFlatSpec with TestDriverComponent with Matchers {

  val mapper = new ObjectMapper()

  val defaultTables: List[TableModel] = List(
    new TableModel()
      .name("table1")
      .primaryKey(null)
      .rowCount(10)
      .columns(List("integer-field", "boolean-field", "timestamp-field").map(new ColumnModel().name(_)).asJava),
    new TableModel()
      .name("table2")
      .primaryKey(List("table2PK").asJava)
      .rowCount(123)
      .columns(List("col2a", "col2b").map(new ColumnModel().name(_)).asJava),
    new TableModel()
      .name("table3")
      .primaryKey(List("compound", "pk").asJava)
      .rowCount(456)
      .columns(List("col3.1", "col3.2").map(new ColumnModel().name(_)).asJava)
  )

  /* A "factory" method to create SnapshotModel objects, with default.
   */
  def createSnapshotModel(tables: List[TableModel] = defaultTables,
                          relationships: List[RelationshipModel] = List.empty
  ): SnapshotModel =
    new SnapshotModel()
      .id(snapshotUUID)
      .tables(tables.asJava)
      .relationships(relationships.asJava)
      .dataProject("unittest-dataproject")
      .name("unittest-name")
      .relationships(relationships.asJava)

  behavior of "HttpDataRepoDAO"

  it should "add datarepo_row_id to snapshot tables" in {

    // Mock the Data Repo server to return a snapshot model (datarepo_row_ids not included)
    val jsonHeader = new Header("Content-Type", "application/json")
    val mockPort = 32123
    val snapshotModel = mapper.writeValueAsString(createSnapshotModel())

    val mockServer = startClientAndServer(mockPort)
    mockServer
      .when(
        request()
          .withMethod("GET")
          .withPath(s"/api/repository/v1/snapshots/${snapshotUUID.toString}")
      )
      .respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(snapshotModel)
          .withStatusCode(StatusCodes.OK.intValue)
      )

    val dataRepoDAO = new HttpDataRepoDAO(s"http://localhost:$mockPort")
    val snapshotResponse = dataRepoDAO.getSnapshot(snapshotUUID, userInfo.accessToken)
    mockServer.stopAsync()

    snapshotResponse.getId shouldBe snapshotUUID
    snapshotResponse.getTables().foreach {
      _.getColumns.filter(col => col.getName() == "datarepo_row_id") should not be empty
    }
  }

  val snapshotUUID: UUID = UUID.randomUUID()
}
