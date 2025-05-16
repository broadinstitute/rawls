package org.broadinstitute.dsde.rawls.dataaccess.datarepo

import akka.http.scaladsl.model.StatusCodes
import com.fasterxml.jackson.databind.ObjectMapper
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.datarepo.{DataRepoBigQuerySupport, DataRepoEntityProviderSpecSupport}
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.Header
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.language.postfixOps

class HttpDataRepoDAOSpec
    extends AnyFlatSpec
    with TestDriverComponent
    with Matchers
    with DataRepoEntityProviderSpecSupport {

  val mapper = new ObjectMapper()

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

    val dataRepoDAO = new HttpDataRepoDAO("mock", s"http://localhost:$mockPort")
    val snapshotResponse = dataRepoDAO.getSnapshot(snapshotUUID, userInfo.accessToken)
    mockServer.stopAsync()

    snapshotResponse.getId shouldBe snapshotUUID
    snapshotResponse.getTables().foreach {
      _.getColumns.filter(col => col.getName() == DataRepoBigQuerySupport.datarepoRowIdColumn) should not be empty
    }
  }
}
