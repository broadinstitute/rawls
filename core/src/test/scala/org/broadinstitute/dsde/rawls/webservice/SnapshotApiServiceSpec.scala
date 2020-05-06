package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.model.{DataRepoSnapshot, DataRepoSnapshotReference}

import scala.concurrent.ExecutionContext

class SnapshotApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {

    val gcsDAO = new MockGoogleServicesDAO("test")
    gcsDAO.storeToken(userInfo, "test_token")

    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withEmptyTestDataApiServices[T](testCode: TestApiService => T): T = {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  it should "return 201 when creating a reference to a snapshot" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      DataRepoSnapshot(
        name = "foo",
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }
  }

  it should "return 404 when creating a reference to a snapshot that doesn't exist" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      DataRepoSnapshot(
        name = "foo",
        snapshotId = "fakesnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 when creating a reference to a snapshot in a workspace that doesn't exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/foo/bar/snapshots", httpJson(
      DataRepoSnapshot(
        name = "foo",
        snapshotId = "bar"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 200 when getting a reference to a snapshot" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      DataRepoSnapshot(
        name = "foo",
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        val response = responseAs[DataRepoSnapshotReference]
        assertResult(StatusCodes.Created) {status}

        Get(s"${testData.wsName.path}/snapshots/${response.referenceId}") ~>
          sealRoute(services.snapshotRoutes) ~>
          check { assertResult(StatusCodes.OK) {status} }
      }
  }

  it should "return 404 when getting a reference to a snapshot that doesn't exist" in withTestDataApiServices { services =>
    Get(s"${testData.wsName.path}/snapshots/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 when getting a reference to a snapshot in a workspace that doesn't exist" in withTestDataApiServices { services =>
    Get(s"/workspaces/foo/bar/snapshots/doesntmatter") ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

}
