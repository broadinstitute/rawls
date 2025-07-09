package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.json.DefaultJsonProtocol.listFormat

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SnapshotApiServiceSpec extends ApiServiceSpec {

  val v3BaseSnapshotsPath = s"${testData.wsName.path}/snapshots/v3"
  val v3WorkspaceIdBaseSnapshotsPath = s"/workspaces/${testData.workspace.workspaceIdAsUUID}/snapshots/v3"

  case class TestApiService(dataSource: SlickDataSource,
                            user: String,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withApiServicesSecure[T](dataSource: SlickDataSource, withUser: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService: TestApiService =
      new TestApiService(dataSource, withUser, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO) {
        override val samDAO: MockSamDAO = new MockSamDAO(this.dataSource) {
          override def userHasAction(resourceTypeName: SamResourceTypeName,
                                     resourceId: String,
                                     action: SamResourceAction,
                                     cts: RawlsRequestContext
          ): Future[Boolean] = {

            val result = user match {
              case testData.userReader.userEmail.value => Set(SamWorkspaceActions.read).contains(action)
              case _                                   => false
            }
            Future.successful(result)
          }
        }
      }
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServicesSecure(dataSource, user) { services =>
        testCode(services)
      }
    }

  "SnapshotV3ApiService" should "return 204 when creating multiple snapshots using workspaceName" in withTestDataApiServices {
    services =>
      Post(v3BaseSnapshotsPath, List(UUID.randomUUID, UUID.randomUUID)) ~>
        sealRoute(services.snapshotRoutes(userInfo = userInfo)) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
  }

  it should "return 204 when creating multiple snapshots using workspaceId" in withTestDataApiServices { services =>
    Post(v3WorkspaceIdBaseSnapshotsPath, List(UUID.randomUUID, UUID.randomUUID)) ~>
      sealRoute(services.snapshotRoutes(userInfo = userInfo)) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }
}
