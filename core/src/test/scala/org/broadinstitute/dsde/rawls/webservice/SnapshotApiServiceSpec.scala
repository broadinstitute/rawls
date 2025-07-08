package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.json.DefaultJsonProtocol.listFormat

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SnapshotApiServiceSpec extends ApiServiceSpec {

  val v3BaseSnapshotsPath = s"${testData.wsName.path}/snapshots/v3"
  val v3WorkspaceIdBaseSnapshotsPath = s"/workspaces/${testData.workspace.workspaceIdAsUUID}/snapshots/v3"

  // base MockWorkspaceManagerDAO always returns a value for enumerateDataReferences.
  // this version, used inside this spec, throws errors on specific workspaces,
  // but otherwise returns a value.
  class SnapshotApiServiceSpecWorkspaceManagerDAO extends MockWorkspaceManagerDAO {
    override def enumerateDataRepoSnapshotReferences(workspaceId: UUID,
                                                     offset: Int,
                                                     limit: Int,
                                                     ctx: RawlsRequestContext
    ): ResourceList =
      workspaceId match {
        case testData.workspaceTerminatedSubmissions.workspaceIdAsUUID =>
          throw new ApiException(404, "unit test intentional not-found")
        case testData.workspaceSubmittedSubmission.workspaceIdAsUUID =>
          throw new ApiException(418, "unit test intentional teapot")
        case _ =>
          super.enumerateDataRepoSnapshotReferences(workspaceId, offset, limit, ctx)
      }

  }

  case class TestApiService(dataSource: SlickDataSource,
                            user: String,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO,
                            override val workspaceManagerDAO: MockWorkspaceManagerDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource,
                                        user,
                                        new MockGoogleServicesDAO("test"),
                                        new MockGooglePubSubDAO,
                                        new SnapshotApiServiceSpecWorkspaceManagerDAO()
    )
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withApiServicesSecure[T](dataSource: SlickDataSource, withUser: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource,
                                        withUser,
                                        new MockGoogleServicesDAO("test"),
                                        new MockGooglePubSubDAO,
                                        new SnapshotApiServiceSpecWorkspaceManagerDAO()
    ) {
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
