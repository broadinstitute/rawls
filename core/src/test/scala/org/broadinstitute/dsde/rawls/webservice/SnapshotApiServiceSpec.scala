package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{CloningInstructionsEnum, DataReferenceDescription, DataReferenceList, DataRepoSnapshot, ReferenceTypeEnum}
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.{DataReferenceName, ErrorReport, NamedDataRepoSnapshot, SamResourceAction, SamResourceTypeName, SamWorkspaceActions, UserInfo}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class SnapshotApiServiceSpec extends ApiServiceSpec {

  // base MockWorkspaceManagerDAO always returns a value for enumerateDataReferences.
  // this version, used inside this spec, throws errors on specific workspaces,
  // but otherwise returns a value.
  class SnapshotApiServiceSpecWorkspaceManagerDAO extends MockWorkspaceManagerDAO {
    override def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList = {
      workspaceId match {
        case testData.workspaceTerminatedSubmissions.workspaceIdAsUUID =>
          throw new ApiException(404, "unit test intentional not-found")
        case testData.workspaceSubmittedSubmission.workspaceIdAsUUID =>
          throw new ApiException(418, "unit test intentional teapot")
        case _ =>
          super.enumerateDataReferences(workspaceId, offset, limit, accessToken)
      }

    }
  }

  case class TestApiService(dataSource: SlickDataSource, user: String, gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO, override val workspaceManagerDAO: MockWorkspaceManagerDAO)
                           (implicit override val executionContext: ExecutionContext)
    extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {

    val gcsDAO = new MockGoogleServicesDAO("test")
    gcsDAO.storeToken(userInfo, "test_token")

    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO, new SnapshotApiServiceSpecWorkspaceManagerDAO())
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withApiServicesSecure[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO, new SnapshotApiServiceSpecWorkspaceManagerDAO()) {
      override val samDAO: MockSamDAO = new MockSamDAO(dataSource) {
        override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {

          val result = user match {
            case testData.userReader.userEmail.value => Set(SamWorkspaceActions.read).contains(action)
            case _ => false
          }
          Future.successful(result)
        }
      }
    }
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

  def withTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServicesSecure(dataSource, user) { services =>
        testCode(services)
      }
    }
  }

  "SnapshotApiService" should "return 201 when creating a reference to a snapshot" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }

  it should "return 404 when creating a reference to a snapshot that doesn't exist" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "fakesnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when creating a reference to a snapshot in a workspace that doesn't exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/foo/bar/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "bar"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 when getting a reference to a snapshot" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        val response = responseAs[DataReferenceDescription]
        assertResult(StatusCodes.Created) {
          status
        }

        Get(s"${testData.wsName.path}/snapshots/${response.getReferenceId}") ~>
          sealRoute(services.snapshotRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
          }
      }
  }

  it should "return 400 when getting a reference to a snapshot that is not a valid UUID" in withTestDataApiServices { services =>
    Get(s"${testData.wsName.path}/snapshots/not-a-valid-uuid") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 404 when getting a reference to a snapshot that doesn't exist" in withTestDataApiServices { services =>
    Get(s"${testData.wsName.path}/snapshots/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when getting a reference to a snapshot in a workspace that doesn't exist" in withTestDataApiServices { services =>
    Get(s"/workspaces/foo/bar/snapshots/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when a user can only read a workspace and tries to add a snapshot" in withTestDataApiServicesAndUser(testData.userReader.userEmail.value) { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 404 when a user tries to add a snapshot to a workspace that they don't have access to" in withTestDataApiServicesAndUser("no-access") { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when a user tries to get a snapshot from a workspace that they don't have access to" in withTestDataApiServicesAndUser("no-access") { services =>
    Get(s"${testData.wsName.path}/snapshots/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 when a user lists all snapshots in a workspace" in withTestDataApiServices { services =>
    // First, create two data references
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        val response = responseAs[DataReferenceDescription]
        assertResult(StatusCodes.Created) {status}
        Post(s"${testData.wsName.path}/snapshots", httpJson(
          NamedDataRepoSnapshot(
            name = DataReferenceName("bar"),
            snapshotId = "realsnapshot2"
          )
        )) ~>
          sealRoute(services.snapshotRoutes) ~>
          check {
            val response = responseAs[DataReferenceDescription]
            assertResult(StatusCodes.Created) {status}
            // Then, list them both
            Get(s"${testData.wsName.path}/snapshots?offset=0&limit=10") ~>
              sealRoute(services.snapshotRoutes) ~>
              check {
                val response = responseAs[DataReferenceList]
                assertResult(StatusCodes.OK) {status}
                // Our mock doesn't guarantee order, so we just check that there are two
                // elements, that one is named "foo", and that one is named "bar"
                assert(response.getResources.size == 2)
                assertResult(Set("foo", "bar")) { response.getResources.asScala.map(_.getName).toSet }
              }
          }
      }

  }

  it should "return 404 when a user lists references without workspace read permission" in withTestDataApiServicesAndUser("no-access") { services =>
    Get(s"${testData.wsName.path}/snapshots?offset=0&limit=10") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when a user lists references in a workspace that doesn't exist" in withTestDataApiServicesAndUser(testData.userReader.userEmail.value) { services =>
    Get(s"/workspaces/test/value/snapshots?offset=0&limit=10") ~>
    sealRoute(services.snapshotRoutes) ~>
    check {
      assertResult(StatusCodes.NotFound) {
        status
      }
    }
  }

  it should "return 200 and empty list when a user lists all snapshots in a workspace that exists in Rawls but not Workspace Manager" in withTestDataApiServices { services =>
    // We hijack the "workspaceTerminatedSubmissions" workspace in the shared testData to represent
    // a workspace that exists in Rawls but returns 404 from Workspace Manager.
    Get(s"${testData.workspaceTerminatedSubmissions.path}/snapshots?offset=0&limit=10") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        val response = responseAs[DataReferenceList]
        assertResult(StatusCodes.OK) {status}
        assert(response.getResources.isEmpty)
      }
  }

  it should "bubble up non-404 errors from Workspace Manager" in withTestDataApiServices { services =>
    // We hijack the "workspaceSubmittedSubmission" workspace in the shared testData to represent
    // a workspace that throws a 418 error.
    Get(s"${testData.workspaceSubmittedSubmission.path}/snapshots?offset=0&limit=10") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.ImATeapot) {response.status}
      }
  }

  it should "return 204 when a user deletes a snapshot" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/snapshots", httpJson(
      NamedDataRepoSnapshot(
        name = DataReferenceName("foo"),
        snapshotId = "realsnapshot"
      )
    )) ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        val response = responseAs[DataReferenceDescription]
        assertResult(StatusCodes.Created) {status}
        Delete(s"${testData.wsName.path}/snapshots/${response.getReferenceId}") ~>
          sealRoute(services.snapshotRoutes) ~>
          check { assertResult(StatusCodes.NoContent) {status} }

        //verify that it was deleted
        Delete(s"${testData.wsName.path}/snapshots/${response.getReferenceId}") ~>
          sealRoute(services.snapshotRoutes) ~>
          check { assertResult(StatusCodes.NotFound) {status} }
      }
  }

  it should "return 400 when a user tries to delete a snapshot that is not a valid UUID" in withTestDataApiServices { services =>
    Delete(s"${testData.wsName.path}/snapshots/not-a-valid-uuid") ~>
      sealRoute(services.snapshotRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 403 when a user can only read a workspace and tries to delete a snapshot" in withTestDataApiServicesAndUser("reader-access") { services =>
    Delete(s"${testData.wsName.path}/snapshots/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.Forbidden) {status} }
  }

  it should "return 404 when a user tries to delete a snapshot from a workspace that they don't have access to" in withTestDataApiServicesAndUser("no-access") { services =>
    val dataReference = new DataRepoSnapshot().instanceName("foo").snapshot("bar")
    val id = services.workspaceManagerDAO.createDataReference(UUID.fromString(testData.workspace.workspaceId), DataReferenceName("test"), ReferenceTypeEnum.DATA_REPO_SNAPSHOT, dataReference, CloningInstructionsEnum.NOTHING, OAuth2BearerToken("foo")).getReferenceId
    Delete(s"${testData.wsName.path}/snapshots/${id.toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 when a user tries to delete a snapshot that doesn't exist" in withTestDataApiServices { services =>
    Delete(s"${testData.wsName.path}/snapshots/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }
}
