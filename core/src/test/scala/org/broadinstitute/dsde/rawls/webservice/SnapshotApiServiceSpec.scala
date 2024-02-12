package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class SnapshotApiServiceSpec extends ApiServiceSpec {

  val v2BaseSnapshotsPath = s"${testData.wsName.path}/snapshots/v2"

  val defaultNamedSnapshotJson = httpJson(
    NamedDataRepoSnapshot(
      name = DataReferenceName("foo"),
      description = Option(DataReferenceDescriptionField("bar")),
      snapshotId = UUID.randomUUID()
    )
  )
  val defaultSnapshotUpdateBodyJson = httpJson(
    new UpdateDataRepoSnapshotReferenceRequestBody().name("foo2").description("bar2")
  )

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

  "SnapshotV2ApiService" should "return 201 when creating a reference to a snapshot" in withTestDataApiServices {
    services =>
      Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
  }

  it should "return 404 when creating a reference to a snapshot that doesn't exist" in withTestDataApiServices {
    services =>
      Post(
        v2BaseSnapshotsPath,
        httpJson(
          NamedDataRepoSnapshot(
            name = DataReferenceName("fakesnapshot"),
            description = Option(DataReferenceDescriptionField("bar")),
            snapshotId = UUID.randomUUID()
          )
        )
      ) ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 404 when creating a reference to a snapshot in a workspace that doesn't exist" in withTestDataApiServices {
    services =>
      Post("/workspaces/foo/bar/snapshots/v2", defaultNamedSnapshotJson) ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 200 when getting a reference to a snapshot" in withTestDataApiServices { services =>
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        val response = responseAs[DataRepoSnapshotResource]
        assertResult(StatusCodes.Created) {
          status
        }

        Get(s"${v2BaseSnapshotsPath}/${response.getMetadata.getResourceId}") ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
          }
      }
  }

  it should "return 400 when getting a reference to a snapshot that is not a valid UUID" in withTestDataApiServices {
    services =>
      Get(s"${v2BaseSnapshotsPath}/not-a-valid-uuid") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 404 when getting a reference to a snapshot that doesn't exist" in withTestDataApiServices {
    services =>
      Get(s"${v2BaseSnapshotsPath}/${UUID.randomUUID().toString}") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 404 when getting a reference to a snapshot in a workspace that doesn't exist" in withTestDataApiServices {
    services =>
      Get(s"/workspaces/foo/bar/snapshots/v2/${UUID.randomUUID().toString}") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 200 when getting a reference to a snapshot-by-name" in withTestDataApiServices { services =>
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        val response = responseAs[DataRepoSnapshotResource]
        assertResult(StatusCodes.Created) {
          status
        }

        Get(s"${v2BaseSnapshotsPath}/name/${response.getMetadata.getName}") ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
          }
      }
  }

  it should "return 404 when getting a reference to a snapshot-by-name that doesn't exist" in withTestDataApiServices {
    services =>
      Get(s"${v2BaseSnapshotsPath}/name/reference-intentionally-does-not-exist") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 404 when getting a reference to a snapshot-by-name in a workspace that doesn't exist" in withTestDataApiServices {
    services =>
      Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          val response = responseAs[DataRepoSnapshotResource]
          assertResult(StatusCodes.Created) {
            status
          }

          Get(s"/workspaces/foo/bar/snapshots/v2/name/${response.getMetadata.getName}") ~>
            sealRoute(services.snapshotRoutes()) ~>
            check {
              assertResult(StatusCodes.NotFound) {
                status
              }
            }
        }
  }

  it should "return 403 when a user can only read a workspace and tries to add a snapshot" in withTestDataApiServicesAndUser(
    testData.userReader.userEmail.value
  ) { services =>
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 404 when a user tries to add a snapshot to a workspace that they don't have access to" in withTestDataApiServicesAndUser(
    "no-access"
  ) { services =>
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when a user tries to get a snapshot from a workspace that they don't have access to" in withTestDataApiServicesAndUser(
    "no-access"
  ) { services =>
    Get(s"${v2BaseSnapshotsPath}/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 when a user lists all snapshots in a workspace" in withTestDataApiServices { services =>
    // First, create two data references
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        val response = responseAs[DataRepoSnapshotResource]
        assertResult(StatusCodes.Created) {
          status
        }
        Post(
          v2BaseSnapshotsPath,
          httpJson(
            NamedDataRepoSnapshot(
              name = DataReferenceName("bar"),
              description = Option(DataReferenceDescriptionField("bar")),
              snapshotId = UUID.randomUUID()
            )
          )
        ) ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            val response = responseAs[DataRepoSnapshotResource]
            assertResult(StatusCodes.Created) {
              status
            }
            // Then, list them both
            Get(s"${v2BaseSnapshotsPath}?offset=0&limit=10") ~>
              sealRoute(services.snapshotRoutes()) ~>
              check {
                val response = responseAs[SnapshotListResponse]
                assertResult(StatusCodes.OK) {
                  status
                }
                // Our mock doesn't guarantee order, so we just check that there are two
                // elements, that one is named "foo", and that one is named "bar"
                val resources = response.gcpDataRepoSnapshots
                assert(resources.size == 2)
                assertResult(Set("foo", "bar")) {
                  resources.map(_.getMetadata.getName).toSet
                }
              }
          }
      }

  }

  it should "return 404 when a user lists references without workspace read permission" in withTestDataApiServicesAndUser(
    "no-access"
  ) { services =>
    Get(s"${v2BaseSnapshotsPath}?offset=0&limit=10") ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when a user lists references in a workspace that doesn't exist" in withTestDataApiServicesAndUser(
    testData.userReader.userEmail.value
  ) { services =>
    Get("/workspaces/test/value/snapshots/v2?offset=0&limit=10") ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 and empty list when a user lists all snapshots in a workspace that exists in Rawls but not Workspace Manager" in withTestDataApiServices {
    services =>
      // We hijack the "workspaceTerminatedSubmissions" workspace in the shared testData to represent
      // a workspace that exists in Rawls but returns 404 from Workspace Manager.
      Get(s"${testData.workspaceTerminatedSubmissions.path}/snapshots/v2?offset=0&limit=10") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          val response = responseAs[SnapshotListResponse]
          assertResult(StatusCodes.OK) {
            status
          }
          assert(response.gcpDataRepoSnapshots.isEmpty)
        }
  }

  it should "bubble up non-404 errors from Workspace Manager" in withTestDataApiServices { services =>
    // We hijack the "workspaceSubmittedSubmission" workspace in the shared testData to represent
    // a workspace that throws a 418 error.
    Get(s"${testData.workspaceSubmittedSubmission.path}/snapshots/v2?offset=0&limit=10") ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.ImATeapot) {
          response.status
        }
      }
  }

  it should "return 204 when a user updates a snapshot" in withTestDataApiServices { services =>
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        val response = responseAs[DataRepoSnapshotResource]
        assertResult(StatusCodes.Created, "Unexpected snapshot creation status") {
          status
        }
        Patch(s"${v2BaseSnapshotsPath}/${response.getMetadata.getResourceId}", defaultSnapshotUpdateBodyJson) ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            assertResult(StatusCodes.NoContent, "Unexpected snapshot update response") {
              status
            }
          }

        // verify that it was updated
        Get(s"${v2BaseSnapshotsPath}/${response.getMetadata.getResourceId}") ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            val response = responseAs[DataRepoSnapshotResource]
            assertResult(StatusCodes.OK, "Unexpected return code getting updated snapshot") {
              status
            }
            assert(response.getMetadata.getName == "foo2", "Unexpected result of updating snapshot name")
            assert(response.getMetadata.getDescription == "bar2", "Unexpected result of updating snapshot description")
          }
      }
  }

  it should "return 400 when a user tries to update a snapshot that is not a valid UUID" in withTestDataApiServices {
    services =>
      Patch(s"${v2BaseSnapshotsPath}/not-a-valid-uuid", defaultSnapshotUpdateBodyJson) ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 403 when a user can only read a workspace and tries to update a snapshot" in withTestDataApiServicesAndUser(
    "reader-access"
  ) { services =>
    Patch(s"${v2BaseSnapshotsPath}/${UUID.randomUUID().toString}", defaultSnapshotUpdateBodyJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 404 when a user tries to update a snapshot from a workspace that they don't have access to" in withTestDataApiServicesAndUser(
    "no-access"
  ) { services =>
    val id = services.workspaceManagerDAO
      .createDataRepoSnapshotReference(
        UUID.fromString(testData.workspace.workspaceId),
        UUID.randomUUID(),
        DataReferenceName("test"),
        Option(DataReferenceDescriptionField("description")),
        "foo",
        CloningInstructionsEnum.NOTHING,
        testContext
      )
      .getMetadata
      .getResourceId
    Patch(s"${v2BaseSnapshotsPath}/${id.toString}", defaultSnapshotUpdateBodyJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when a user tries to update a snapshot that doesn't exist" in withTestDataApiServices {
    services =>
      Patch(s"${v2BaseSnapshotsPath}/${UUID.randomUUID().toString}", defaultSnapshotUpdateBodyJson) ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 204 when a user deletes a snapshot" in withTestDataApiServices { services =>
    Post(v2BaseSnapshotsPath, defaultNamedSnapshotJson) ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        val response = responseAs[DataRepoSnapshotResource]
        assertResult(StatusCodes.Created) {
          status
        }
        Delete(s"${v2BaseSnapshotsPath}/${response.getMetadata.getResourceId}") ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            assertResult(StatusCodes.NoContent) {
              status
            }
          }

        // verify that it was deleted
        Delete(s"${v2BaseSnapshotsPath}/${response.getMetadata.getResourceId}") ~>
          sealRoute(services.snapshotRoutes()) ~>
          check {
            assertResult(StatusCodes.NotFound) {
              status
            }
          }
      }
  }

  it should "return 400 when a user tries to delete a snapshot that is not a valid UUID" in withTestDataApiServices {
    services =>
      Delete(s"${v2BaseSnapshotsPath}/not-a-valid-uuid") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 403 when a user can only read a workspace and tries to delete a snapshot" in withTestDataApiServicesAndUser(
    "reader-access"
  ) { services =>
    Delete(s"${v2BaseSnapshotsPath}/${UUID.randomUUID().toString}") ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 404 when a user tries to delete a snapshot from a workspace that they don't have access to" in withTestDataApiServicesAndUser(
    "no-access"
  ) { services =>
    val id = services.workspaceManagerDAO
      .createDataRepoSnapshotReference(
        UUID.fromString(testData.workspace.workspaceId),
        UUID.randomUUID(),
        DataReferenceName("test"),
        Option(DataReferenceDescriptionField("description")),
        "foo",
        CloningInstructionsEnum.NOTHING,
        testContext
      )
      .getMetadata
      .getResourceId
    Delete(s"${v2BaseSnapshotsPath}/${id.toString}") ~>
      sealRoute(services.snapshotRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 when a user tries to delete a snapshot that doesn't exist" in withTestDataApiServices {
    services =>
      Delete(s"${v2BaseSnapshotsPath}/${UUID.randomUUID().toString}") ~>
        sealRoute(services.snapshotRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

}
