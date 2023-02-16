package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestData
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.language.reflectiveCalls

class PetSASpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource,
                            user: RawlsUser,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with StandardUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: RawlsUser = RawlsUser(userInfo))(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

/// Create workspace to test switch -- this workspace is accessible by a User with petSA and a regular SA
  val petSA = UserInfo(RawlsUserEmail("pet-123456789876543212345@abc.iam.gserviceaccount.com"),
                       OAuth2BearerToken("token"),
                       123,
                       RawlsUserSubjectId("123456789876")
  )
  "WorkspaceApi" should "return 201 for post to workspaces with Pet SA" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~> addHeader("OIDC_access_token",
                                                              petSA.accessToken.value
    ) ~> addHeader("OIDC_CLAIM_expires_in", petSA.accessTokenExpiresIn.toString) ~> addHeader("OIDC_CLAIM_email",
                                                                                              petSA.userEmail.value
    ) ~> addHeader("OIDC_CLAIM_user_id", petSA.userSubjectId.value) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status

        }
        assertResult(newWorkspace) {
          val ws = runAndWait(workspaceQuery.findByName(newWorkspace.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.attributes, Option(Set.empty))
        }
        assertResult(newWorkspace) {
          val ws = responseAs[WorkspaceDetails]
          WorkspaceRequest(ws.namespace, ws.name, ws.attributes.getOrElse(Map()), Option(Set.empty))
        }
      }
  }

//get a workspace with a service account
  it should "get a workspace using regular SA" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path) ~> addHeader("OIDC_access_token",
                                                    testWorkspaces.userSAProjectOwnerUserInfo.accessToken.value
    ) ~> addHeader("OIDC_CLAIM_expires_in",
                   testWorkspaces.userSAProjectOwnerUserInfo.accessTokenExpiresIn.toString
    ) ~> addHeader("OIDC_CLAIM_email", testWorkspaces.userSAProjectOwnerUserInfo.userEmail.value) ~> addHeader(
      "OIDC_CLAIM_user_id",
      testWorkspaces.userSAProjectOwnerUserInfo.userSubjectId.value
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = currentTime()
        assertResult(
          WorkspaceResponse(
            Option(WorkspaceAccessLevels.Owner),
            Option(true),
            Option(true),
            Option(true),
            WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = dateTime),
                                                     Some(Set()),
                                                     true,
                                                     Some(WorkspaceCloudPlatform.Gcp)
            ),
            Option(WorkspaceSubmissionStats(None, None, 0)),
            Option(WorkspaceBucketOptions(false)),
            Option(Set.empty),
            None
          )
        ) {
          val response = responseAs[WorkspaceResponse]
          WorkspaceResponse(
            response.accessLevel,
            response.canShare,
            response.canCompute,
            response.catalog,
            response.workspace.copy(lastModified = dateTime),
            response.workspaceSubmissionStats,
            response.bucketOptions,
            response.owners,
            None
          )
        }
      }
  }

//get a workspace with a pet service account
  it should "get a workspace using pet SA" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path) ~> addHeader("OIDC_access_token", petSA.accessToken.value) ~> addHeader(
      "OIDC_CLAIM_expires_in",
      petSA.accessTokenExpiresIn.toString
    ) ~> addHeader("OIDC_CLAIM_email", petSA.userEmail.value) ~> addHeader("OIDC_CLAIM_user_id",
                                                                           petSA.userSubjectId.value
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = currentTime()
        assertResult(
          WorkspaceResponse(
            Option(WorkspaceAccessLevels.Owner),
            Option(true),
            Option(true),
            Option(true),
            WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = dateTime),
                                                     Some(Set()),
                                                     true,
                                                     Some(WorkspaceCloudPlatform.Gcp)
            ),
            Option(WorkspaceSubmissionStats(None, None, 0)),
            Option(WorkspaceBucketOptions(false)),
            Option(Set.empty),
            None
          )
        ) {
          val response = responseAs[WorkspaceResponse]
          WorkspaceResponse(
            response.accessLevel,
            response.canShare,
            response.canCompute,
            response.catalog,
            response.workspace.copy(lastModified = dateTime),
            response.workspaceSubmissionStats,
            response.bucketOptions,
            response.owners,
            None
          )
        }
      }
  }

/////////////

  val testWorkspaces = new TestData {
    import driver.api._
    val userProjectOwner = RawlsUser(
      UserInfo(RawlsUserEmail("project-owner-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543210101")
      )
    )
    val userOwner = RawlsUser(
      UserInfo(testData.userOwner.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212345")
      )
    )
    val userWriter = RawlsUser(
      UserInfo(testData.userWriter.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(testData.userReader.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )
    val userSAProjectOwnerUserInfo = UserInfo(
      RawlsUserEmail("project-owner-access-sa@abc.iam.gserviceaccount.com"),
      OAuth2BearerToken("SA-but-not-pet-token"),
      123,
      RawlsUserSubjectId("123456789876543210202")
    )
    val userSAProjectOwner = RawlsUser(userSAProjectOwnerUserInfo)

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")

    val workspace1Id = UUID.randomUUID().toString
    val workspace = makeWorkspaceWithUsers(
      billingProject,
      workspaceName.name,
      workspace1Id,
      "bucket1",
      Some("workflow-collection"),
      testDate,
      testDate,
      "testUser",
      Map(AttributeName.withDefaultNS("a") -> AttributeString("x")),
      false
    )

    override def save() =
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        workspaceQuery.createOrUpdate(workspace)
      )
  }
}
