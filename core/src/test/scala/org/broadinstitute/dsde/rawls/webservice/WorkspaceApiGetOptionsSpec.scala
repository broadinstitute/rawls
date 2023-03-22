package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestData
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import spray.json._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/** Tests for the get-workspace API, focused on the user's ability to specify which fields
  * should be calculated and returned in the WorkspaceResponse.
  *
  * created ~ 9/9/19
  *
  * @author davidan
  */
class WorkspaceApiGetOptionsSpec extends ApiServiceSpec {
  import driver.api._

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(span: Option[Span]): Directive1[UserInfo] =
      // just return the cookie text as the common name
      user match {
        case testData.userProjectOwner.userEmail.value =>
          provide(
            UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userProjectOwner.userSubjectId)
          )
        case testData.userOwner.userEmail.value =>
          provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userOwner.userSubjectId))
        case testData.userWriter.userEmail.value =>
          provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userWriter.userSubjectId))
        case testData.userReader.userEmail.value =>
          provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userReader.userSubjectId))
        case "no-access" =>
          provide(
            UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212348"))
          )
        case _ =>
          provide(
            UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212349"))
          )
      }
  }

  case class TestApiService(dataSource: SlickDataSource,
                            user: String,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectivesWithUser

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  class TestWorkspaces() extends TestData {
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

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")
    val workspace2Name = WorkspaceName(billingProject.projectName.value, "emptyattrs")

    val workspace1Id = UUID.randomUUID().toString
    val workspace = makeWorkspaceWithUsers(
      billingProject,
      workspaceName.name,
      workspace1Id,
      "bucket1",
      Some(workspace1Id),
      testDate,
      testDate,
      "testUser",
      Map(AttributeName.withDefaultNS("a") -> AttributeString("x"),
          AttributeName.withDefaultNS("description") -> AttributeString("my description")
      ),
      false,
      WorkspaceVersions.V2,
      billingProject.googleProjectId,
      Option(GoogleProjectNumber(UUID.randomUUID().toString)),
      Some(RawlsBillingAccountName("billing-account")),
      None,
      Option(testDate)
    )

    val workspace2Id = UUID.randomUUID().toString
    val workspace2 = makeWorkspaceWithUsers(billingProject,
                                            workspace2Name.name,
                                            workspace2Id,
                                            "bucket2",
                                            Some(workspace2Id),
                                            testDate,
                                            testDate,
                                            "testUser",
                                            Map(),
                                            false
    )

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sample4 = Entity("sample4", "sample", Map.empty)
    val sample5 = Entity("sample5", "sample", Map.empty)
    val sample6 = Entity("sample6", "sample", Map.empty)
    val sampleSet = Entity(
      "sampleset",
      "sample_set",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          Seq(
            sample1.toReference,
            sample2.toReference,
            sample3.toReference
          )
        )
      )
    )

    val methodConfig = MethodConfiguration(
      "dsde",
      "testConfig",
      Some("Sample"),
      None,
      Map("param1" -> AttributeString("foo")),
      Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")),
      AgoraMethod(workspaceName.namespace, "method-a", 1)
    )
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, workspaceName)
    val submissionTemplate = createTestSubmission(
      workspace,
      methodConfig,
      sampleSet,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> testData.inputResolutions,
          sample2 -> testData.inputResolutions,
          sample3 -> testData.inputResolutions
      ),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> testData.inputResolutions2,
          sample5 -> testData.inputResolutions2,
          sample6 -> testData.inputResolutions2
      )
    )
    val submissionSuccess = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Done,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Succeeded))
    )
    val submissionFail = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Done,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Failed))
    )
    val submissionRunning1 = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Submitted,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Running))
    )
    val submissionRunning2 = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Submitted,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Running))
    )

    override def save() =
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        workspaceQuery.createOrUpdate(workspace),
        workspaceQuery.createOrUpdate(workspace2),
        withWorkspaceContext(workspace) { ctx =>
          DBIO.seq(
            entityQuery.save(ctx, sample1),
            entityQuery.save(ctx, sample2),
            entityQuery.save(ctx, sample3),
            entityQuery.save(ctx, sample4),
            entityQuery.save(ctx, sample5),
            entityQuery.save(ctx, sample6),
            entityQuery.save(ctx, sampleSet),
            methodConfigurationQuery.create(ctx, methodConfig),
            submissionQuery.create(ctx, submissionSuccess),
            submissionQuery.create(ctx, submissionFail),
            submissionQuery.create(ctx, submissionRunning1),
            submissionQuery.create(ctx, submissionRunning2)
          )
        }
      )
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  val testTime = currentTime()

  // canonical full WorkspaceResponse to use in expectations below
  val fullWorkspaceResponse = WorkspaceResponse(
    Option(WorkspaceAccessLevels.Owner),
    Option(true),
    Option(true),
    Option(true),
    WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = testTime),
                                             Some(Set()),
                                             true,
                                             Some(WorkspaceCloudPlatform.Gcp)
    ),
    Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)),
    Option(WorkspaceBucketOptions(false)),
    Option(Set.empty),
    None
  )

  // no includes, no excludes
  "WorkspaceApi" should "include all options when getting a workspace if no params specified" in withTestWorkspacesApiServices {
    services =>
      implicit val timeout: Duration = 10.seconds
      Get(testWorkspaces.workspace.path) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(fullWorkspaceResponse) {
            val response = responseAs[WorkspaceResponse]
            response.copy(workspace = response.workspace.copy(lastModified = testTime))
          }
        }
  }

  // START fields tests

  // canonical bare-minimum WorkspaceResponse to use in expectations below
  val minimalWorkspaceResponse = WorkspaceResponse(
    None,
    None,
    None,
    None,
    WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = testTime), None, false),
    None,
    None,
    None,
    None
  )

  "WorkspaceApi, when using fields param" should "include accessLevel when asked to" in withTestWorkspacesApiServices {
    services =>
      Get(testWorkspaces.workspace.path + "?fields=accessLevel") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
          val actual = responseAs[String].parseJson.asJsObject
          val expected = JsObject("accessLevel" -> JsString(fullWorkspaceResponse.accessLevel.get.toString))
          assertResult(expected)(actual)
        }
  }

  it should "include bucketOptions" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=bucketOptions") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("bucketOptions" -> fullWorkspaceResponse.bucketOptions.get.toJson)
        assertResult(expected)(actual)
      }
  }

  it should "include canCompute" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=canCompute") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("canCompute" -> JsBoolean(fullWorkspaceResponse.canCompute.get))
        assertResult(expected)(actual)
      }
  }

  it should "include canShare" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=canShare") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("canShare" -> JsBoolean(fullWorkspaceResponse.canShare.get))
        assertResult(expected)(actual)
      }
  }

  it should "include catalog" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=catalog") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("catalog" -> JsBoolean(fullWorkspaceResponse.catalog.get))
        assertResult(expected)(actual)
      }
  }

  it should "include owners" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=owners") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("owners" -> JsArray(fullWorkspaceResponse.owners.get.toVector.map(JsString(_))))
        assertResult(expected)(actual)
      }
  }

  it should "include workspace.attributes" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspace.attributes") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject(
          "workspace" -> JsObject(
            "attributes" ->
              JsObject("a" -> JsString("x"), "description" -> JsString("my description"))
          )
        )
        assertResult(expected)(actual)
      }
  }

  it should "include individual keys inside workspace.attributes" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspace.attributes.description") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject(
          "workspace" -> JsObject(
            "attributes" ->
              JsObject("description" -> JsString("my description"))
          )
        )
        assertResult(expected)(actual)
      }
  }

  it should "include workspace.authorizationDomain" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspace.authorizationDomain") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject(
          "workspace" -> JsObject(
            "authorizationDomain" ->
              JsArray(
                fullWorkspaceResponse.workspace.authorizationDomain.get.toVector.map(groupRef =>
                  JsObject("membersGroupName" -> JsString(groupRef.membersGroupName.value))
                )
              )
          )
        )
        assertResult(expected)(actual)
      }
  }

  it should "include workspaceSubmissionStats" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspaceSubmissionStats") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("workspaceSubmissionStats" -> fullWorkspaceResponse.workspaceSubmissionStats.get.toJson)
        assertResult(expected)(actual)
      }
  }

  // find all the members of the WorkspaceDetails case class; check that each of them is returned inside the workspace object as long as it is defined
  import scala.reflect.runtime.universe._
  val rm = runtimeMirror(getClass.getClassLoader)
  val workspaceDetailsMirror = rm.reflect(WorkspaceDetails(testWorkspaces.workspace, Set.empty))

  val workspaceDetailsMembers: List[String] = typeOf[WorkspaceDetails].members.collect {
    case m: MethodSymbol if m.isCaseAccessor && !workspaceDetailsMirror.reflectMethod(m).apply().equals(None) =>
      m.name.toString
  }.toList
  workspaceDetailsMembers.foreach { workspaceKey =>
    it should s"include $workspaceKey subkey of workspace when specifying the top-level key" in withTestWorkspacesApiServices {
      services =>
        Get(testWorkspaces.workspace.path + "?fields=workspace") ~>
          sealRoute(services.workspaceRoutes) ~>
          check {
            assertResult(StatusCodes.OK)(status)
            val actual = responseAs[String].parseJson.asJsObject
            val workspaceFields = actual.fields.getOrElse("workspace", new JsObject(Map.empty)).asJsObject.fields
            assert(workspaceFields.contains(workspaceKey))
          }
    }
  }

  it should "include multiple keys simultaneously when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=canShare,workspace.attributes,accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject(
          "accessLevel" -> JsString(fullWorkspaceResponse.accessLevel.get.toString),
          "canShare" -> JsBoolean(fullWorkspaceResponse.canShare.get),
          "workspace" -> JsObject(
            "attributes" -> JsObject("a" -> JsString("x"), "description" -> JsString("my description"))
          )
        )
        assertResult(expected)(actual)
      }
  }

  // this test targets a specific bug that arose during development; worth keeping in.
  it should "include workspace.attributes even when attributes are empty" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace2.path + "?fields=workspace.attributes") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("workspace" -> JsObject("attributes" -> JsObject()))
        assertResult(expected)(actual)
      }
  }

  it should "handle duplicate values just fine" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=accessLevel,accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject("accessLevel" -> JsString(fullWorkspaceResponse.accessLevel.get.toString))
        assertResult(expected)(actual)
      }
  }

  it should "not return any attributes that aren't in the db" in withTestWorkspacesApiServices { services =>
    Get(
      testWorkspaces.workspace.path + "?fields=workspace.attributes.description,workspace.attributes.tag:tags,workspace.attributes.library:orsp"
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson.asJsObject
        val expected = JsObject(
          "workspace" -> JsObject(
            "attributes" -> JsObject("description" -> JsString("my description"))
          )
        )
        assertResult(expected)(actual)
      }
  }

  // START query param behavior tests

  it should s"return 400 Bad Request for unknown fields value in querystring" in withTestWorkspacesApiServices {
    services =>
      Get(testWorkspaces.workspace.path + "?fields=accessLevel,IntentionallyBadValueForUnitTest,AnotherBadOne") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
          val parsedResponse = responseAs[ErrorReport]
          assertResult("Unrecognized field names: AnotherBadOne, IntentionallyBadValueForUnitTest") {
            parsedResponse.message
          }
        }
  }

  it should s"return 400 Bad Request if fields param is specified multiple times" in withTestWorkspacesApiServices {
    services =>
      Get(testWorkspaces.workspace.path + "?fields=accessLevel&fields=canShare,workspace.attributes") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
          val parsedResponse = responseAs[ErrorReport]
          assertResult("Parameter 'fields' may not be present multiple times.") {
            parsedResponse.message
          }
        }
  }

  // get-workspace-by-id delegates to get-workspace-by-name, so we test its option handling briefly, enough
  // to know we are passing the WorkspaceFieldSpecs along.
  it should "include multiple keys simultaneously when getting a workspace by id" in withTestWorkspacesApiServices {
    services =>
      Get(
        s"/workspaces/id/${testWorkspaces.workspace.workspaceId}" + "?fields=canShare,workspace.attributes,accessLevel"
      ) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
          val actual = responseAs[String].parseJson.asJsObject
          val expected = JsObject(
            "accessLevel" -> JsString(fullWorkspaceResponse.accessLevel.get.toString),
            "canShare" -> JsBoolean(fullWorkspaceResponse.canShare.get),
            "workspace" -> JsObject(
              "attributes" -> JsObject("a" -> JsString("x"), "description" -> JsString("my description"))
            )
          )
          assertResult(expected)(actual)
        }
  }

}
