package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestData
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext


class WorkspaceApiListOptionsSpec extends ApiServiceSpec {
  import driver.api._

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(): Directive1[UserInfo] = {
      // just return the cookie text as the common name
      user match {
        case testData.userProjectOwner.userEmail.value => provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userProjectOwner.userSubjectId))
        case testData.userOwner.userEmail.value => provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userOwner.userSubjectId))
        case testData.userWriter.userEmail.value => provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userWriter.userSubjectId))
        case testData.userReader.userEmail.value => provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userReader.userSubjectId))
        case "no-access" => provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212348")))
        case _ => provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212349")))
      }
    }
  }

  case class TestApiService(dataSource: SlickDataSource, user: String, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  class PaginatedListTestWorkspaces() extends TestData {
    val userOwner = RawlsUser(UserInfo(testData.userOwner.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345")))

    val workspaceName1 = WorkspaceName("a-billing-project", "g-ws-name")
    val workspaceName2 = WorkspaceName("b-billing-project", "f-ws-name")
    val workspaceName3 = WorkspaceName("c-billing-project", "e-ws-name")
    val workspaceName4 = WorkspaceName("d-billing-project", "d-ws-name")
    val workspaceName5 = WorkspaceName("e-billing-project", "c-ws-name")
    val workspaceName6 = WorkspaceName("f-billing-project", "b-ws-name")
    val workspaceName7 = WorkspaceName("g-billing-project", "a-ws-name")

    val attributesForWorkspace1 = Map(AttributeName.withTagsNS() -> AttributeString("tag1"), AttributeName.withTagsNS() -> AttributeString("tag2"), AttributeName.withDefaultNS("att1") -> AttributeString("val1"))
    val attributesForWorkspace2 = Map(AttributeName.withTagsNS() -> AttributeString("tag1"), AttributeName.withTagsNS() -> AttributeString("tag2"), AttributeName.withDefaultNS("att1") -> AttributeString("val1"))
    val attributesForWorkspace3 = Map(AttributeName.withTagsNS() -> AttributeString("tag1"), AttributeName.withTagsNS() -> AttributeString("tag2"), AttributeName.withDefaultNS("att1") -> AttributeString("val1"))
    val attributesForWorkspace4 = Map(AttributeName.withTagsNS() -> AttributeString("tag1"), AttributeName.withDefaultNS("att1") -> AttributeString("val1"))
    val attributesForWorkspace5 = Map(AttributeName.withTagsNS() -> AttributeString("tag1"), AttributeName.withDefaultNS("att1") -> AttributeString("val1"))
    val attributesForWorkspace6 = Map(AttributeName.withDefaultNS("att1") -> AttributeString("val1"))
    val attributesForWorkspace7 = Map(AttributeName.withDefaultNS("att1") -> AttributeString("val1"))

    val billingProject1 = RawlsBillingProject(RawlsBillingProjectName(workspaceName1.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val billingProject2 = RawlsBillingProject(RawlsBillingProjectName(workspaceName2.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val billingProject3 = RawlsBillingProject(RawlsBillingProjectName(workspaceName3.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val billingProject4 = RawlsBillingProject(RawlsBillingProjectName(workspaceName4.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val billingProject5 = RawlsBillingProject(RawlsBillingProjectName(workspaceName5.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val billingProject6 = RawlsBillingProject(RawlsBillingProjectName(workspaceName6.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)
    val billingProject7 = RawlsBillingProject(RawlsBillingProjectName(workspaceName7.namespace), "testBucketUrl", CreationStatuses.Ready, None, None)

    val workspace1 =  makeWorkspaceWithUsers(billingProject1, workspaceName1.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "e-testUser", attributesForWorkspace1, false)
    val workspace2 =  makeWorkspaceWithUsers(billingProject2, workspaceName2.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "c-testUser", attributesForWorkspace2, false)
    val workspace3 =  makeWorkspaceWithUsers(billingProject3, workspaceName3.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "b-testUser", attributesForWorkspace3, false)
    val workspace4 =  makeWorkspaceWithUsers(billingProject4, workspaceName4.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "a-testUser", attributesForWorkspace4, false)
    val workspace5 =  makeWorkspaceWithUsers(billingProject5, workspaceName5.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "f-testUser", attributesForWorkspace5, false)
    val workspace6 =  makeWorkspaceWithUsers(billingProject6, workspaceName6.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "d-testUser", attributesForWorkspace6, false)
    val workspace7 =  makeWorkspaceWithUsers(billingProject7, workspaceName7.name, UUID.randomUUID().toString, "bucketName", Some("workflow-collection"), currentTime(), currentTime(), "g-testUser", attributesForWorkspace7, false)
    val workspaceList = List(workspace1, workspace2, workspace3, workspace4, workspace5, workspace6, workspace7)

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sample4 = Entity("sample4", "sample", Map.empty)
    val sample5 = Entity("sample5", "sample", Map.empty)
    val sample6 = Entity("sample6", "sample", Map.empty)
    val sampleSet = Entity("sampleset", "sample_set", Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sample1.toReference,
      sample2.toReference,
      sample3.toReference
    ))))

    val methodConfig = MethodConfiguration("dsde", "testConfig", Some("Sample"), None, Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), AgoraMethod(workspaceName1.namespace, "method-a", 1))
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, workspaceName1)
    val submissionTemplate = createTestSubmission(workspace1, methodConfig, sampleSet, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> testData.inputResolutions, sample2 -> testData.inputResolutions, sample3 -> testData.inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> testData.inputResolutions2, sample5 -> testData.inputResolutions2, sample6 -> testData.inputResolutions2))

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

    override def save() = {
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject1),
        rawlsBillingProjectQuery.create(billingProject2),
        rawlsBillingProjectQuery.create(billingProject3),
        rawlsBillingProjectQuery.create(billingProject4),
        rawlsBillingProjectQuery.create(billingProject5),
        rawlsBillingProjectQuery.create(billingProject6),
        rawlsBillingProjectQuery.create(billingProject7),

        workspaceQuery.save(workspace1),
        workspaceQuery.save(workspace2),
        workspaceQuery.save(workspace3),
        workspaceQuery.save(workspace4),
        workspaceQuery.save(workspace5),
        workspaceQuery.save(workspace6),
        workspaceQuery.save(workspace7),


        withWorkspaceContext(workspace1) { ctx =>
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
      ).withPinnedSession
    }
  }

  class TestWorkspaces() extends TestData {
    val userOwner = RawlsUser(UserInfo(testData.userOwner.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345")))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), "testBucketUrl", CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")

    val workspace2Name = WorkspaceName(billingProject.projectName.value, "testworkspace2")

    val workspace1Id = UUID.randomUUID().toString
    val workspace = makeWorkspaceWithUsers(billingProject, workspaceName.name, workspace1Id, "bucket1", Some(workspace1Id), testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("a") -> AttributeString("x"), AttributeName.withDefaultNS("description") -> AttributeString("workspace one")), false)

    val workspace2Id = UUID.randomUUID().toString
    val workspace2 = makeWorkspaceWithUsers(billingProject, workspace2Name.name, workspace2Id, "bucket2", Some(workspace2Id), testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("b") -> AttributeString("y"), AttributeName.withDefaultNS("description") -> AttributeString("workspace two")), false)

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sample4 = Entity("sample4", "sample", Map.empty)
    val sample5 = Entity("sample5", "sample", Map.empty)
    val sample6 = Entity("sample6", "sample", Map.empty)
    val sampleSet = Entity("sampleset", "sample_set", Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(Seq(
      sample1.toReference,
      sample2.toReference,
      sample3.toReference
    ))))

    val methodConfig = MethodConfiguration("dsde", "testConfig", Some("Sample"), None, Map("param1"-> AttributeString("foo")), Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")), AgoraMethod(workspaceName.namespace, "method-a", 1))
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, workspaceName)
    val submissionTemplate = createTestSubmission(workspace, methodConfig, sampleSet, WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3), Map(sample1 -> testData.inputResolutions, sample2 -> testData.inputResolutions, sample3 -> testData.inputResolutions),
      Seq(sample4, sample5, sample6), Map(sample4 -> testData.inputResolutions2, sample5 -> testData.inputResolutions2, sample6 -> testData.inputResolutions2))
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

    override def save() = {
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),

        workspaceQuery.save(workspace),
        workspaceQuery.save(workspace2),

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
      ).withPinnedSession
    }
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withTestWorkspacesApiServicesEmptyDatabase[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(emptyData) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }


  "WorkspaceApi list-workspaces with fields param" should "return full response if no fields param" in withTestWorkspacesApiServices { services =>
    Get("/workspaces") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dateTime = currentTime()
        assertResult(Set(
          WorkspaceListResponse(WorkspaceAccessLevels.Owner, WorkspaceDetails(testWorkspaces.workspace.copy(lastModified = dateTime), Set.empty), Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)), false),
          WorkspaceListResponse(WorkspaceAccessLevels.Owner, WorkspaceDetails(testWorkspaces.workspace2.copy(lastModified = dateTime), Set.empty), Option(WorkspaceSubmissionStats(None, None, 0)), false)
        )) {
          responseAs[Array[WorkspaceListResponse]].toSet[WorkspaceListResponse].map(wslr => wslr.copy(workspace = wslr.workspace.copy(lastModified = dateTime)))
        }
      }
  }

  it should "return full response if querystring exists but no fields param" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?thisisnotfields=noitsnot") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dateTime = currentTime()
        assertResult(Set(
          WorkspaceListResponse(WorkspaceAccessLevels.Owner, WorkspaceDetails(testWorkspaces.workspace.copy(lastModified = dateTime), Set.empty), Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)), false),
          WorkspaceListResponse(WorkspaceAccessLevels.Owner, WorkspaceDetails(testWorkspaces.workspace2.copy(lastModified = dateTime), Set.empty), Option(WorkspaceSubmissionStats(None, None, 0)), false)
        )) {
          responseAs[Array[WorkspaceListResponse]].toSet[WorkspaceListResponse].map(wslr => wslr.copy(workspace = wslr.workspace.copy(lastModified = dateTime)))
        }
      }
  }

  it should "filter response to a single key" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject("accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString)),
          JsObject("accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString))
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to multiple keys" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=accessLevel,public") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString),
            "public" -> JsBoolean(false)
          ),
          JsObject(
            "accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString),
            "public" -> JsBoolean(false)
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to nested keys" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=workspaceSubmissionStats,workspace.workspaceId,workspace.bucketName") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "workspace" -> JsObject(
              "workspaceId" -> JsString(testWorkspaces.workspace1Id.toString),
              "bucketName" -> JsString("bucket1")
            ),
            "workspaceSubmissionStats" -> JsObject(
              "lastSuccessDate" -> JsString(testDate.toString),
              "lastFailureDate" ->  JsString(testDate.toString),
              "runningSubmissionsCount" -> JsNumber(2)
            )
          ),
          JsObject(
            "workspace" -> JsObject(
              "workspaceId" -> JsString(testWorkspaces.workspace2Id.toString),
              "bucketName" -> JsString("bucket2")
            ),
            "workspaceSubmissionStats" -> JsObject(
              "runningSubmissionsCount" -> JsNumber(0)
            )
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to entire subtrees" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=public,workspace.attributes") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "a" -> JsString("x"),
                "description" -> JsString("workspace one")
              )
            )
          ),
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "b" -> JsString("y"),
                "description" -> JsString("workspace two")
              )
            )
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to individual attributes" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=public,workspace.attributes.description") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "description" -> JsString("workspace one")
              )
            )
          ),
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "description" -> JsString("workspace two")
              )
            )
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "throw error with unrecognized field value" in withTestWorkspacesApiServices { services =>
    // NB: "canShare" is valid for get-workspace but not list-workspaces.
    Get("/workspaces?fields=accessLevel,somethingNotRecognized,canShare") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
        val actual = responseAs[ErrorReport]

        // NB: field names in error response are alphabetized for deterministic behavior
        assertResult("Unrecognized field names: canShare, somethingNotRecognized") { actual.message }
      }
  }

  it should "throw error if field param specified multiple times" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=accessLevel&fields=public") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) { status }
        val actual = responseAs[ErrorReport]

        assertResult("Parameter 'fields' may not be present multiple times.") { actual.message }
      }
  }


  ///////////

  // default values, return workspaces
  "WorkspaceApi paginated list-workspaces" should "return default response for no specified fields, filters or sort order" in withTestWorkspacesApiServices { services =>
    Get("/workspaces/workspaceQuery") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val result = responseAs[WorkspaceQueryResponse]

        val dateTime = currentTime()
        assertResult(2) {
          result.results.length
        }

      }
  }

  // specify default values, return workspaces
  "WorkspaceApi paginated list-workspaces" should "return workspaces for specified fields, and no filters or sort order" in withTestWorkspacesApiServices { services =>
    Get("/workspaces/workspaceQuery?page=1&pageSize=10&sortField=name&sortDirection=asc") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val result = responseAs[WorkspaceQueryResponse]
        println(result)

      }
  }

  "WorkspaceApi paginated list-workspaces" should "return no workspaces when the user has no access to any workspaces" in withTestWorkspacesApiServicesEmptyDatabase { services =>
    Get("/workspaces/workspaceQuery") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(WorkspaceQueryResponse(WorkspaceQuery(1,10,"name",SortDirections.Ascending,None,None,None,None,None,None),WorkspaceQueryResultMetadata(0,0,0),List())) {
          responseAs[WorkspaceQueryResponse]
        }
      }
  }


  "WorkspaceApi paginated list-workspaces" should "return workspaces for with a search term on namespace and name" in withTestWorkspacesApiServices { services =>
    Get("/workspaces/workspaceQuery") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val result = responseAs[WorkspaceQueryResponse]

        val dateTime = currentTime()
        assertResult(2) {
          result.results.length
        }

      }
  }
//
//  "WorkspaceApi paginated list-workspaces" should "return workspaces for with a filter on billing project" in withTestWorkspacesApiServices { services =>
//    Get("/workspaces/workspaceQuery") ~>
//      sealRoute(services.workspaceRoutes) ~>
//      check {
//        assertResult(StatusCodes.OK) {
//          status
//        }
//        val result = responseAs[WorkspaceQueryResponse]
//
//        val dateTime = currentTime()
//        assertResult(2) {
//          result.results.length
//        }
//
//      }
//  }
//
//  "WorkspaceApi paginated list-workspaces" should "return workspaces for with a filter on submisstionStatus" in withTestWorkspacesApiServices { services =>
//    Get("/workspaces/workspaceQuery") ~>
//      sealRoute(services.workspaceRoutes) ~>
//      check {
//        assertResult(StatusCodes.OK) {
//          status
//        }
//        val result = responseAs[WorkspaceQueryResponse]
//
//        val dateTime = currentTime()
//        assertResult(2) {
//          result.results.length
//        }
//
//      }
//  }
//
//  "WorkspaceApi paginated list-workspaces" should "return workspaces for with a filter on accessLevel" in withTestWorkspacesApiServices { services =>
//    Get("/workspaces/workspaceQuery") ~>
//      sealRoute(services.workspaceRoutes) ~>
//      check {
//        assertResult(StatusCodes.OK) {
//          status
//        }
//        val result = responseAs[WorkspaceQueryResponse]
//
//        val dateTime = currentTime()
//        assertResult(2) {
//          result.results.length
//        }
//
//      }
//  }
//
//  "WorkspaceApi paginated list-workspaces" should "return workspaces for with a filter on workspaceName" in withTestWorkspacesApiServices { services =>
//    Get("/workspaces/workspaceQuery") ~>
//      sealRoute(services.workspaceRoutes) ~>
//      check {
//        assertResult(StatusCodes.OK) {
//          status
//        }
//        val result = responseAs[WorkspaceQueryResponse]
//
//        val dateTime = currentTime()
//        assertResult(2) {
//          result.results.length
//        }
//
//      }
//  }

  //more tests on sorting, direction

  // more tests on combo of filter search and sorting and specified fields

  // tests on page and pageSize

  // tests on exceptions


  ///////////

  /** Workspaces are returned in database row order. This order is deterministic for repeated runs against
    * the same static data in the DB ... but repeated runs of this spec each re-insert data to the DB,
    * which result in different row orders. We cannot predict row order ahead of time - we'd need to add a
    * SORT BY clause to the SELECT query, which would change the order at runtime. So, in this spec
    * we'll compare arrays out of order.
    *
    * WorkspaceApiSpec and elsewhere handle this by calling .toSet on results; that's another option.
    * "should contain theSameElementsAs" gives us slightly better error messaging.
    *
    * @param expected
    * @param actual
    */
  def orderInsensitiveCompare(expected: JsArray, actual: Any): Unit = {
    actual match {
      case jsa: JsArray =>
        jsa.elements should contain theSameElementsAs(expected.elements)
      case x =>
        fail(s"actual value is of type ${x.getClass.getName}; expected JsArray.")
    }

  }


}

