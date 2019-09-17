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

import scala.concurrent.ExecutionContext

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

  class TestWorkspaces() extends TestData {
    val userProjectOwner = RawlsUser(UserInfo(RawlsUserEmail("project-owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543210101")))
    val userOwner = RawlsUser(UserInfo(testData.userOwner.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345")))
    val userWriter = RawlsUser(UserInfo(testData.userWriter.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userReader = RawlsUser(UserInfo(testData.userReader.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), "testBucketUrl", CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")
    val workspace2Name = WorkspaceName(billingProject.projectName.value, "emptyattrs")

    val workspace1Id = UUID.randomUUID().toString
    val workspace = makeWorkspaceWithUsers(billingProject, workspaceName.name, workspace1Id, "bucket1", Some(workspace1Id), testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("a") -> AttributeString("x")), false)

    val workspace2Id = UUID.randomUUID().toString
    val workspace2 = makeWorkspaceWithUsers(billingProject, workspace2Name.name, workspace2Id, "bucket2", Some(workspace2Id), testDate, testDate, "testUser", Map(), false)


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
      )
    }
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  val testTime = currentTime()

  // canonical full WorkspaceResponse to use in expectations below
  val fullWorkspaceResponse = WorkspaceResponse(Option(WorkspaceAccessLevels.Owner), Option(true), Option(true), Option(true), WorkspaceDetails(testWorkspaces.workspace.copy(lastModified = testTime), Set.empty), Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)), Option(WorkspaceBucketOptions(false)), Option(Set.empty))

  // no includes, no excludes
  "WorkspaceApi" should "include all options when getting a workspace if no params specified" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(fullWorkspaceResponse){
          val response = responseAs[WorkspaceResponse]
          response.copy(workspace = response.workspace.copy(lastModified = testTime))
        }
      }
  }


  // START fields tests

  // canonical bare-minimum WorkspaceResponse to use in expectations below
  val minimalWorkspaceResponse = WorkspaceResponse(None, None, None, None, WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = testTime), None, false), None, None, None)

  "WorkspaceApi, when using includeKey params" should "include accessLevel when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(accessLevel = fullWorkspaceResponse.accessLevel)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.accessLevel.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include bucketOptions appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=bucketOptions") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(bucketOptions = fullWorkspaceResponse.bucketOptions)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.bucketOptions.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include canCompute appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=canCompute") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(canCompute = fullWorkspaceResponse.canCompute)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.canCompute.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include canShare appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=canShare") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(canShare = fullWorkspaceResponse.canShare)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.canShare.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include catalog appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=catalog") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(catalog = fullWorkspaceResponse.catalog)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.catalog.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include owners appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=owners") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(owners = fullWorkspaceResponse.owners)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.owners.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include workspace.attributes appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspace.attributes") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(workspace = minimalWorkspaceResponse.workspace.copy(attributes = fullWorkspaceResponse.workspace.attributes))
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.workspace.attributes.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include workspace.authorizationDomain appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspace.authorizationDomain") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(workspace = minimalWorkspaceResponse.workspace.copy(authorizationDomain = fullWorkspaceResponse.workspace.authorizationDomain))
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.workspace.authorizationDomain.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include workspaceSubmissionStats appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=workspaceSubmissionStats") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(workspaceSubmissionStats = fullWorkspaceResponse.workspaceSubmissionStats)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.workspaceSubmissionStats.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "include multiple keys simultaneously when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=canShare,workspace.attributes,accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(
          accessLevel = fullWorkspaceResponse.accessLevel,
          canShare = fullWorkspaceResponse.canShare,
          workspace = minimalWorkspaceResponse.workspace.copy(attributes = fullWorkspaceResponse.workspace.attributes))
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertions
        assert(actual.accessLevel.isDefined)
        assert(actual.canShare.isDefined)
        assert(actual.workspace.attributes.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  // this test targets a specific bug that arose during development; worth keeping in.
  it should "include workspace.attributes even when attributes are empty" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace2.path + "?fields=workspace.attributes") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertions
        assert(actual.workspace.attributes.isDefined, "attributes key should be present in response")
        actual.workspace.attributes.foreach( attrs => assert(attrs.isEmpty, "attributes value should be an empty map"))
      }
  }

  it should "handle duplicates just fine" in withTestWorkspacesApiServices { services =>
    // the canShare and catalog options here should have no effect
    Get(testWorkspaces.workspace.path + "?fields=accessLevel,accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = minimalWorkspaceResponse.copy(accessLevel = fullWorkspaceResponse.accessLevel)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assert(actual.accessLevel.isDefined)
        // compare full results
        assertResult(expected) { actual }
      }
  }

  // START query param behavior tests

  it should s"return 400 Bad Request for unknown fields value in querystring" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?fields=IntentionallyBadValueForUnitTest,AnotherBadOne") ~>
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



}

