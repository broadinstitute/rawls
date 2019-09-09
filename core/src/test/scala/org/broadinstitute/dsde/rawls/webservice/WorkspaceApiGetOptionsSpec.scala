package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{OAuth2BearerToken, _}
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, TestData}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by davidan on 9/9/19.
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

//  val userWriterNoCompute = RawlsUserEmail("writer-access-no-compute")
//  val userWriterNoComputeOnProject = RawlsUserEmail("writer-access-no-compute-on-project")
//
//  def withApiServicesSecure[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {
//    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO) {
//      override val samDAO: MockSamDAO = new MockSamDAO(dataSource) {
//        override def userHasAction(resourceTypeName: SamResourceTypeName, resourceId: String, action: SamResourceAction, userInfo: UserInfo): Future[Boolean] = {
//          val result = userInfo.userEmail match {
//            case testData.userOwner.userEmail => true
//            case testData.userProjectOwner.userEmail => true
//            case testData.userWriter.userEmail => Set(SamWorkspaceActions.read, SamWorkspaceActions.write, SamWorkspaceActions.compute, SamBillingProjectActions.launchBatchCompute).contains(action)
//            case `userWriterNoCompute` => Set(SamWorkspaceActions.read, SamWorkspaceActions.write).contains(action)
//            case `userWriterNoComputeOnProject` => Set(SamWorkspaceActions.read, SamWorkspaceActions.write, SamWorkspaceActions.compute).contains(action)
//            case testData.userReader.userEmail => Set(SamWorkspaceActions.read).contains(action)
//            case _ => false
//          }
//          Future.successful(result)
//        }
//      }
//    }
//    try {
//      testCode(apiService)
//    } finally {
//      apiService.cleanupSupervisor
//    }
//  }
//
//  def withApiServicesMockitoSam[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {
//    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO) {
//      override val samDAO: SamDAO = mock[SamDAO]
//    }
//    try {
//      testCode(apiService)
//    } finally {
//      apiService.cleanupSupervisor
//    }
//  }
//
//  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
//    withDefaultTestDatabase { dataSource: SlickDataSource =>
//      withApiServices(dataSource)(testCode)
//    }
//  }
//
//  def withTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T = {
//    withDefaultTestDatabase { dataSource: SlickDataSource =>
//      withApiServicesSecure(dataSource, user) { services =>
//        testCode(services)
//      }
//    }
//  }
//
//  def withTestDataApiServicesMockitoSam[T](testCode: TestApiService => T): T = {
//    withDefaultTestDatabase { dataSource: SlickDataSource =>
//      withApiServicesMockitoSam(dataSource) { services =>
//        testCode(services)
//      }
//    }
//  }
//
//  def withEmptyWorkspaceApiServices[T](user: String)(testCode: TestApiService => T): T = {
//    withCustomTestDatabase(new EmptyWorkspace) { dataSource: SlickDataSource =>
//      withApiServices(dataSource, user)(testCode)
//    }
//  }
//
//  def withLockedWorkspaceApiServices[T](user: String)(testCode: TestApiService => T): T = {
//    withCustomTestDatabase(new LockedWorkspace) { dataSource: SlickDataSource =>
//      withApiServicesSecure(dataSource, user)(testCode)
//    }
//  }

  class TestWorkspaces() extends TestData {
    val userProjectOwner = RawlsUser(UserInfo(RawlsUserEmail("project-owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543210101")))
    val userOwner = RawlsUser(UserInfo(testData.userOwner.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345")))
    val userWriter = RawlsUser(UserInfo(testData.userWriter.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userReader = RawlsUser(UserInfo(testData.userReader.userEmail, OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), "testBucketUrl", CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")

    val workspace1Id = UUID.randomUUID().toString
    val workspace = makeWorkspaceWithUsers(billingProject, workspaceName.name, workspace1Id, "bucket1", Some(workspace1Id), testDate, testDate, "testUser", Map(AttributeName.withDefaultNS("a") -> AttributeString("x")), false)

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


  // TODO: unit tests directly against WorkspaceService.parseParams ??




  //
  val testTime = currentTime()

  // canonical full WorkspaceResponse to use in expectations below
  val fullWorkspaceResponse = WorkspaceResponse(Option(WorkspaceAccessLevels.Owner), Option(true), Option(true), Option(true), WorkspaceDetails(testWorkspaces.workspace.copy(lastModified = testTime), Set.empty), Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)), Option(WorkspaceBucketOptions(false)), Option(Set.empty))

  // no includes, no excluces
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

  // options:  accessLevel, bucketOptions, canCompute, canShare, catalog, owners, workspace.attributes, workspace.authorizationDomain, workspaceSubmissionStats


  // START excludeKey tests

  "WorkspaceApi, when using excludeKey params" should "exclude accessLevel when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=accessLevel") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(accessLevel = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.accessLevel }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude bucketOptions appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=bucketOptions") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(bucketOptions = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.bucketOptions }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude canCompute appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=canCompute") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(canCompute = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.canCompute }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude canShare appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=canShare") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(canShare = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.canShare }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude catalog appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=catalog") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(catalog = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.catalog }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude owners appropriately when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=owners") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(owners = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.owners }
        assert(actual.bucketOptions.isDefined, "why is bucket  options empty?????")
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude workspace.attributes when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=workspace.attributes") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(workspace = fullWorkspaceResponse.workspace.copy(attributes = None))
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.workspace.attributes }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude workspace.authorizationDomain when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=workspace.authorizationDomain") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(workspace = fullWorkspaceResponse.workspace.copy(authorizationDomain = None))
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.workspace.authorizationDomain }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude workspaceSubmissionStats when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=workspaceSubmissionStats") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(workspaceSubmissionStats = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertion
        assertResult(None) { actual.workspaceSubmissionStats }
        // compare full results
        assertResult(expected) { actual }
      }
  }

  it should "exclude multiple keys simultaneously when asked to" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path + "?excludeKey=bucketOptions&excludeKey=owners&excludeKey=workspaceSubmissionStats") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = fullWorkspaceResponse.copy(bucketOptions = None, owners = None, workspaceSubmissionStats = None)
        val parsedResponse = responseAs[WorkspaceResponse]
        val actual = parsedResponse.copy(workspace = parsedResponse.workspace.copy(lastModified = testTime))
        // targeted assertions
        assertResult(None) { actual.bucketOptions }
        assertResult(None) { actual.owners }
        assertResult(None) { actual.workspaceSubmissionStats }
        // compare full results
        assertResult(expected) { actual }
      }
  }







}

