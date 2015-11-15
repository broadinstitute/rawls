package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, GraphEntityDAO, GraphMethodConfigurationDAO, GraphWorkspaceDAO, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{SubmissionFormat, SubmissionReportFormat, SubmissionRequestFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class SubmissionApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll() = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll() = {
    super.afterAll
    mockServer.stopServer
  }

  case class TestApiService(dataSource: DataSource)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with MockUserInfoDirectives {
    def actorRefFactory = system

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource,
      gcsDAO
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    gcsDAO.storeToken(userInfo, "test_token")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), gcsDAO, submissionSupervisor)_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withApiServices(dataSource: DataSource)(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  "SubmissionApi" should "return 404 Not Found when creating a submission using a MethodConfiguration that doesn't exist in the workspace" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`, SubmissionRequest("dsde","not there","Pattern","pattern1", None).toJson.toString)) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when creating a submission using an Entity that doesn't exist in the workspace" in withTestDataApiServices { services =>
    val mcName = MethodConfigurationName("three_step","dsde", testData.wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern", Map.empty, Map("three_step.cgrep.pattern"->AttributeString("String")), Map.empty, MethodRepoMethod("dsde","three_step",1))
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, methodConf.toJson.toString)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`, SubmissionRequest(mcName.namespace, mcName.name,"Pattern","pattern1", None).toJson.toString)) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  private def createAndMonitorSubmission(wsName: WorkspaceName, methodConf: MethodConfiguration,
                                         submissionEntity: Entity, submissionExpression: Option[String],
                                          services: TestApiService): Submission = {
      Post(s"/workspaces/${wsName.namespace}/${wsName.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, methodConf.toJson.toString)) ~>
          sealRoute(services.methodConfigRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      val submissionRq = SubmissionRequest(methodConf.namespace, methodConf.name, submissionEntity.entityType, submissionEntity.name, submissionExpression)
      Post(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions", HttpEntity(ContentTypes.`application/json`, submissionRq.toJson.toString)) ~>
          sealRoute(services.submissionRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          val submission = responseAs[SubmissionReport]
          Get(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions/${submission.submissionId}") ~>
                  sealRoute(services.submissionRoutes) ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              return responseAs[Submission]
            }
        }

    fail("Unable to create and monitor submissions")
  }

  it should "return 201 Created when creating and monitoring a submission with no expression" in withTestDataApiServices { services =>
  val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services)

    assertResult(1) {
      submission.workflows.size
    }
  }
  it should "return 201 Created when creating and monitoring a submission with valid expression" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sset1, Option("this.samples"), services)

    assertResult(3) {
      submission.workflows.size
    }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map("foo" -> AttributeString("x"), "bar" -> AttributeNumber(3), "splat" -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace + "2", testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    Map.empty
  )

  it should "return 200 on getting a submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${testData.submission1.submissionId}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(testData.submission1) {responseAs[Submission]}
      }
  }

  it should "return 404 on getting a nonexistent submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/unrealSubmission42") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
  }

  it should "return 200 when listing submissions" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(List(testData.submissionTerminateTest, testData.submission1, testData.submission2, testData.submissionUpdateEntity, testData.submissionUpdateWorkspace)) {
          responseAs[List[Submission]]
        }
      }
  }
}
