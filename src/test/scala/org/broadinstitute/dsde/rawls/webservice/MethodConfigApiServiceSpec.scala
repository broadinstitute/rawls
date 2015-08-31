package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, GraphEntityDAO, GraphMethodConfigurationDAO, GraphWorkspaceDAO, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockOpenAmDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class MethodConfigApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  // these tokens won't work for login to remote services: that requires a password and is therefore limited to the integration test
  def addOpenAmCookie(token: String) = addHeader(Cookie(HttpCookie("iPlanetDirectoryPro", token)))
  def addMockOpenAmCookie = addOpenAmCookie("test_token")

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

  case class TestApiService(dataSource: DataSource) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with GoogleAuthApiService with MockOpenAmDirectives {
    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor)_

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

  "MethodConfigApi" should "return 201 on create method configuration" in withTestDataApiServices { services =>
    val newMethodConfig = MethodConfiguration("dsde", "testConfigNew", "samples", Map("ready" -> AttributeString("true")), Map("param1" -> AttributeString("foo")), Map("out" -> AttributeString("bar")),
      MethodRepoConfiguration(testData.wsName.namespace+"_config", "method-a", "1"), MethodRepoMethod(testData.wsName.namespace, "method-a", "1"))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, newMethodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(newMethodConfig) {
              methodConfigDAO.get(workspaceContext, newMethodConfig.namespace, newMethodConfig.name, txn).get
            }
          }
        }
        assertResult(Some(HttpHeaders.Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newMethodConfig.path(testData.wsName)))))) {
          header("Location")
        }
      }
  }

  it should "return 409 on method configuration rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, "testConfig2_changed", txn).isDefined
            }
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn)
            }
          }
        }
      }
  }

  it should "return 404 on method configuration rename, method configuration does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/foox/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).isDefined
            }
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, "foox", txn)
            }
          }
        }
      }
  }

  it should "return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn)
            }
          }
        }
      }
  }

  it should "return 404 method configuration delete, method configuration does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}x") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(true) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).isDefined
            }
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, "foox", txn)
            }
          }
        }
      }
  }

  it should "return 200 on update method configuration" in withTestDataApiServices { services =>
    val modifiedMethodConfig = testData.methodConfig.copy(inputs = testData.methodConfig.inputs + ("param2" -> AttributeString("foo2")))
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}", HttpEntity(ContentTypes.`application/json`, modifiedMethodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(modifiedMethodConfig) {
          responseAs[MethodConfiguration]
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult(Option(AttributeString("foo2"))) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.inputs.get("param2")
            }
          }
        }
      }
  }

  it should "return 404 on update method configuration" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/update}", HttpEntity(ContentTypes.`application/json`, testData.methodConfig.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairCreated.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult("testConfig1") {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
            }
          }
        }
      }
  }

  it should "return 409 on copy method configuration to existing name" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairConflict.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairNotFound.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  val copyFromMethodRepo = "/methodconfigs/copyFromMethodRepo"

  it should "return 201 on copy method configuration from method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoGood.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            assertResult("testConfig1") {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
            }
          }
        }
      }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in withTestDataApiServices { services =>
    val existingMethodConfigCopy = MethodRepoConfigurationQuery("workspace_test", "rawls_test_good", "1", testData.methodConfigName)
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, existingMethodConfigCopy.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoMissing.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoEmptyPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoBadPayload.toJson.toString())) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 200 when generating a method config template from a valid method" in withTestDataApiServices { services =>
    val method = MethodRepoMethod("dsde","three_step","1")
    Post("/methodconfigs/template",HttpEntity(ContentTypes.`application/json`,method.toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        val methodConfiguration = MethodConfiguration("namespace","name","rootEntityType",Map(), Map("three_step.cgrep.pattern" -> AttributeString("expression")),
          Map("three_step.ps.procs"->AttributeString("expression"),"three_step.cgrep.count"->AttributeString("expression"), "three_step.wc.count"->AttributeString("expression")),
          MethodRepoConfiguration("none","none","none"),MethodRepoMethod("dsde","three_step","1"))
        assertResult(methodConfiguration) { responseAs[MethodConfiguration] }
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when generating a method config template from a bogus method" in withTestDataApiServices { services =>
    Post("/methodconfigs/template",HttpEntity(ContentTypes.`application/json`,MethodRepoMethod("dsde","three_step","2").toJson.toString)) ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 on get method configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "list method Configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, txn) { workspaceContext =>
            val configs = methodConfigDAO.list(workspaceContext, txn).toSet
            assertResult(configs) {
              responseAs[Array[MethodConfigurationShort]].toSet
            }
          }
        }
      }
  }
}
