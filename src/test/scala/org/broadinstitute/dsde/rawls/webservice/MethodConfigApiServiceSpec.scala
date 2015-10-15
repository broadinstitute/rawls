package org.broadinstitute.dsde.rawls.webservice

import akka.actor.{Props, Actor, PoisonPill}
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, GraphEntityDAO, GraphMethodConfigurationDAO, GraphWorkspaceDAO, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
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

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}

/**
 * Created by dvoet on 4/24/15.
 */
class MethodConfigApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(500.seconds)

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

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleServicesDAO, submissionSupervisor)_

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
      MethodRepoMethod(testData.wsName.namespace, "method-a", 1))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, newMethodConfig.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
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

  it should "validate attribute syntax in create method configuration" in withTestDataApiServices { services =>
    val inputs = Map("good_in" -> AttributeString("this.foo"), "bad_in" -> AttributeString("does.not.parse"))
    val outputs = Map("good_out" -> AttributeString("this.bar"), "bad_out" -> AttributeString("also.does.not.parse"))
    val newMethodConfig = MethodConfiguration("dsde", "testConfigNew", "samples", Map("ready" -> AttributeString("true")), inputs, outputs,
      MethodRepoMethod(testData.wsName.namespace, "method-a", 1))

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map("bad_in" -> "Failed at line 1, column 1: `workspace.' expected but `d' found")
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map("bad_out" -> "Failed at line 1, column 1: `workspace.' expected but `a' found")

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", HttpEntity(ContentTypes.`application/json`, newMethodConfig.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(ValidatedMethodConfiguration(newMethodConfig, expectedSuccessInputs, expectedFailureInputs, expectedSuccessOutputs, expectedFailureOutputs)) {
          responseAs[ValidatedMethodConfiguration]
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            // all inputs and outputs are saved, regardless of parsing errors
            for ((key, value) <- inputs) assertResult(Option(value)) {
              methodConfigDAO.get(workspaceContext, newMethodConfig.namespace, newMethodConfig.name, txn).get.inputs.get(key)
            }
            for ((key, value) <- outputs) assertResult(Option(value)) {
              methodConfigDAO.get(workspaceContext, newMethodConfig.namespace, newMethodConfig.name, txn).get.outputs.get(key)
            }
          }
        }
      }

  }

  it should "return 409 on method configuration rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", HttpEntity(ContentTypes.`application/json`, MethodConfigurationName("testConfig2_changed", testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)).toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
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
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
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
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            assertResult(None) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn)
            }
          }
        }
      }
  }

  it should "return 404 method configuration delete, method configuration does not exist" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}x") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }

        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
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
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(modifiedMethodConfig) {
          responseAs[ValidatedMethodConfiguration].methodConfiguration
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            assertResult(Option(AttributeString("foo2"))) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.inputs.get("param2")
            }
          }
        }
      }
  }

  it should "validate attribute syntax in update method configuration" in withTestDataApiServices { services =>
    val newInputs = Map("good_in" -> AttributeString("this.foo"), "bad_in" -> AttributeString("does.not.parse"))
    val newOutputs = Map("good_out" -> AttributeString("this.bar"), "bad_out" -> AttributeString("also.does.not.parse"))
    val modifiedMethodConfig = testData.methodConfig.copy(inputs = newInputs, outputs = newOutputs)

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map("bad_in" -> "Failed at line 1, column 1: `workspace.' expected but `d' found")
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map("bad_out" -> "Failed at line 1, column 1: `workspace.' expected but `a' found")

    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}", HttpEntity(ContentTypes.`application/json`, modifiedMethodConfig.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(ValidatedMethodConfiguration(modifiedMethodConfig, expectedSuccessInputs, expectedFailureInputs, expectedSuccessOutputs, expectedFailureOutputs)) {
          responseAs[ValidatedMethodConfiguration]
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            // all inputs and outputs are saved, regardless of parsing errors
            for ((key, value) <- newInputs) assertResult(Option(value)) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.inputs.get(key)
            }
            for ((key, value) <- newOutputs) assertResult(Option(value)) {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.outputs.get(key)
            }
          }
        }
      }
  }

  it should "get syntax validation information for a method configuration" in withTestDataApiServices { services =>
    val theInputs = Map("good_in" -> AttributeString("this.foo"), "bad_in" -> AttributeString("does.not.parse"))
    val theOutputs = Map("good_out" -> AttributeString("this.bar"), "bad_out" -> AttributeString("also.does.not.parse"))

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map("bad_in" -> "Failed at line 1, column 1: `workspace.' expected but `d' found")
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map("bad_out" -> "Failed at line 1, column 1: `workspace.' expected but `a' found")

    val foo = testData.methodConfig.copy(name = "blah",inputs = theInputs, outputs = theOutputs)

    services.dataSource.inTransaction { txn =>
      withWorkspaceContext(testData.workspace, writeLock = true, txn) { workspaceContext =>
        methodConfigDAO.save(workspaceContext, foo, txn)
      }
    }

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/blah/validate") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assertResult(ValidatedMethodConfiguration(foo, expectedSuccessInputs, expectedFailureInputs, expectedSuccessOutputs, expectedFailureOutputs)) {
          responseAs[ValidatedMethodConfiguration]
        }
      }
  }

  it should "return 404 on update method configuration" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/update}", HttpEntity(ContentTypes.`application/json`, testData.methodConfig.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairCreated.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            assertResult("testConfig1") {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
            }
          }
        }
      }
  }

  it should "return 409 on copy method configuration to existing name" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairConflict.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", HttpEntity(ContentTypes.`application/json`, testData.methodConfigNamePairNotFound.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  val copyToMethodRepo = "/methodconfigs/copyToMethodRepo"

  it should "return 200 on copy method configuration to method repo" in withTestDataApiServices { services =>
    Post(copyToMethodRepo, HttpEntity(ContentTypes.`application/json`, MethodRepoConfigurationExport("mcns", "mcn", testData.methodConfigName).toJson.toString)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration to method repo if config dne" in withTestDataApiServices { services =>
    Post(copyToMethodRepo, HttpEntity(ContentTypes.`application/json`, MethodRepoConfigurationExport("mcns", "mcn", testData.methodConfigName3).toJson.toString)) ~>
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
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            assertResult("testConfig1") {
              methodConfigDAO.get(workspaceContext, testData.methodConfig.namespace, testData.methodConfig.name, txn).get.name
            }
          }
        }
      }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in withTestDataApiServices { services =>
    val existingMethodConfigCopy = MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, testData.methodConfigName)
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, existingMethodConfigCopy.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoMissing.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoEmptyPayload.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, HttpEntity(ContentTypes.`application/json`, testData.methodRepoBadPayload.toJson.toString())) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 200 when generating a method config template from a valid method" in withTestDataApiServices { services =>
    val method = MethodRepoMethod("dsde","three_step",1)
    Post("/methodconfigs/template",HttpEntity(ContentTypes.`application/json`,method.toJson.toString)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        val methodConfiguration = MethodConfiguration("namespace","name","rootEntityType",Map(), Map("three_step.cgrep.pattern" -> AttributeString("expression")),
          Map("three_step.ps.procs"->AttributeString("expression"),"three_step.cgrep.count"->AttributeString("expression"), "three_step.wc.count"->AttributeString("expression")),
          MethodRepoMethod("dsde","three_step",1))
        assertResult(methodConfiguration) { responseAs[MethodConfiguration] }
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when generating a method config template from a bogus method" in withTestDataApiServices { services =>
    Post("/methodconfigs/template",HttpEntity(ContentTypes.`application/json`,MethodRepoMethod("dsde","three_step",2).toJson.toString)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 on get method configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "list method Configuration" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        services.dataSource.inTransaction { txn =>
          withWorkspaceContext(testData.workspace, writeLock = false, txn) { workspaceContext =>
            val configs = methodConfigDAO.list(workspaceContext, txn).toSet
            assertResult(configs) {
              responseAs[Array[MethodConfigurationShort]].toSet
            }
          }
        }
      }
  }
}
