package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.http._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class MethodConfigApiServiceSpec extends ApiServiceSpec {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  override implicit val routeTestTimeout = RouteTestTimeout(500.seconds)

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"))
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

  "MethodConfigApi" should "return 201 on create method configuration" in withTestDataApiServices { services =>
    val newMethodConfig = MethodConfiguration("dsde", "testConfigNew", "samples", Map("ready" -> AttributeString("true")), Map("param1" -> AttributeString("foo")), Map("out" -> AttributeString("bar")),
      MethodRepoMethod(testData.wsName.namespace, "method-a", 1))
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", httpJson(newMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(newMethodConfig) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), newMethodConfig.namespace, newMethodConfig.name)).get
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
    val expectedFailureInputs = Map("bad_in" -> "Failed at line 1, column 1: string matching regex `^\\\".*\\\"$' expected but `d' found")
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map("bad_out" -> "Failed at line 1, column 1: `workspace.' expected but `a' found")

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", httpJson(newMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(ValidatedMethodConfiguration(newMethodConfig, expectedSuccessInputs, expectedFailureInputs, expectedSuccessOutputs, expectedFailureOutputs)) {
          responseAs[ValidatedMethodConfiguration]
        }
        // all inputs and outputs are saved, regardless of parsing errors
        for ((key, value) <- inputs) assertResult(Option(value)) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), newMethodConfig.namespace, newMethodConfig.name)).get.inputs.get(key)
        }
        for ((key, value) <- outputs) assertResult(Option(value)) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), newMethodConfig.namespace, newMethodConfig.name)).get.outputs.get(key)
        }
      }
  }

  // DSDEEPB-1433
  it should "successfully create two method configs with the same name but different namespaces" in withTestDataApiServices { services =>
    val mc1 = MethodConfiguration("ws1", "testConfig", "samples", Map(), Map(), Map(), MethodRepoMethod(testData.wsName.namespace, "method-a", 1))
    val mc2 = MethodConfiguration("ws2", "testConfig", "samples", Map(), Map(), Map(), MethodRepoMethod(testData.wsName.namespace, "method-a", 1))

    create(mc1)
    create(mc2)
    get(mc1)
    get(mc2)

    def create(mc: MethodConfiguration) = {
      Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs", httpJson(mc)) ~>
        sealRoute(services.methodConfigRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(ValidatedMethodConfiguration(mc, Seq(), Map(), Seq(), Map())) {
            responseAs[ValidatedMethodConfiguration]
          }
        }
    }

    def get(mc: MethodConfiguration) = {
      Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${mc.namespace}/${mc.name}") ~>
        sealRoute(services.methodConfigRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }
    }
  }

  it should "return 409 on method configuration rename when rename already exists" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", httpJson(MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)))) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 204 on method configuration rename" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}/rename", httpJson(MethodConfigurationName("testConfig2_changed", testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)))) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(true) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, "testConfig2_changed")).isDefined
        }
        assertResult(None) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name))
        }
      }
  }

  it should "return 404 on method configuration rename, method configuration does not exist" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/foox/rename", httpJson(MethodConfigurationName(testData.methodConfig.name, testData.methodConfig.namespace, WorkspaceName(testData.workspace.namespace, testData.workspace.name)))) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
        assertResult(true) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).isDefined
        }
        assertResult(None) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, "foox"))
        }
      }
  }

  /*
   * test disabled until we decide what to do with submissions that reference deleted configs
   */
  ignore should "*DISABLED* return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name))
        }
      }
  }

  it should "return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig3.namespace}/${testData.methodConfig3.name}") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig3.namespace, testData.methodConfig3.name))
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
        assertResult(true) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).isDefined
        }
        assertResult(None) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, "foox"))
        }
      }
  }

  it should "return 200 on update method configuration" in withTestDataApiServices { services =>
    val modifiedMethodConfig = testData.methodConfig.copy(inputs = testData.methodConfig.inputs + ("param2" -> AttributeString("foo2")))
    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}", httpJson(modifiedMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(modifiedMethodConfig) {
          responseAs[ValidatedMethodConfiguration].methodConfiguration
        }
        assertResult(Option(AttributeString("foo2"))) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).get.inputs.get("param2")
        }
      }
  }

  it should "validate attribute syntax in update method configuration" in withTestDataApiServices { services =>
    val newInputs = Map("good_in" -> AttributeString("this.foo"), "bad_in" -> AttributeString("does.not.parse"))
    val newOutputs = Map("good_out" -> AttributeString("this.bar"), "bad_out" -> AttributeString("also.does.not.parse"))
    val modifiedMethodConfig = testData.methodConfig.copy(inputs = newInputs, outputs = newOutputs)

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map("bad_in" -> "Failed at line 1, column 1: string matching regex `^\\\".*\\\"$' expected but `d' found")
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map("bad_out" -> "Failed at line 1, column 1: `workspace.' expected but `a' found")

    Put(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/${testData.methodConfig.namespace}/${testData.methodConfig.name}", httpJson(modifiedMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(ValidatedMethodConfiguration(modifiedMethodConfig, expectedSuccessInputs, expectedFailureInputs, expectedSuccessOutputs, expectedFailureOutputs)) {
          responseAs[ValidatedMethodConfiguration]
        }
        // all inputs and outputs are saved, regardless of parsing errors
        for ((key, value) <- newInputs) assertResult(Option(value)) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).get.inputs.get(key)
        }
        for ((key, value) <- newOutputs) assertResult(Option(value)) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).get.outputs.get(key)
        }
      }
  }

  it should "get syntax validation information for a method configuration" in withTestDataApiServices { services =>
    val theInputs = Map("good_in" -> AttributeString("this.foo"), "bad_in" -> AttributeString("does.not.parse"))
    val theOutputs = Map("good_out" -> AttributeString("this.bar"), "bad_out" -> AttributeString("also.does.not.parse"))

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map("bad_in" -> "Failed at line 1, column 1: string matching regex `^\\\".*\\\"$' expected but `d' found")
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map("bad_out" -> "Failed at line 1, column 1: `workspace.' expected but `a' found")

    val foo = testData.methodConfig.copy(name = "blah",inputs = theInputs, outputs = theOutputs)

    runAndWait(methodConfigurationQuery.save(SlickWorkspaceContext(testData.workspace), foo))

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
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/methodconfigs/update}", httpJson(testData.methodConfig)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairCreated)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult("testConfig1") {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).get.name
        }
      }
  }

  it should "return 409 on copy method configuration to existing name" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairConflict)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairNotFound)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  val copyToMethodRepo = "/methodconfigs/copyToMethodRepo"

  it should "return 200 on copy method configuration to method repo" in withTestDataApiServices { services =>
    Post(copyToMethodRepo, httpJson(MethodRepoConfigurationExport("mcns", "mcn", testData.methodConfigName))) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration to method repo if config dne" in withTestDataApiServices { services =>
    Post(copyToMethodRepo, httpJson(MethodRepoConfigurationExport("mcns", "mcn", testData.methodConfigName3))) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  val copyFromMethodRepo = "/methodconfigs/copyFromMethodRepo"

  it should "return 201 on copy method configuration from method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, httpJson(testData.methodRepoGood)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult("testConfig1") {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.methodConfig.namespace, testData.methodConfig.name)).get.name
        }
      }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in withTestDataApiServices { services =>
    val existingMethodConfigCopy = MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, testData.methodConfigName)
    Post(copyFromMethodRepo, httpJson(existingMethodConfigCopy)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, httpJson(testData.methodRepoMissing)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, httpJson(testData.methodRepoEmptyPayload)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in withTestDataApiServices { services =>
    Post(copyFromMethodRepo, httpJson(testData.methodRepoBadPayload)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }
  }

  it should "return 200 when generating a method config template from a valid method" in withTestDataApiServices { services =>
    val method = MethodRepoMethod("dsde","three_step",1)
    Post("/methodconfigs/template", httpJson(method)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        val methodConfiguration = MethodConfiguration("namespace","name","rootEntityType",Map(), Map("three_step.cgrep.pattern" -> AttributeString("expression")),
          Map("three_step.ps.procs"->AttributeString("expression"),"three_step.cgrep.count"->AttributeString("expression"), "three_step.wc.count"->AttributeString("expression")),
          MethodRepoMethod("dsde","three_step",1))
        assertResult(methodConfiguration) { responseAs[MethodConfiguration] }
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 200 getting method inputs and outputs" in withTestDataApiServices { services =>
    val method = MethodRepoMethod("dsde","three_step",1)
    Post("/methodconfigs/inputsOutputs", httpJson(method)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(MethodInputsOutputs(Seq(MethodInput("three_step.cgrep.pattern","String",false)),Seq(MethodOutput("three_step.ps.procs","File"), MethodOutput("three_step.cgrep.count","Int"), MethodOutput("three_step.wc.count","Int")))) {
          responseAs[MethodInputsOutputs]
        }
      }
  }

  it should "return 404 when generating a method config template from a bogus method" in withTestDataApiServices { services =>
    Post("/methodconfigs/template", httpJson(MethodRepoMethod("dsde","three_step",2))) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 404 getting method inputs and outputs from a bogus method" in withTestDataApiServices { services =>
    Post("/methodconfigs/template", httpJson(MethodRepoMethod("dsde","three_step",2))) ~>
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
        val configs = runAndWait(methodConfigurationQuery.list(SlickWorkspaceContext(testData.workspace))).toSet
        assertResult(configs) {
          responseAs[Array[MethodConfigurationShort]].toSet
        }
      }
  }
}
