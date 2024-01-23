package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.testkit.RouteTestTimeout
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.broadinstitute.dsde.rawls.dataaccess.MockCromwellSwaggerClient.makeBadWorkflowDescription
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.scalatest.concurrent.ScalaFutures
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class MethodConfigApiServiceSpec extends ApiServiceSpec with TestDriverComponent with ScalaFutures {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit override val routeTestTimeout: RouteTestTimeout = RouteTestTimeout(500.seconds)

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives

  val badWdlFromMockServer = WdlSource("Bad syntax workflow returned from Agora mock server")
  val badWdlFromMockServerDescription =
    makeBadWorkflowDescription(
      "Agora-bad-wdl",
      List(
        "ERROR: Finished parsing without consuming all tokens.\n\nBad syntax workflow returned from Agora mock server\n^\n     "
      )
    )
  mockCromwellSwaggerClient.workflowDescriptions += (badWdlFromMockServer -> badWdlFromMockServerDescription)

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def testCreateMethodConfiguration(method: MethodRepoMethod, wdlName: String, services: TestApiService) = {
    val inputMethodConfig =
      MethodConfiguration(
        "dsde",
        s"testConfigNew-${method.repo.scheme}",
        Some("samples"),
        None,
        Map(s"$wdlName.cgrep.pattern" -> AttributeString("this.foo")),
        Map(s"$wdlName.cgrep.count" -> AttributeString("this.bar")),
        method
      )
    val expectedMethodConfig = inputMethodConfig.copy(prerequisites = Some(Map())) // test that empty prereqs work too
    withStatsD {
      Post(s"${testData.workspace.path}/methodconfigs", httpJson(inputMethodConfig)) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(expectedMethodConfig) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace, inputMethodConfig.namespace, inputMethodConfig.name)
            ).get
          }
          // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
          assertResult(
            Some(
              Location(
                Uri("http",
                    Uri.Authority(Uri.Host("example.com")),
                    Uri.Path(expectedMethodConfig.path(testData.wsName))
                )
              )
            )
          ) {
            header("Location")
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected =
        expectedHttpRequestMetrics("post", s"$wsPathForRequestMetrics.methodconfigs", StatusCodes.Created.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  "MethodConfigApi" should "return 201 on create method configuration in Agora" in withTestDataApiServices { services =>
    testCreateMethodConfiguration(AgoraMethod("dsde", "three_step", 1), "three_step", services)
  }

  it should "return 201 on create method configuration in Dockstore" in withTestDataApiServices { services =>
    testCreateMethodConfiguration(DockstoreMethod("dockstore-method-path", "dockstore-method-version"),
                                  "three_step_dockstore",
                                  services
    )
  }

  it should "update the workspace last modified date on create method configuration" in withTestDataApiServices {
    services =>
      val newMethodConfig = MethodConfiguration(
        "dsde",
        "testConfigNew",
        Some("samples"),
        None,
        Map("three_step.cgrep.pattern" -> AttributeString("this.foo")),
        Map("three_step.cgrep.count" -> AttributeString("this.bar")),
        AgoraMethod("dsde", "three_step", 1)
      )
      Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
      withStatsD {
        Get(testData.workspace.path) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
          }
      } { capturedMetrics =>
        val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
        val expected = expectedHttpRequestMetrics("get", s"$wsPathForRequestMetrics", StatusCodes.OK.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }
  }

  it should "validate attribute syntax in create method configuration" in withTestDataApiServices { services =>
    // This tests that invalid MC expressions still return 201 and a ValidatedMethodConfiguration with validation results in it
    val inputs = Map("goodAndBad.goodAndBadTask.good_in" -> AttributeString("this.foo"),
                     "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("does.not.parse")
    )
    val outputs = Map(
      "goodAndBad.goodAndBadTask.good_out" -> AttributeString("this.bar"),
      "goodAndBad.goodAndBadTask.bad_out" -> AttributeString("also.does.not.parse"),
      "empty_out" -> AttributeString("")
    )
    val newMethodConfig = MethodConfiguration("dsde",
                                              "good_and_bad2",
                                              Some("samples"),
                                              None,
                                              inputs,
                                              outputs,
                                              AgoraMethod("dsde", "good_and_bad", 1)
    )

    val expectedSuccessInputs = Seq("goodAndBad.goodAndBadTask.good_in")
    val expectedFailureInputs = Map(
      "goodAndBad.goodAndBadTask.bad_in" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'does' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )
    val expectedSuccessOutputs = Seq("goodAndBad.goodAndBadTask.good_out", "empty_out")
    val expectedFailureOutputs = Map(
      "goodAndBad.goodAndBadTask.bad_out" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'also' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )

    Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        val validated = responseAs[ValidatedMethodConfiguration]
        assertResult(newMethodConfig)(validated.methodConfiguration)
        assertSameElements(expectedSuccessInputs, validated.validInputs)
        assertSameElements(expectedFailureInputs, validated.invalidInputs)
        assertSameElements(expectedSuccessOutputs, validated.validOutputs)
        assertSameElements(expectedFailureOutputs, validated.invalidOutputs)

        // all inputs and outputs are saved, regardless of parsing errors
        for ((key, value) <- inputs) assertResult(Option(value)) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace, newMethodConfig.namespace, newMethodConfig.name)
          ).get.inputs.get(key)
        }
        for ((key, value) <- outputs) assertResult(Option(value)) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace, newMethodConfig.namespace, newMethodConfig.name)
          ).get.outputs.get(key)
        }
      }
  }

  it should "validate attribute syntax in create method configuration without data model" in withTestDataApiServices {
    services =>
      // This tests that invalid MC expressions still return 201 and a ValidatedMethodConfiguration with validation results in it
      val inputs = Map("goodAndBad.goodAndBadTask.good_in" -> AttributeString("workspace.foo"),
                       "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("blah")
      )
      val outputs = Map(
        "goodAndBad.goodAndBadTask.good_out" -> AttributeString("workspace.bar"),
        "goodAndBad.goodAndBadTask.bad_out" -> AttributeString("this.nomodel"),
        "empty_out" -> AttributeString("")
      )
      val newMethodConfig = MethodConfiguration("dsde",
                                                "good_and_bad2",
                                                None,
                                                None,
                                                inputs,
                                                outputs,
                                                AgoraMethod("dsde", "good_and_bad", 1)
      )

      val expectedSuccessInputs = Seq("goodAndBad.goodAndBadTask.good_in")
      val expectedFailureInputs = Map(
        "goodAndBad.goodAndBadTask.bad_in" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'blah' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
      )
      val expectedSuccessOutputs = Seq("goodAndBad.goodAndBadTask.good_out", "empty_out")
      val expectedFailureOutputs = Map(
        "goodAndBad.goodAndBadTask.bad_out" -> "Output expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."
      )

      Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          val validated = responseAs[ValidatedMethodConfiguration]
          assertResult(newMethodConfig)(validated.methodConfiguration)
          assertSameElements(expectedSuccessInputs, validated.validInputs)
          assertSameElements(expectedFailureInputs, validated.invalidInputs)
          assertSameElements(expectedSuccessOutputs, validated.validOutputs)
          assertSameElements(expectedFailureOutputs, validated.invalidOutputs)

          // all inputs and outputs are saved, regardless of parsing errors
          for ((key, value) <- inputs) assertResult(Option(value)) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace, newMethodConfig.namespace, newMethodConfig.name)
            ).get.inputs.get(key)
          }
          for ((key, value) <- outputs) assertResult(Option(value)) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace, newMethodConfig.namespace, newMethodConfig.name)
            ).get.outputs.get(key)
          }
        }
  }

  it should "return 404 if you try to create a method configuration that points to an unknown method" in withTestDataApiServices {
    services =>
      val newMethodConfig = MethodConfiguration("dsde",
                                                "good_and_bad2",
                                                Some("samples"),
                                                None,
                                                Map(),
                                                Map(),
                                                AgoraMethod("dsde", "method_doesnt_exist", 1)
      )

      Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "not allow library attributes in outputs for create method configuration by curator" in withTestDataApiServices {
    services =>
      val inputs = Map("lib_ent_in" -> AttributeString("this.library:foo"),
                       "lib_ws_in" -> AttributeString("workspace.library:foo")
      )
      val outputs = Map("lib_ent_out" -> AttributeString("this.library:bar"),
                        "lib_ws_out" -> AttributeString("workspace.library:bar")
      )
      val newMethodConfig = MethodConfiguration("dsde",
                                                "testConfigNew",
                                                Some("samples"),
                                                None,
                                                inputs,
                                                outputs,
                                                AgoraMethod(testData.wsName.namespace, "method-a", 1)
      )

      val expectedSuccessInputs = Seq("lib_ent_in", "lib_ws_in")
      val expectedSuccessOutputs = Seq("lib_ent_out", "lib_ws_out")

      Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "allow library attributes in input for create method configuration by non-curator" in withTestDataApiServices {
    services =>
      val inputs = Map("goodAndBad.goodAndBadTask.good_in" -> AttributeString("this.library:foo"),
                       "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("workspace.library:foo")
      )
      val outputs = Map("goodAndBad.goodAndBadTask.good_out" -> AttributeString("this.bar"),
                        "goodAndBad.goodAndBadTask.bad_out" -> AttributeString("workspace.bar")
      )

      val newMethodConfig = MethodConfiguration("dsde",
                                                "good_and_bad2",
                                                Some("samples"),
                                                None,
                                                inputs,
                                                outputs,
                                                AgoraMethod("dsde", "good_and_bad", 1)
      )

      val expectedSuccessInputs = Set("goodAndBad.goodAndBadTask.good_in", "goodAndBad.goodAndBadTask.bad_in")
      val expectedSuccessOutputs = Set("goodAndBad.goodAndBadTask.good_out", "goodAndBad.goodAndBadTask.bad_out")

      revokeCuratorRole(services)

      Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(
            ValidatedMethodConfiguration(newMethodConfig,
                                         expectedSuccessInputs,
                                         Map(),
                                         Set(),
                                         Set(),
                                         expectedSuccessOutputs,
                                         Map()
            )
          ) {
            responseAs[ValidatedMethodConfiguration]
          }
          // all inputs and outputs are saved, regardless of parsing errors
          for ((key, value) <- inputs) assertResult(Option(value)) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace, newMethodConfig.namespace, newMethodConfig.name)
            ).get.inputs.get(key)
          }
          for ((key, value) <- outputs) assertResult(Option(value)) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace, newMethodConfig.namespace, newMethodConfig.name)
            ).get.outputs.get(key)
          }
        }
  }

  it should "not allow library attributes in outputs for create method configuration by non-curator" in withTestDataApiServices {
    services =>
      val inputs = Map("lib_ent_in" -> AttributeString("this.library:foo"),
                       "lib_ws_in" -> AttributeString("workspace.library:foo")
      )
      val outputs = Map("lib_ent_out" -> AttributeString("this.library:bar"),
                        "lib_ws_out" -> AttributeString("workspace.library:bar")
      )
      val newMethodConfig = MethodConfiguration("dsde",
                                                "testConfigNew",
                                                Some("samples"),
                                                None,
                                                inputs,
                                                outputs,
                                                AgoraMethod(testData.wsName.namespace, "method-a", 1)
      )

      revokeCuratorRole(services)

      val expectedSuccessInputs = Seq("lib_ent_in", "lib_ws_in")
      val expectedSuccessOutputs = Seq("lib_ent_out", "lib_ws_out")

      Post(s"${testData.workspace.path}/methodconfigs", httpJson(newMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  // DSDEEPB-1433
  it should "successfully create two method configs with the same name but different namespaces" in withTestDataApiServices {
    services =>
      val mc1 = MethodConfiguration("ws1",
                                    "testConfig",
                                    Some("samples"),
                                    None,
                                    Map(),
                                    Map(),
                                    AgoraMethod("dsde", "good_and_bad", 1)
      )
      val mc2 = MethodConfiguration("ws2",
                                    "testConfig",
                                    Some("samples"),
                                    None,
                                    Map(),
                                    Map(),
                                    AgoraMethod("dsde", "good_and_bad", 1)
      )

      create(mc1)
      create(mc2)
      get(mc1)
      get(mc2)

      def create(mc: MethodConfiguration) =
        Post(s"${testData.workspace.path}/methodconfigs", httpJson(mc)) ~>
          sealRoute(services.methodConfigRoutes()) ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
            assertResult(
              ValidatedMethodConfiguration(mc,
                                           Set(),
                                           Map(),
                                           Set("goodAndBad.goodAndBadTask.bad_in", "goodAndBad.goodAndBadTask.good_in"),
                                           Set(),
                                           Set(),
                                           Map()
              )
            ) {
              responseAs[ValidatedMethodConfiguration]
            }
          }

      def get(mc: MethodConfiguration) =
        Get(s"${testData.workspace.path}/methodconfigs/${mc.namespace}/${mc.name}") ~>
          sealRoute(services.methodConfigRoutes()) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
          }
  }

  it should "return 204 on method configuration rename" in withTestDataApiServices { services =>
    Post(
      s"${testData.agoraMethodConfig.path(testData.workspace)}/rename",
      httpJson(
        MethodConfigurationName("testConfig2_changed",
                                testData.agoraMethodConfig.namespace,
                                WorkspaceName(testData.workspace.namespace, testData.workspace.name)
        )
      )
    ) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(true) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.agoraMethodConfig.namespace,
                                         "testConfig2_changed"
            )
          ).isDefined
        }
        assertResult(None) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.agoraMethodConfig.namespace,
                                         testData.agoraMethodConfig.name
            )
          )
        }
      }
  }

  it should "return 204 on method configuration rename on top of yourself" in withTestDataApiServices { services =>
    withStatsD {
      Post(
        s"${testData.agoraMethodConfig.path(testData.workspace)}/rename",
        httpJson(
          MethodConfigurationName(testData.agoraMethodConfig.name,
                                  testData.agoraMethodConfig.namespace,
                                  WorkspaceName(testData.workspace.namespace, testData.workspace.name)
          )
        )
      ) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected = expectedHttpRequestMetrics("post",
                                                s"$wsPathForRequestMetrics.methodconfigs.redacted.redacted.rename",
                                                StatusCodes.NoContent.intValue,
                                                1
      )
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 400 on method configuration rename when workspace in URI doesn't match payload" in withTestDataApiServices {
    services =>
      withStatsD {
        Post(
          s"${testData.methodConfig2.path(testData.workspace)}/rename",
          httpJson(
            MethodConfigurationName(testData.agoraMethodConfig.name,
                                    testData.agoraMethodConfig.namespace,
                                    WorkspaceName("uh_oh", "bad_times")
            )
          )
        ) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.BadRequest) {
              status
            }
          }
      } { capturedMetrics =>
        val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
        val expected = expectedHttpRequestMetrics("post",
                                                  s"$wsPathForRequestMetrics.methodconfigs.redacted.redacted.rename",
                                                  StatusCodes.BadRequest.intValue,
                                                  1
        )
        assertSubsetOf(expected, capturedMetrics)
      }
  }

  it should "return 409 on method configuration rename when destination already exists" in withTestDataApiServices {
    services =>
      withStatsD {
        Post(
          s"${testData.methodConfig2.path(testData.workspace)}/rename",
          httpJson(
            MethodConfigurationName(testData.agoraMethodConfig.name,
                                    testData.agoraMethodConfig.namespace,
                                    WorkspaceName(testData.workspace.namespace, testData.workspace.name)
            )
          )
        ) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.Conflict) {
              status
            }
          }
      } { capturedMetrics =>
        val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
        val expected = expectedHttpRequestMetrics("post",
                                                  s"$wsPathForRequestMetrics.methodconfigs.redacted.redacted.rename",
                                                  StatusCodes.Conflict.intValue,
                                                  1
        )
        assertSubsetOf(expected, capturedMetrics)
      }
  }

  it should "update the workspace last modified date on method configuration rename" in withTestDataApiServices {
    services =>
      Post(
        s"${testData.agoraMethodConfig.path(testData.workspace)}/rename",
        httpJson(
          MethodConfigurationName("testConfig2_changed",
                                  testData.agoraMethodConfig.namespace,
                                  WorkspaceName(testData.workspace.namespace, testData.workspace.name)
          )
        )
      ) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      Get(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes()) ~>
        check {
          assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
        }
  }

  it should "return 404 on method configuration rename, method configuration does not exist" in withTestDataApiServices {
    services =>
      Post(
        s"${testData.workspace.path}/methodconfigs/${testData.agoraMethodConfig.namespace}/foox/rename",
        httpJson(
          MethodConfigurationName(testData.agoraMethodConfig.name,
                                  testData.agoraMethodConfig.namespace,
                                  WorkspaceName(testData.workspace.namespace, testData.workspace.name)
          )
        )
      ) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
          assertResult(true) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace,
                                           testData.agoraMethodConfig.namespace,
                                           testData.agoraMethodConfig.name
              )
            ).isDefined
          }
          assertResult(None) {
            runAndWait(methodConfigurationQuery.get(testData.workspace, testData.agoraMethodConfig.namespace, "foox"))
          }
        }
  }

  /*
   * test disabled until we decide what to do with submissions that reference deleted configs
   */
  /*
  ignore should "*DISABLED* return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(testData.agoraMethodConfig.path(testData.workspace)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(methodConfigurationQuery.get(SlickWorkspaceContext(testData.workspace), testData.agoraMethodConfig.namespace, testData.agoraMethodConfig.name))
        }
      }
  }
   */

  it should "return 204 method configuration delete" in withTestDataApiServices { services =>
    Delete(testData.methodConfig3.path(testData.workspace)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
        assertResult(None) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.methodConfig3.namespace,
                                         testData.methodConfig3.name
            )
          )
        }
      }
  }

  it should "update the workspace last modified date on delete method configuration" in withTestDataApiServices {
    services =>
      withStatsD {
        Delete(testData.methodConfig3.path(testData.workspace)) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.NoContent) {
              status
            }
          }
      } { capturedMetrics =>
        val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
        val expected = expectedHttpRequestMetrics("delete",
                                                  s"$wsPathForRequestMetrics.methodconfigs.redacted.redacted",
                                                  StatusCodes.NoContent.intValue,
                                                  1
        )
        assertSubsetOf(expected, capturedMetrics)
      }
      Get(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes()) ~>
        check {
          assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
        }
  }

  it should "return 404 method configuration delete, method configuration does not exist" in withTestDataApiServices {
    services =>
      Delete(testData.agoraMethodConfig.copy(name = "DNE").path(testData.workspace)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }

          assertResult(true) {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace,
                                           testData.agoraMethodConfig.namespace,
                                           testData.agoraMethodConfig.name
              )
            ).isDefined
          }
          assertResult(None) {
            runAndWait(methodConfigurationQuery.get(testData.workspace, testData.agoraMethodConfig.namespace, "foox"))
          }
        }
  }

  def check200AddMC(httpMethod: RequestBuilder) = withTestDataApiServices { services =>
    val combinations =
      List(
        (testData.agoraMethodConfig,
         testData.agoraMethodConfig
           .copy(inputs = testData.agoraMethodConfig.inputs + ("param2" -> AttributeString("foo2"))),
         2
        ),
        (testData.dockstoreMethodConfig,
         testData.dockstoreMethodConfig
           .copy(inputs = testData.dockstoreMethodConfig.inputs + ("param3" -> AttributeString("foo3"))),
         3
        )
      )

    combinations foreach { pair: (MethodConfiguration, MethodConfiguration, Int) =>
      val original = pair._1
      val edited = pair._2
      val fooFactor = pair._3

      httpMethod(original.path(testData.workspace), httpJson(edited)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.OK) {
            println(original)
            status
          }
          assertResult(edited) {
            responseAs[ValidatedMethodConfiguration].methodConfiguration
          }
          assertResult(Option(AttributeString(s"foo$fooFactor"))) {
            runAndWait(methodConfigurationQuery.get(testData.workspace, original.namespace, original.name)).get.inputs
              .get(s"param$fooFactor")
          }
        }
    }
  }

  it should "return 200 on put method configuration" in {
    check200AddMC(Put)
  }

  it should "return 200 on post method configuration" in {
    check200AddMC(Post)
  }

  def checkLastModified(httpMethod: RequestBuilder) = withTestDataApiServices { services =>
    val modifiedMethodConfig = testData.agoraMethodConfig.copy(inputs =
      testData.agoraMethodConfig.inputs + ("param2" -> AttributeString("foo2"))
    )
    withStatsD {
      httpMethod(testData.agoraMethodConfig.path(testData.workspace), httpJson(modifiedMethodConfig)) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }
      Get(testData.workspace.path) ~>
        services.sealedInstrumentedRoutes ~>
        check {
          assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected = expectedHttpRequestMetrics(httpMethod.method.name.toLowerCase,
                                                s"$wsPathForRequestMetrics.methodconfigs.redacted.redacted",
                                                StatusCodes.OK.intValue,
                                                1
      ) ++
        expectedHttpRequestMetrics("get", s"${wsPathForRequestMetrics}", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "update the workspace last modified date on put method configuration" in {
    checkLastModified(Put)
  }

  it should "update the workspace last modified date on post method configuration" in {
    checkLastModified(Post)
  }

  def checkValidAttributeSyntax(httpMethod: RequestBuilder) = withTestDataApiServices { services =>
    val expectedSuccessInputs = Seq("goodAndBad.goodAndBadTask.good_in")
    val expectedFailureInputs = Map(
      "goodAndBad.goodAndBadTask.bad_in" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'does' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )
    val expectedSuccessOutputs = Seq("goodAndBad.goodAndBadTask.good_out", "empty_out")
    val expectedFailureOutputs = Map(
      "goodAndBad.goodAndBadTask.bad_out" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'also' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )

    httpMethod(testData.goodAndBadMethodConfig.path(testData.workspace), httpJson(testData.goodAndBadMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val validated = responseAs[ValidatedMethodConfiguration]
        assertResult(testData.goodAndBadMethodConfig)(validated.methodConfiguration)
        assertSameElements(expectedSuccessInputs, validated.validInputs)
        assertSameElements(expectedFailureInputs, validated.invalidInputs)
        assertSameElements(expectedSuccessOutputs, validated.validOutputs)
        assertSameElements(expectedFailureOutputs, validated.invalidOutputs)
        // all inputs and outputs are saved, regardless of parsing errors
        for ((key, value) <- testData.goodAndBadMethodConfig.inputs) assertResult(Option(value)) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.goodAndBadMethodConfig.namespace,
                                         testData.goodAndBadMethodConfig.name
            )
          ).get.inputs.get(key)
        }
        for ((key, value) <- testData.goodAndBadMethodConfig.outputs) assertResult(Option(value)) {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.goodAndBadMethodConfig.namespace,
                                         testData.goodAndBadMethodConfig.name
            )
          ).get.outputs.get(key)
        }
      }
  }

  it should "validate attribute syntax in put method configuration" in {
    checkValidAttributeSyntax(Put)
  }

  it should "validate attribute syntax in post method configuration" in {
    checkValidAttributeSyntax(Post)
  }

  def checkNoLibraryAttributesInOutputsByCurator(httpMethod: RequestBuilder) = withTestDataApiServices { services =>
    val newInputs = Map("good_in" -> AttributeString("this.foo"))
    val newOutputs = Map("good_out" -> AttributeString("this.library:bar"))
    val modifiedMethodConfig = testData.agoraMethodConfig.copy(inputs = newInputs, outputs = newOutputs)

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map.empty[String, String]
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map.empty[String, String]

    httpMethod(testData.agoraMethodConfig.path(testData.workspace), httpJson(modifiedMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow library attributes in outputs for put method configuration by curator" in {
    checkNoLibraryAttributesInOutputsByCurator(Put)
  }

  it should "not allow library attributes in outputs for post method configuration by curator" in {
    checkNoLibraryAttributesInOutputsByCurator(Post)
  }

  def checkNoLibraryAttributesInOutputsByNonCurator(httpMethod: RequestBuilder) = withTestDataApiServices { services =>
    val newInputs = Map("good_in" -> AttributeString("this.foo"))
    val newOutputs = Map("good_out" -> AttributeString("this.library:bar"))
    val modifiedMethodConfig = testData.agoraMethodConfig.copy(inputs = newInputs, outputs = newOutputs)

    revokeCuratorRole(services)

    val expectedSuccessInputs = Seq("good_in")
    val expectedFailureInputs = Map.empty[String, String]
    val expectedSuccessOutputs = Seq("good_out")
    val expectedFailureOutputs = Map.empty[String, String]

    Put(testData.agoraMethodConfig.path(testData.workspace), httpJson(modifiedMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow library attributes in outputs for put method configuration by non-curator" in {
    checkNoLibraryAttributesInOutputsByNonCurator(Put)
  }

  it should "not allow library attributes in outputs for post method configuration by non-curator" in {
    checkNoLibraryAttributesInOutputsByNonCurator(Post)
  }

  it should "return 400 on put method configuration if the location differs between URI and JSON body" in withTestDataApiServices {
    services =>
      val modifiedMethodConfig =
        testData.agoraMethodConfig.copy(name = "different",
                                        inputs =
                                          testData.agoraMethodConfig.inputs + ("param2" -> AttributeString("foo2"))
        )
      Put(testData.agoraMethodConfig.path(testData.workspace), httpJson(modifiedMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 409 on update method configuration if the destination already exists" in withTestDataApiServices {
    services =>
      val modifiedMethodConfig =
        testData.methodConfig2.copy(inputs = testData.agoraMethodConfig.inputs + ("param2" -> AttributeString("foo2")))
      Post(testData.agoraMethodConfig.path(testData.workspace), httpJson(modifiedMethodConfig)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Conflict) {
            status
          }
        }
  }

  it should "get syntax validation information for a method configuration" in withTestDataApiServices { services =>
    val theInputs = Map("goodAndBad.goodAndBadTask.good_in" -> AttributeString("this.foo"),
                        "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("does.not.parse")
    )
    val theOutputs = Map(
      "goodAndBad.goodAndBadTask.good_out" -> AttributeString("this.bar"),
      "goodAndBad.goodAndBadTask.bad_out" -> AttributeString("also.does.not.parse"),
      "goodAndBad.goodAndBadTask.empty_out" -> AttributeString("")
    )

    val expectedSuccessInputs = Seq("goodAndBad.goodAndBadTask.good_in")
    val expectedFailureInputs = Map(
      "goodAndBad.goodAndBadTask.bad_in" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'does' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )
    val expectedSuccessOutputs = Seq("goodAndBad.goodAndBadTask.good_out", "goodAndBad.goodAndBadTask.empty_out")
    val expectedFailureOutputs = Map(
      "goodAndBad.goodAndBadTask.bad_out" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'also' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )

    val mc = testData.goodAndBadMethodConfig.copy(name = "blah",
                                                  inputs = theInputs,
                                                  outputs = theOutputs,
                                                  prerequisites = Some(Map())
    )

    runAndWait(methodConfigurationQuery.create(testData.workspace, mc))

    Get(s"${mc.path(testData.workspace)}/validate") ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val validated = responseAs[ValidatedMethodConfiguration]
        assertResult(mc)(validated.methodConfiguration)
        assertSameElements(expectedSuccessInputs, validated.validInputs)
        assertSameElements(expectedFailureInputs, validated.invalidInputs)
        assertSameElements(expectedSuccessOutputs, validated.validOutputs)
        assertSameElements(expectedFailureOutputs, validated.invalidOutputs)
      }
  }

  it should "get syntax validation information when using a reserved output attribute" in {
    withTestDataApiServices { services =>
      val entityType = "some_type_of_entity"

      val theInputs = Map(
        "goodAndBad.goodAndBadTask.good_in" -> AttributeString("this.foo"),
        "goodAndBad.goodAndBadTask.bad_in" -> AttributeString("does.not.parse")
      )
      val theOutputs = Map(
        "goodAndBad.goodAndBadTask.good_out" -> AttributeString("this.bar"),
        "goodAndBad.goodAndBadTask.bad_out" -> AttributeString(s"this.${entityType}_id"),
        "goodAndBad.goodAndBadTask.empty_out" -> AttributeString("")
      )

      val expectedSuccessInputs = Seq("goodAndBad.goodAndBadTask.good_in")
      val expectedFailureInputs = Map(
        "goodAndBad.goodAndBadTask.bad_in" -> "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'does' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
      )
      val expectedSuccessOutputs = Seq("goodAndBad.goodAndBadTask.good_out", "goodAndBad.goodAndBadTask.empty_out")
      val expectedFailureOutputs = Map(
        "goodAndBad.goodAndBadTask.bad_out" -> s"Attribute name ${entityType}_id is reserved and cannot be overwritten"
      )

      val mc = testData.goodAndBadMethodConfig.copy(
        name = "blah",
        inputs = theInputs,
        outputs = theOutputs,
        prerequisites = Option(Map()),
        rootEntityType = Option(entityType)
      )

      runAndWait(methodConfigurationQuery.create(testData.workspace, mc))

      Get(s"${mc.path(testData.workspace)}/validate") ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          val validated = responseAs[ValidatedMethodConfiguration]
          assertResult(mc)(validated.methodConfiguration)
          assertSameElements(expectedSuccessInputs, validated.validInputs)
          assertSameElements(expectedFailureInputs, validated.invalidInputs)
          assertSameElements(expectedSuccessOutputs, validated.validOutputs)
          assertSameElements(expectedFailureOutputs, validated.invalidOutputs)
        }
    }
  }

  it should "return 404 on update method configuration" in withTestDataApiServices { services =>
    Post(s"${testData.workspace.path}/methodconfigs/update}", httpJson(testData.agoraMethodConfig)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 on copy method Agora configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairCreated)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult("testConfig1") {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.agoraMethodConfig.namespace,
                                         testData.agoraMethodConfig.name
            )
          ).get.name
        }
      }
  }

  it should "return 201 on copy Dockstore method configuration" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairCreatedDockstore)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult("dockstore-config-name") {
          runAndWait(
            methodConfigurationQuery.get(testData.workspace,
                                         testData.dockstoreMethodConfig.namespace,
                                         testData.dockstoreMethodConfig.name
            )
          ).get.name
        }
      }
  }

  it should "update the destination workspace last modified date on copy method configuration" in withTestDataApiServices {
    services =>
      Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairCreated)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
      Get(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes()) ~>
        check {
          assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
        }
  }

  it should "return 409 on copy method configuration to existing name" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairConflict)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }
  }

  it should "return 404 on copy method configuration from bogus source" in withTestDataApiServices { services =>
    Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairNotFound)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "not allow copy method configuration with library attributes in outputs by curator" in withTestDataApiServices {
    services =>
      Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairFromLibrary)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "allow copy method configuration with library attributes in outputs by non-curator" in withTestDataApiServices {
    services =>
      revokeCuratorRole(services)

      Post("/methodconfigs/copy", httpJson(testData.methodConfigNamePairFromLibrary)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  val copyToMethodRepo = "/methodconfigs/copyToMethodRepo"

  it should "return 200 on copy Agora method configuration to Agora" in withTestDataApiServices { services =>
    withStatsD {
      Post(copyToMethodRepo, httpJson(MethodRepoConfigurationExport("mcns", "mcn", testData.agoraMethodConfigName))) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }
    } { capturedMetrics =>
      val expected =
        expectedHttpRequestMetrics("post", "configurations", StatusCodes.OK.intValue, 1, Some(Subsystems.Agora))
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 400 on copy Dockstore method configuration to Agora" in withTestDataApiServices { services =>
    Post(s"/api${copyToMethodRepo}",
         httpJson(MethodRepoConfigurationExport("mcns", "mcn", testData.dockstoreMethodConfigName))
    ) ~>
      services.route ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
        val responseString = Unmarshal(response.entity).to[String].futureValue
        assert(responseString.contains("Action not supported for method repo"))
      }
  }

  it should "return 404 on copy method configuration to method repo if config dne" in withTestDataApiServices {
    services =>
      Post(copyToMethodRepo, httpJson(MethodRepoConfigurationExport("mcns", "mcn", testData.methodConfigName3))) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  val copyFromMethodRepo = "/methodconfigs/copyFromMethodRepo"

  it should "foo return 201 on copy method configuration from method repo" in withTestDataApiServices { services =>
    withStatsD {
      Post(copyFromMethodRepo, httpJson(testData.methodRepoGood)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult("testConfig1") {
            runAndWait(
              methodConfigurationQuery.get(testData.workspace,
                                           testData.agoraMethodConfig.namespace,
                                           testData.agoraMethodConfig.name
              )
            ).get.name
          }
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("get",
                                                s"configurations.redacted.redacted.redacted",
                                                StatusCodes.OK.intValue,
                                                1,
                                                Some(Subsystems.Agora)
      )
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 409 on copy method configuration from method repo to existing name" in withTestDataApiServices {
    services =>
      val existingMethodConfigCopy =
        MethodRepoConfigurationImport("workspace_test", "rawls_test_good", 1, testData.agoraMethodConfigName)
      Post(copyFromMethodRepo, httpJson(existingMethodConfigCopy)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Conflict) {
            status
          }
        }
  }

  it should "return 404 on copy method configuration from bogus source in method repo" in withTestDataApiServices {
    services =>
      Post(copyFromMethodRepo, httpJson(testData.methodRepoMissing)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 422 on copy method configuration when method repo payload is missing" in withTestDataApiServices {
    services =>
      Post(copyFromMethodRepo, httpJson(testData.methodRepoEmptyPayload)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.UnprocessableEntity) {
            status
          }
        }
  }

  it should "return 422 on copy method configuration when method repo payload is unparseable" in withTestDataApiServices {
    services =>
      withStatsD {
        Post(copyFromMethodRepo, httpJson(testData.methodRepoBadPayload)) ~>
          services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.UnprocessableEntity) {
              status
            }
          }
      } { capturedMetrics =>
        val wsPathForRequestMetrics = s"methodconfigs.copyFromMethodRepo"
        val expected =
          expectedHttpRequestMetrics("post", wsPathForRequestMetrics, StatusCodes.UnprocessableEntity.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }
  }

  it should "not allow copy method configuration from repo with library attributes in outputs by curator" in withTestDataApiServices {
    services =>
      Post(copyFromMethodRepo, httpJson(testData.methodRepoLibrary)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "not allow copy method configuration from repo with library attributes in outputs by non-curator" in withTestDataApiServices {
    services =>
      revokeCuratorRole(services)

      Post(copyFromMethodRepo, httpJson(testData.methodRepoLibrary)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 200 when generating a method config template from a valid Agora method" in withTestDataApiServices {
    services =>
      val method = AgoraMethod("dsde", "three_step", 1)
      Post("/methodconfigs/template", httpJson(method)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          val methodConfiguration = MethodConfiguration(
            "namespace",
            "name",
            Some("rootEntityType"),
            Some(Map()),
            Map("three_step.cgrep.pattern" -> AttributeString("")),
            Map("three_step.ps.procs" -> AttributeString(""),
                "three_step.cgrep.count" -> AttributeString(""),
                "three_step.wc.count" -> AttributeString("")
            ),
            AgoraMethod("dsde", "three_step", 1)
          )
          assertResult(methodConfiguration)(responseAs[MethodConfiguration])
          assertResult(StatusCodes.OK)(status)
        }
  }

  it should "return 200 when generating a method config template from a valid Dockstore method" in withTestDataApiServices {
    services =>
      val method: MethodRepoMethod = DockstoreMethod("dockstore-method-path", "dockstore-method-version")
      Post("/methodconfigs/template", httpJson(method)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          val methodConfiguration = MethodConfiguration(
            "namespace",
            "name",
            Some("rootEntityType"),
            Some(Map()),
            Map("three_step_dockstore.cgrep.pattern" -> AttributeString("")),
            Map(
              "three_step_dockstore.ps.procs" -> AttributeString(""),
              "three_step_dockstore.cgrep.count" -> AttributeString(""),
              "three_step_dockstore.wc.count" -> AttributeString("")
            ),
            method
          )
          assertResult(methodConfiguration)(responseAs[MethodConfiguration])
          assertResult(StatusCodes.OK)(status)
        }
  }

  it should "return 200 getting method inputs and outputs for an Agora method" in withTestDataApiServices { services =>
    withStatsD {
      val method = AgoraMethod("dsde", "three_step", 1)
      Post("/methodconfigs/inputsOutputs", httpJson(method)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.OK)(status)
          val expectedIn = Seq(MethodInput("three_step.cgrep.pattern", "String", false))
          val expectedOut = Seq(MethodOutput("three_step.ps.procs", "File"),
                                MethodOutput("three_step.cgrep.count", "Int"),
                                MethodOutput("three_step.wc.count", "Int")
          )
          val result = responseAs[MethodInputsOutputs]
          assertSameElements(expectedIn, result.inputs)
          assertSameElements(expectedOut, result.outputs)
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("get",
                                                "methods.redacted.redacted.redacted",
                                                StatusCodes.OK.intValue,
                                                1,
                                                Some(Subsystems.Agora)
      )
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 200 getting method inputs and outputs for a Dockstore method" in withTestDataApiServices {
    services =>
      val method: MethodRepoMethod = DockstoreMethod("dockstore-method-path", "dockstore-method-version")
      Post("/methodconfigs/inputsOutputs", httpJson(method)) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.OK)(status)
          val expectedIn = Seq(MethodInput("three_step_dockstore.cgrep.pattern", "String", false))
          val expectedOut = Seq(
            MethodOutput("three_step_dockstore.ps.procs", "File"),
            MethodOutput("three_step_dockstore.cgrep.count", "Int"),
            MethodOutput("three_step_dockstore.wc.count", "Int")
          )
          val result = responseAs[MethodInputsOutputs]
          assertSameElements(expectedIn, result.inputs)
          assertSameElements(expectedOut, result.outputs)
        }
  }

  it should "return 400 for a method inputs and outputs request missing method name" in withTestDataApiServices {
    services =>
      Post("/methodconfigs/inputsOutputs", httpJson(AgoraMethod("dsde", "", 2))) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.BadRequest)(status)
          val responseString = Unmarshal(response.entity).to[String].futureValue
          assert(responseString.contains("Invalid request."))
        }
  }

  it should "return 404 when generating a method config template from a missing method" in withTestDataApiServices {
    services =>
      Post("/methodconfigs/template", httpJson(AgoraMethod("dsde", "three_step", 2))) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound)(status)
        }
  }

  it should "return 404 getting method inputs and outputs from a missing method" in withTestDataApiServices {
    services =>
      Post("/methodconfigs/inputsOutputs", httpJson(AgoraMethod("dsde", "three_step", 2))) ~>
        sealRoute(services.methodConfigRoutes()) ~>
        check {
          assertResult(StatusCodes.NotFound)(status)
        }
  }

  it should "return 400 when generating a method config template from an invalid method" in withTestDataApiServices {
    services =>
      Post("/api/methodconfigs/template", httpJson(AgoraMethod("dsde", "bad_wdl", 1))) ~>
        sealRoute(services.route) ~>
        check {
          assertResult(StatusCodes.BadRequest)(status)
          val responseString = Unmarshal(response.entity).to[String].futureValue
          assert(
            responseString.contains(
              "ERROR: Finished parsing without consuming all tokens."
            )
          )
          assert(
            responseString.contains(
              "Bad syntax workflow returned from Agora mock server"
            )
          )
        }
  }

  it should "return 400 getting method inputs and outputs from an invalid method" in withTestDataApiServices {
    services =>
      Post("/api/methodconfigs/inputsOutputs", httpJson(AgoraMethod("dsde", "bad_wdl", 1))) ~>
        sealRoute(services.route) ~>
        check {
          assertResult(StatusCodes.BadRequest)(status)
          val responseString = Unmarshal(response.entity).to[String].futureValue
          assert(
            responseString.contains(
              "ERROR: Finished parsing without consuming all tokens."
            )
          )
          assert(
            responseString.contains(
              "Bad syntax workflow returned from Agora mock server"
            )
          )
        }
  }

  it should "return 200 on get method configuration" in withTestDataApiServices { services =>
    Get(testData.agoraMethodConfig.path(testData.workspace)) ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  private def getConfigs(includeDockstore: Boolean) = {
    val allConfigs = runAndWait(methodConfigurationQuery.listActive(testData.workspace)).toSet

    // Protect against test data changing and silently invalidating tests
    assertResult(true) {
      allConfigs.exists(_.methodRepoMethod.repo == Dockstore)
    }

    if (includeDockstore)
      allConfigs
    else
      allConfigs.filterNot(_.methodRepoMethod.repo == Dockstore)
  }

  it should "list method Configurations for Agora by default" in withTestDataApiServices { services =>
    Get(s"${testData.workspace.path}/methodconfigs") ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(getConfigs(includeDockstore = false)) {
          responseAs[Array[MethodConfigurationShort]].toSet
        }
      }
  }

  it should "list method Configurations for Agora with allRepos=false" in withTestDataApiServices { services =>
    Get(s"${testData.workspace.path}/methodconfigs?allRepos=false") ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(getConfigs(includeDockstore = false)) {
          responseAs[Array[MethodConfigurationShort]].toSet
        }
      }
  }

  it should "return Bad Request if you use a bogus value for allRepos" in withTestDataApiServices { services =>
    Get(s"${testData.workspace.path}/methodconfigs?allRepos=banana") ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "list method Configurations for all repos with allRepos=true" in withTestDataApiServices { services =>
    Get(s"${testData.workspace.path}/methodconfigs?allRepos=true") ~>
      sealRoute(services.methodConfigRoutes()) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(getConfigs(includeDockstore = true)) {
          responseAs[Array[MethodConfigurationShort]].toSet
        }
      }
  }
}
