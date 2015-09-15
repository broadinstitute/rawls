package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID

import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.{MethodConfigApiService, SubmissionApiService, WorkspaceApiService}
import spray.http.StatusCodes

import scala.concurrent.duration._

class RemoteServicesSpec extends IntegrationTestBase with WorkspaceApiService with MethodConfigApiService with SubmissionApiService {
  def actorRefFactory = system

  // setup workspace service
  val workspaceServiceConstructor = workspaceServiceWithDbName("remote-services-latest")

  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  val wsns = "namespace"
  val wsname = UUID.randomUUID().toString
  val methodConfig = MethodConfiguration(wsns, "testConfig", "samples", Map("ready" -> AttributeString("true")), Map("param1" -> AttributeString("foo")), Map("out" -> AttributeString("bar")), MethodRepoConfiguration("method-a-config", "dsde", 1), MethodRepoMethod("method-a", "dsde", 1))
  val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, WorkspaceName(wsns, wsname))

  val workspace = WorkspaceRequest(wsns, wsname, Map.empty)

  val uniqueMethodConfigName = UUID.randomUUID.toString
  val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, WorkspaceName(wsns, wsname))
  val methodRepoGood = MethodRepoConfigurationQuery("rawls_integration_testing", "rawls_test_good", 2, newMethodConfigName)
  val methodRepoMissing = MethodRepoConfigurationQuery("rawls_integration_testing", "rawls_test_missing", 1, methodConfigName)
  val methodRepoEmptyPayload = MethodRepoConfigurationQuery("rawls_integration_testing", "rawls_test_empty_payload", 1, methodConfigName)
  val methodRepoBadPayload = MethodRepoConfigurationQuery("rawls_integration_testing", "rawls_test_bad_payload", 1, methodConfigName)

  val copyFromMethodRepoEndpoint = "/methodconfigs/copyFromMethodRepo"

  "RemoteServicesSpec" should "copy a method config from the method repo" ignore {
    // need to init workspace
    Post(s"/workspaces", httpJson(workspace)) ~>
      addSecurityHeaders ~>
      sealRoute(workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(copyFromMethodRepoEndpoint, httpJson(methodRepoGood)) ~>
      addSecurityHeaders ~>
      sealRoute(methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(copyFromMethodRepoEndpoint, httpJson(methodRepoGood)) ~>
      addSecurityHeaders ~>
      sealRoute(methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }

    Post(copyFromMethodRepoEndpoint, httpJson(methodRepoMissing)) ~>
      addSecurityHeaders ~>
      sealRoute(methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }

    Post(copyFromMethodRepoEndpoint, httpJson(methodRepoEmptyPayload)) ~>
      addSecurityHeaders ~>
      sealRoute(methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }

    Post(copyFromMethodRepoEndpoint, httpJson(methodRepoBadPayload)) ~>
      addSecurityHeaders ~>
      sealRoute(methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }

  }

}
