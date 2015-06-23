package org.broadinstitute.dsde.rawls

import java.util.UUID

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.{WorkspaceApiService, MethodConfigApiService, JobApiService}
import org.joda.time.DateTime
import spray.http.StatusCodes
import scala.concurrent.duration._

import WorkspaceJsonSupport._

class RemoteServicesSpec extends IntegrationTestBase with WorkspaceApiService with MethodConfigApiService with JobApiService {
  def actorRefFactory = system

  // setup workspace service
  val workspaceServiceConstructor = workspaceServiceWithDbName("remote-services-latest") // TODO move this into config?

  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  "RemoteServicesSpec" should "copy a method config from the method repo" in {
    // TODO make a common test space? copied these from other tests

    val wsns = "namespace"
    val wsname = UUID.randomUUID().toString
    val methodConfig = MethodConfiguration("testConfig", "samples", wsns, "method-a", "1", Map("ready" -> "true"), Map("param1" -> "foo"), Map("out" -> "bar"), WorkspaceName(wsns, wsname), "dsde")
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, methodConfig.workspaceName)

    val workspace = Workspace(
      wsns,
      wsname,
      DateTime.now().withMillis(0),
      "test",
      Map.empty
    )

    val uniqueMethodConfigName = UUID.randomUUID.toString
    val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, methodConfig.workspaceName)
    val methodRepoGood = MethodRepoConfigurationQuery("workspace_test", "rawls_test_good", "1", newMethodConfigName)
    val methodRepoMissing = MethodRepoConfigurationQuery("workspace_test", "rawls_test_missing", "1", methodConfigName)
    val methodRepoEmptyPayload = MethodRepoConfigurationQuery("workspace_test", "rawls_test_empty_payload", "1", methodConfigName)
    val methodRepoBadPayload = MethodRepoConfigurationQuery("workspace_test", "rawls_test_bad_payload", "1", methodConfigName)

    // need to init workspace
    Post(s"/workspaces", httpJson(workspace)) ~>
      addOpenAmCookie ~>
      sealRoute(postWorkspaceRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val endpoint = "/methodconfigs/copyFromMethodRepo"

    Post(endpoint, httpJson(methodRepoGood)) ~>
      addOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(endpoint, httpJson(methodRepoGood)) ~>
      addOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }

    Post(endpoint, httpJson(methodRepoMissing)) ~>
      addOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }

    Post(endpoint, httpJson(methodRepoEmptyPayload)) ~>
      addOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }

    Post(endpoint, httpJson(methodRepoBadPayload)) ~>
      addOpenAmCookie ~>
      sealRoute(copyMethodRepoConfigurationRoute) ~>
      check {
        assertResult(StatusCodes.UnprocessableEntity) {
          status
        }
      }

  }

}
