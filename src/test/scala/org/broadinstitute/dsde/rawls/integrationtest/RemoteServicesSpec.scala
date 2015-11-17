package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.{AdminApiService, MethodConfigApiService, SubmissionApiService, WorkspaceApiService}
import spray.http.StatusCodes

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RemoteServicesSpec extends IntegrationTestBase with WorkspaceApiService with MethodConfigApiService with SubmissionApiService with AdminApiService {
  def actorRefFactory = system

  // setup workspace service
  val (workspaceServiceConstructor, userServiceConstructor, dataSource) = workspaceServiceWithDbName("integration-test-latest")

  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  val wsns = "broad-dsde-dev"
  val wsname = UUID.randomUUID().toString
  val methodConfig = MethodConfiguration(wsns, "testConfig", "samples", Map("ready" -> AttributeString("true")), Map("param1" -> AttributeString("foo")), Map("out" -> AttributeString("bar")), MethodRepoMethod("method-a", "dsde", 1))
  val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, WorkspaceName(wsns, wsname))

  val workspace = WorkspaceRequest(wsns, wsname, Map.empty)

  val uniqueMethodConfigName = UUID.randomUUID.toString
  val newMethodConfigName = MethodConfigurationName(uniqueMethodConfigName, methodConfig.namespace, WorkspaceName(wsns, wsname))
  val methodRepoGood = MethodRepoConfigurationImport("rawls_integration_testing", "rawls_test_good", 2, newMethodConfigName)
  val methodRepoMissing = MethodRepoConfigurationImport("rawls_integration_testing", "rawls_test_missing", 1, methodConfigName)
  val methodRepoEmptyPayload = MethodRepoConfigurationImport("rawls_integration_testing", "rawls_test_empty_payload", 1, methodConfigName)
  val methodRepoBadPayload = MethodRepoConfigurationImport("rawls_integration_testing", "rawls_test_bad_payload", 1, methodConfigName)

  val export = MethodRepoConfigurationExport("rawls_integration_testing", uniqueMethodConfigName, newMethodConfigName)

  val copyFromMethodRepoEndpoint = "/methodconfigs/copyFromMethodRepo"
  val copyToMethodRepoEndpoint = "/methodconfigs/copyToMethodRepo"

  "RemoteServicesSpec" should "copy a method config from the method repo" ignore {
    // need to init workspace
    dataSource.inTransaction() { txn =>
      containerDAO.authDAO.saveUser(RawlsUser(RawlsUserSubjectId("x"), RawlsUserEmail(gcsDAO.clientSecrets.getDetails.get("client_email").toString)), txn)
      containerDAO.billingDAO.saveProject(RawlsBillingProject(RawlsBillingProjectName(workspace.namespace), Set(RawlsUserRef(RawlsUserSubjectId("x")))), txn)
    }

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

    Post(copyToMethodRepoEndpoint, httpJson(export)) ~>
      addSecurityHeaders ~>
      sealRoute(methodConfigRoutes) ~>
      check {
        println(response.entity.asString)
        assertResult(StatusCodes.OK) {
          status
        }
      }


  }

}
