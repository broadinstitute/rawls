package org.broadinstitute.dsde.test.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.scalatest.FreeSpec

class MethodConfigApiSpec extends FreeSpec with WorkspaceFixtures with LazyLogging with BillingFixtures with RandomUtil
  with MethodFixtures {

  /*
   * This test does
   *
   * Given) a clean billing project and a registered user
   * When)  the user can create two new workspaces
   * and)   the user can create new method config in one workspace
   * Then)  the user can copy the method config from one workspace to another workspace
   *
   */
  "import method config" - {
    "copy from a workspace" in {

      val user = UserPool.chooseProjectOwner
      implicit val authToken: AuthToken = user.makeAuthToken()

      withCleanBillingProject(user) { billingProject =>

        val sourceWorkspaceName = uuidWithPrefix("workspaceSrc")
        Rawls.workspaces.create(billingProject, sourceWorkspaceName);
        register cleanUp Orchestration.workspaces.delete(billingProject, sourceWorkspaceName)

        val destWorkspaceName = uuidWithPrefix("workspaceDest")
        Rawls.workspaces.create(billingProject, destWorkspaceName);
        register cleanUp Orchestration.workspaces.delete(billingProject, destWorkspaceName)

        withMethod("MethodConfigApiSpec_from_workspace", MethodData.SimpleMethod, 1) { methodName =>
          val method = MethodData.SimpleMethod.copy(methodName = methodName)
          Rawls.methodconfigs.createMethodConfigInWorkspace(
            billingProject, sourceWorkspaceName, method, method.methodNamespace, method.methodName, 1, Map.empty, Map.empty, method.rootEntityType)

          Orchestration.workspaces.waitForBucketReadAccess(billingProject, destWorkspaceName)

          val sourceMethodConfig = Map(
            "name" -> method.methodName,
            "namespace" -> method.methodNamespace,
            "workspaceName" -> Map(
              "namespace" -> billingProject,
              "name" -> sourceWorkspaceName))

          val destMethodName: String = uuidWithPrefix(method.methodName)
          val destMethodNamespace: String = uuidWithPrefix(method.methodNamespace)

          val destMethodConfig = Map(
            "name" -> destMethodName,
            "namespace" -> destMethodNamespace,
            "workspaceName" -> Map(
              "namespace" -> billingProject,
              "name" -> destWorkspaceName)
          )

          // copy method config from source workspace to destination workspace
          Rawls.methodconfigs.copyMethodConfigFromWorkspace(sourceMethodConfig, destMethodConfig)

          // verify method config in destination workspace
          val response: HttpResponse = Rawls.methodconfigs.getMethodConfigInWorkspace(billingProject, destWorkspaceName, destMethodNamespace, destMethodName)
          assertResult(StatusCodes.OK) {
            response.status
          }
        }

      }

    }
  }

  /*
   * This test does
   *
   * Given) a clean billing project and a registered user
   * When)  the user can create a new workspace
   * and)   the user can create a method in workspace
   * Then)  the user can import the method config from method repo in workspace
   *
   */
  "import method config from method repo" in {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()
    withCleanBillingProject(user) { billingProject =>

      val workspaceName = uuidWithPrefix("MethodConfigApiSpec_workspace")
      Rawls.workspaces.create(billingProject, workspaceName);
      register cleanUp Orchestration.workspaces.delete(billingProject, workspaceName)

      val name = uuidWithPrefix("MethodConfigApiSpec_Method")
      val namespace = MethodData.SimpleMethod.creationAttributes.get("namespace").head + randomUuid
      val attributes = MethodData.SimpleMethod.creationAttributes ++ Map("name" -> name, "namespace" -> namespace)

      Orchestration.methods.createMethod(attributes)
      register cleanUp Orchestration.methods.redact(namespace, name, SimpleMethodConfig.snapshotId)

      //val methodNameCopy = uuidWithPrefix("MethodConfigApiSpec_CopyMethodName")
      //val methodNamespaceCopy = MethodData.SimpleMethod.creationAttributes.get("namespace").head + randomUuid

      val request = Map(
        "methodRepoNamespace" -> SimpleMethodConfig.configNamespace,
        "methodRepoName" -> SimpleMethodConfig.configName,
        "methodRepoSnapshotId" -> SimpleMethodConfig.snapshotId,
        "destination" -> Map(
          "name" -> name,
          "namespace" -> namespace,
          "workspaceName" -> Map(
            "namespace" -> billingProject,
            "name" -> workspaceName
          )
        )
      )

      Rawls.methodconfigs.copyMethodConfigFromMethodRepo(request)

      // 19:56:05.755 [ScalaTest-run-running-MethodConfigApiSpec] INFO org.broadinstitute.dsde.workbench.service.Rawls$ - Getting method configuration automationmethods3c8e4fa9-b938-47a1-8522-418dddbd2db5/MethodConfigApiSpec_CopyMethodName_04d496bc-b717-407d-a001-0b1acd3e2a26 for workspace gpalloc-dev-master-0uzkc4s/MethodConfigApiSpec_workspace_1f3b666d-0fe9-4ec7-9dfd-614052edfad9
      // 19:56:05.853 [default-akka.actor.default-dispatcher-4] INFO org.broadinstitute.dsde.workbench.service.Rawls$ - API request: HttpRequest(HttpMethod(GET),https://rawls-fiab.dsde-dev.broadinstitute.org:24443/api/workspaces/gpalloc-dev-master-0uzkc4s/MethodConfigApiSpec_workspace_1f3b666d-0fe9-4ec7-9dfd-614052edfad9/methodconfigs/automationmethods3c8e4fa9-b938-47a1-8522-418dddbd2db5/MethodConfigApiSpec_CopyMethodName_04d496bc-b717-407d-a001-0b1acd3e2a26,List(Authorization: Bearer ya29.GoEB5gbFkaUtoxICzZ_KcXTcRmGGEXgNuquGU3tW0kTqXXQpHGY7dds7kIIMmjf5BJP580KO8GZye4nJiZpZIPjVANC_YEvl01UxNfYouuCZS01KnRhAmYdSphhNKzTZGhu1Luh2-d3XXUa-WEBdZluDLCdXIuLN4j_QOXuM_azfban8),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
      // API response: HttpResponse(404 Not Found,List(Date: Mon, 08 Apr 2019 23:56:05 GMT, Server: akka-http/10.1.7, X-Frame-Options: SAMEORIGIN, Access-Control-Allow-Origin: *, Access-Control-Allow-Headers: authorization, content-type, accept, origin, x-app-id, Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD, Access-Control-Max-Age: 1728000),HttpEntity.Strict(application/json,{"causes":[],"message":"automationmethods3c8e4fa9-b938-47a1-8522-418dddbd2db5/MethodConfigApiSpec_CopyMethodName_04d496bc-b717-407d-a001-0b1acd3e2a26 does not exist in SlickWorkspaceContext(Workspace(gpalloc-dev-master-0uzkc4s,MethodConfigApiSpec_workspace_1f3b666d-0fe9-4ec7-9dfd-614052edfad9,b63ea63d-d520-47b7-9327-11aafa16cdd3,fc-b63ea63d-d520-47b7-9327-11aafa16cdd3,Some(b63ea63d-d520-47b7-9327-11aafa16cdd3),2019-04-08T23:56:01.009Z,2019-04-08T23:56:05.663Z,hermione.owner@test.firecloud.org,Map(),false))","source":"rawls","stackTrace":[],"statusCode":404}),HttpProtocol(HTTP/1.1))
      // Expected 200 OK, but got 404 Not Found

      // verify copy was successful
      val response: HttpResponse = Rawls.methodconfigs.getMethodConfigInWorkspace(billingProject, workspaceName, namespace, name)
      assertResult(StatusCodes.OK) {
        response.status
      }
    }
  }

}
