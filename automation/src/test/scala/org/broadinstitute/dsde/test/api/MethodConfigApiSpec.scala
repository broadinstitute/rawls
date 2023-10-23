package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.billingScopes
import org.broadinstitute.dsde.workbench.config.{ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryBillingProject
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsValue, JsonParser}

@MethodsTest
class MethodConfigApiSpec
    extends AnyFreeSpec
    with WorkspaceFixtures
    with LazyLogging
    with RandomUtil
    with MethodFixtures
    with Matchers
    with CleanUp {

  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  "import method config" - {
    /*
     * This test does
     *
     * Given) a registered user
     * When) the user is authenticated with access token
     * Then) the user can get a clean billing project
     * and)  the user can create a new workspace
     * and)  the user can create a method in workspace
     * and)  the user can import a method config from method repo in workspace
     *
     */
    "copy from method repo" in {
      val user = UserPool.chooseProjectOwner
      implicit val authToken: AuthToken = user.makeAuthToken()

      withTemporaryBillingProject(billingAccountId) { billingProject =>
        withCleanUp {

          val workspaceName = uuidWithPrefix("MethodConfigApiSpec_importMethodConfigFromMethodRepoWorkspace")
          Rawls.workspaces.create(billingProject, workspaceName);
          register cleanUp Orchestration.workspaces.delete(billingProject, workspaceName)

          val name = uuidWithPrefix("MethodConfigApiSpec_method")
          val namespace = MethodData.SimpleMethod.creationAttributes.get("namespace").head + randomUuid
          val attributes = MethodData.SimpleMethod.creationAttributes ++ Map("name" -> name, "namespace" -> namespace)

          Orchestration.methods.createMethod(attributes)
          register cleanUp Orchestration.methods.redact(namespace, name, SimpleMethodConfig.snapshotId)

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

          Rawls.methodConfigs.copyMethodConfigFromMethodRepo(request)

          // verify copied method config is in workspace
          assertMethodConfigInWorkspace(billingProject, workspaceName, namespace, name)
        }
      }(user.makeAuthToken(billingScopes))
    }

  }

  private def assertMethodConfigInWorkspace(billingProject: String,
                                            workspaceName: String,
                                            namespace: String,
                                            name: String
  )(implicit token: AuthToken): Unit = {
    val response = Rawls.methodConfigs.getMethodConfigInWorkspace(billingProject, workspaceName, namespace, name)
    val json: JsValue = JsonParser(response)
    val field: JsValue = json.asJsObject.fields("methodRepoMethod")
    assert(field.toString().contains(""""sourceRepo":"agora""""))
  }

}
