package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.scalatest.{FreeSpec, Matchers}
import spray.json.pimpString

class MethodConfigApiSpec extends FreeSpec with WorkspaceFixtures with LazyLogging with BillingFixtures with RandomUtil
  with MethodFixtures with Matchers {

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

          Rawls.methodConfigs.createMethodConfigInWorkspace(
            billingProject, sourceWorkspaceName, method, method.methodNamespace, method.methodName, 1,
            Map.empty, Map.empty, method.rootEntityType)

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
          Rawls.methodConfigs.copyMethodConfigFromWorkspace(sourceMethodConfig, destMethodConfig)

          // verify method config in destination workspace
          val response = Rawls.methodConfigs.getMethodConfigInWorkspace(billingProject, destWorkspaceName,
            destMethodNamespace, destMethodName)
          val parsedStr = response.parseJson.asJsObject.getFields("methodRepoMethod")
          parsedStr should not be empty

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
   * Then)  the user can import a method config from method repo in workspace
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

      // verify copy was successful
      val response = Rawls.methodConfigs.getMethodConfigInWorkspace(billingProject, workspaceName, namespace, name)
      val parsedStr = response.parseJson.asJsObject.getFields("methodRepoMethod")
      parsedStr should not be empty
    }
  }

}
