package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, MethodData, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.{Rawls, RestException}
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.{FreeSpecLike, Matchers}
import org.broadinstitute.dsde.workbench.fixture._

import scala.reflect.api
import spray.json._
import DefaultJsonProtocol._


class MethodLaunchSpec extends TestKit(ActorSystem("MySpec")) with FreeSpecLike with Matchers
  with CleanUp with RandomUtil with Retry
  with BillingFixtures with WorkspaceFixtures with MethodFixtures {

  val methodConfigName: String = SimpleMethodConfig.configName + "_" + UUID.randomUUID().toString

  "launch workflow with input not defined" - {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()
    withCleanBillingProject(user) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_launch_workflow_input_not_defined") { workspaceName =>
        val entity =
          s"""
             |[
             |  {
             |   "name": "participant1","entityType": "participant",
             |   "operations":
             |     [
             |       {
             |         "op": "AddUpdateAttribute",
             |         "attributeName": "partipant1",
             |         "addUpdateAttribute": "testparticipant"
             |       }
             |     ]
             |  }
             |]""".stripMargin
        println("test entity: " + entity.parseJson.toString)
        Rawls.entities.importMetaData(billingProject, workspaceName, entity)
        withMethod("MethodLaunchSpec_input_undefined", MethodData.InputRequiredMethod, 1) { methodName =>
          val method = MethodData.InputRequiredMethod.copy(methodName = methodName)
          Rawls.methodConfigs.createMethodConfigInWorkspace(billingProject, workspaceName, method,
            method.methodNamespace, methodConfigName, 1, Map.empty, Map.empty, method.rootEntityType)
          val exception = intercept[RestException](Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, methodConfigName, "participant",
          "participant1", "this", false))
          println("launch ws exception + " + exception.message.toString)
          exception.message.parseJson.asJsObject.fields("message").convertTo[String] should contain ("Missing inputs:")
        }
      }
    }
  }

}
