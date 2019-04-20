package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, MethodData, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.{AclEntry, Rawls, RestException, WorkspaceAccessLevel}
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.{FreeSpecLike, Matchers}
import org.broadinstitute.dsde.workbench.fixture._
import spray.json._
import DefaultJsonProtocol._
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.concurrent.Eventually


class MethodLaunchSpec extends TestKit(ActorSystem("MySpec")) with FreeSpecLike with Matchers
  with CleanUp with RandomUtil with Retry with Eventually
  with BillingFixtures with WorkspaceFixtures with MethodFixtures {

  val methodConfigName: String = SimpleMethodConfig.configName + "_" + UUID.randomUUID().toString
  val operations = Array(Map("op" -> "AddUpdateAttribute", "attributeName" -> "participant1", "addUpdateAttribute" -> "testparticipant"))
  val entity: Array[Map[String, Any]] = Array(Map("name" -> "participant1", "entityType" -> "participant", "operations" -> operations))

  "launch workflow with input not defined" in {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()
    withCleanBillingProject(user) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_launch_workflow_input_not_defined") { workspaceName =>
        Rawls.entities.importMetaData(billingProject, workspaceName, entity)

        withMethod("MethodLaunchSpec_input_undefined", MethodData.InputRequiredMethod, 1) { methodName =>
          val method = MethodData.InputRequiredMethod.copy(methodName = methodName)
          Rawls.methodConfigs.createMethodConfigInWorkspace(billingProject, workspaceName, method,
            method.methodNamespace, methodConfigName, 1, Map.empty, Map.empty, method.rootEntityType)
          val exception = intercept[RestException](Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, methodConfigName, "participant",
          "participant1", "this", false))
          println("launch ws exception + " + exception.message.toString)
          exception.message.parseJson.asJsObject.fields("message").convertTo[String].contains("Missing inputs:") shouldBe true
        }
      }
    }
  }


  "owner can abort a launched submission" in {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()
    withCleanBillingProject(user) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_abort_submission") { workspaceName =>
        //  api.workspaces.waitForBucketReadAccess(billingProject, workspaceName)

        val shouldUseCallCaching = false
        Rawls.entities.importMetaData(billingProject, workspaceName, entity)

        withMethod("MethodLaunchSpec_abort", MethodData.SimpleMethod) { methodName =>
          val method = MethodData.SimpleMethod.copy(methodName = methodName)
          Rawls.methodConfigs.createMethodConfigInWorkspace(billingProject, workspaceName,
            method, method.methodNamespace, method.methodName, 1,
            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)

          //          api.methodConfigurations.createMethodConfigInWorkspace(billingProject, workspaceName,
          //            method, SimpleMethodConfig.configNamespace, methodName, 1,
          //            SimpleMethodConfig.inputs,  SimpleMethodConfig.inputs, MethodData.SimpleMethod.rootEntityType)

          val getmc = Rawls.methodConfigs.getMethodConfigInWorkspace(billingProject, workspaceName, method.methodNamespace, method.methodName)
          val validatemc = Rawls.methodConfigs.getMethodConfigSyntaxValidationInWorkspace(billingProject, workspaceName, method.methodNamespace, method.methodName)

          println("getmc: " + getmc)
          println("validatemc: " + validatemc)

          val submissionId = Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, method.methodName, method.rootEntityType, "participant1", "this", false)

          //val submissionDetailsPage = methodConfigDetailsPage.launchAnalysis(MethodData.SimpleMethod.rootEntityType, testData.participantId, "", shouldUseCallCaching)

          Rawls.submissions.abortSubmission(billingProject, workspaceName, submissionId)

          // val submissionId = submissionDetailsPage.getSubmissionId

          //submissionDetailsPage.abortSubmission()

          // Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)

          implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))

          // wait api status becomes Aborted
          eventually {
            // val status = submissionDetailsPage.getApiSubmissionStatus(billingProject, workspaceName, submissionId)
            val status = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)
            logger.info(s"Status is $status in Submission $billingProject/$workspaceName/$submissionId")
            withClue(s"Monitoring Submission $billingProject/$workspaceName/$submissionId. Waited for status Aborted.") {
              status shouldBe "Aborted"
            }
          }

          //          // verifiy on UI
          //          submissionDetailsPage.waitUntilSubmissionCompletes()
          //          submissionDetailsPage.getSubmissionStatus shouldBe submissionDetailsPage.ABORTED_STATUS
          //
          //          // once aborted, the abort button should no longer be visible
          //          submissionDetailsPage should not be 'abortButtonVisible

        }
      }
    }
  }

//  "reader does not see an abort button for a launched submission" in {
//    val owner = UserPool.chooseProjectOwner
//    val reader = UserPool.chooseStudent
//
//    implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()
//
//    val readerAuthToken: AuthToken = reader.makeAuthToken()
//
//    withCleanBillingProject(owner) { billingProject =>
//      withWorkspace(billingProject, "MethodLaunchSpec_reader_cannot_abort_submission", aclEntries = List(AclEntry(reader.email, WorkspaceAccessLevel.Reader))) { workspaceName =>
////        api.workspaces.waitForBucketReadAccess(billingProject, workspaceName)(ownerAuthToken)
////        api.workspaces.waitForBucketReadAccess(billingProject, workspaceName)(readerAuthToken)
//
//        val shouldUseCallCaching = false
//        //api.importMetaData(billingProject, workspaceName, "entities", testData.participantEntity)
//        Rawls.entities.importMetaData(billingProject, workspaceName, entity)
//
//        withMethod("MethodLaunchSpec_abort_reader", MethodData.SimpleMethod) { methodName =>
//          val method = MethodData.SimpleMethod.copy(methodName = methodName)
//          //          api.methodConfigurations.createMethodConfigInWorkspace(billingProject, workspaceName,
//          //            method, SimpleMethodConfig.configNamespace, methodName, 1,
//          //            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, MethodData.SimpleMethod.rootEntityType)
//          Rawls.methodConfigs.createMethodConfigInWorkspace(
//            billingProject, workspaceName, method, method.methodNamespace, method.methodName, 1,
//            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)
//
//
//          // TODO: avoid using var?
//          var submissionId: String = ""
//
//          // as owner, launch a submission
//          withSignIn(owner) { _ =>
//            val methodConfigDetailsPage = new WorkspaceMethodConfigDetailsPage(billingProject, workspaceName, SimpleMethodConfig.configNamespace, methodName).open
//            val submissionDetailsPage = methodConfigDetailsPage.launchAnalysis(MethodData.SimpleMethod.rootEntityType, testData.participantId, "", shouldUseCallCaching)
//            submissionId = submissionDetailsPage.getSubmissionId
//          }
//
//          // as reader, view submission details and validate the abort button doesn't appear
//          withSignIn(reader) { _ =>
//            withClue("submissionId as returned by owner block: ") {
//              submissionId should not be ""
//            }
//            val submissionDetailsPage = new SubmissionDetailsPage(billingProject, workspaceName, submissionId).open
//            val status = submissionDetailsPage.getApiSubmissionStatus(billingProject, workspaceName, submissionId)(readerAuthToken)
//            withClue("When the reader views the owner's submission, the submission status: ") {
//              // the UI shows the abort button for the following statuses:
//              List("Accepted", "Evaluating", "Submitting", "Submitted") should contain(status)
//            }
//
//            // test the page's display of the submission ID against the tests' knowledge of the ID. This verifies
//            // we have landed on the right page. Without this test, the next assertion on the abort button's visibility
//            // is a weak assertion - we could be on the wrong page, which means the abort button would also not be visible.
//            submissionDetailsPage.getSubmissionId shouldBe submissionId
//
//            // is the abort button visible?
//            submissionDetailsPage should not be 'abortButtonVisible
//          }
//
//        }
//      }
//    }
//  }

}
