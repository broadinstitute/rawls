package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.UserPool
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, MethodData, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.{AclEntry, Rawls, RestException, WorkspaceAccessLevel}
import org.scalatest.{FreeSpecLike, Matchers}
import org.broadinstitute.dsde.workbench.fixture._
import spray.json._
import DefaultJsonProtocol._
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.concurrent.Eventually


class MethodLaunchSpec extends TestKit(ActorSystem("MySpec")) with FreeSpecLike with Matchers with Eventually
  with BillingFixtures with WorkspaceFixtures with MethodFixtures {

  def createMethodConfigName: String = SimpleMethodConfig.configName + "_" + UUID.randomUUID().toString
  val operations = Array(Map("op" -> "AddUpdateAttribute", "attributeName" -> "participant1", "addUpdateAttribute" -> "testparticipant"))
  val entity: Array[Map[String, Any]] = Array(Map("name" -> "participant1", "entityType" -> "participant", "operations" -> operations))
  val sampleSetOperations = Array(Map("op" -> "CreateAttributeEntityReferenceList", "attributeListName" -> "participantSet"))
  val entitySet: Array[Map[String, Any]] = Array(Map("name" -> "participantSet1", "entityType" -> "participant_set", "operations" -> Array()))
  val entitySetMembershipOperation = Array(Map("op" -> "AddListMember", "attributeListName" -> "participantSetAttribute", "newMember" -> "participant1"))
  val entitySetMembership: Array[Map[String, Any]] = Array(Map("name" -> "participantSet1", "entityType" -> "participant_set", "operations" -> entitySetMembershipOperation))
  val inFlightSubmissionStatuses = List("Accepted", "Evaluating", "Submitting", "Submitted")

  "launching a workflow with input not defined should throw exception" in {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()
    withCleanBillingProject(user) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_launch_workflow_input_not_defined") { workspaceName =>
        Rawls.entities.importMetaData(billingProject, workspaceName, entity)

        withMethod("MethodLaunchSpec_input_undefined", MethodData.InputRequiredMethod, 1) { methodName =>
          val method = MethodData.InputRequiredMethod.copy(methodName = methodName)
          val methodConfigName = createMethodConfigName

          Rawls.methodConfigs.createMethodConfigInWorkspace(billingProject, workspaceName, method,
            method.methodNamespace, methodConfigName, 1, Map.empty, SimpleMethodConfig.outputs, method.rootEntityType)

          val exception = intercept[RestException](Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, methodConfigName, "participant",
          "participant1", "this", false))
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

        Rawls.entities.importMetaData(billingProject, workspaceName, entity)

        withMethod("MethodLaunchSpec_abort", MethodData.SimpleMethod) { methodName =>
          val method = MethodData.SimpleMethod.copy(methodName = methodName)

          Rawls.methodConfigs.createMethodConfigInWorkspace(billingProject, workspaceName,
            method, method.methodNamespace, method.methodName, 1,
            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)

          val submissionId = Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, method.methodName, method.rootEntityType, "participant1", "this", false)

          // make sure the submission has not errored out
          val submissionStatus = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)._1
          inFlightSubmissionStatuses should contain (submissionStatus)

          Rawls.submissions.abortSubmission(billingProject, workspaceName, submissionId)

          implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))

          eventually {
            val status = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)
            logger.info(s"Status is $status in Submission $billingProject/$workspaceName/$submissionId")
            withClue(s"Monitoring Submission $billingProject/$workspaceName/$submissionId. Waited for status Aborted.") {
              status._1 shouldBe "Aborted"
            }
          }
        }
      }
    }
  }

  "reader cannot abort a launched submission" in {
    val owner = UserPool.chooseProjectOwner
    val reader = UserPool.chooseStudent

    implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()
    val readerAuthToken: AuthToken = reader.makeAuthToken()

    withCleanBillingProject(owner) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_reader_cannot_abort_submission", aclEntries = List(AclEntry(reader.email, WorkspaceAccessLevel.Reader))) { workspaceName =>

        Rawls.entities.importMetaData(billingProject, workspaceName, entity)

        withMethod("MethodLaunchSpec_abort_reader", MethodData.SimpleMethod) { methodName =>
          val method = MethodData.SimpleMethod.copy(methodName = methodName)

          Rawls.methodConfigs.createMethodConfigInWorkspace(
            billingProject, workspaceName, method, method.methodNamespace, method.methodName, 1,
            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)

          val submissionId = Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, method.methodName, method.rootEntityType, "participant1", "this", false)(ownerAuthToken)

          val status = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)(readerAuthToken)

          withClue("When the reader views the owner's submission, the submission status: ") {
            inFlightSubmissionStatuses should contain (status._1)
          }

          val exception = intercept[RestException](Rawls.submissions.abortSubmission(billingProject, workspaceName, submissionId)(readerAuthToken))
          exception.message.parseJson.asJsObject.fields("message").convertTo[String].contains("insufficient permissions") shouldBe true

        }
      }
    }
  }

  "launch workflow with wrong root entity" in {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()

    withCleanBillingProject(user) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_launch_workflow_input_not_defined") { workspaceName =>

        Rawls.entities.importMetaData(billingProject, workspaceName, entity)

        withMethod("MethodLaunchSpec_input_undefined", MethodData.InputRequiredMethod, 1) { methodName =>
          val method = MethodData.InputRequiredMethod.copy(methodName = methodName)

          val methodConfigName = createMethodConfigName
          Rawls.methodConfigs.createMethodConfigInWorkspace(
            billingProject, workspaceName, method, method.methodNamespace, methodConfigName, 1,
            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, "sample")

          val exception = intercept[RestException](Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, methodConfigName, "participant",
            "participant1", "this", false))
          exception.message.parseJson.asJsObject.fields("message").convertTo[String].contains("The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type sample.)") shouldBe true
        }
      }
    }
  }


  "launch workflow on set with incorrect expression" in {
    val user = UserPool.chooseProjectOwner
    implicit val authToken: AuthToken = user.makeAuthToken()

    withCleanBillingProject(user) { billingProject =>
      withWorkspace(billingProject, "MethodLaunchSpec_launch_workflow_on_set_without_expression") { workspaceName =>

        Rawls.entities.importMetaData(billingProject, workspaceName, entity)
        Rawls.entities.importMetaData(billingProject, workspaceName, entitySet)
        Rawls.entities.importMetaData(billingProject, workspaceName, entitySetMembership)

        withMethod("MethodLaunchSpec_wf_on_set_without_expression", MethodData.SimpleMethod) { methodName =>
          val method = MethodData.SimpleMethod.copy(methodName = methodName)

          val methodConfigName = createMethodConfigName
          Rawls.methodConfigs.createMethodConfigInWorkspace(
            billingProject, workspaceName, method, method.methodNamespace, methodConfigName, 1,
            SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, method.rootEntityType)

          val exception = intercept[RestException](Rawls.submissions.launchWorkflow(billingProject, workspaceName, method.methodNamespace, methodConfigName, "participant_set",
            "participantSet1", "this", false))
          exception.message.parseJson.asJsObject.fields("message").convertTo[String].contains("The expression in your SubmissionRequest matched only entities of the wrong type") shouldBe true
        }
      }
    }
  }

}
