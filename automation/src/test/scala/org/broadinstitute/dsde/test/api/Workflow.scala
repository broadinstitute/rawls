package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.{Method, MethodData, MethodFixtures, SimpleMethodConfig}
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.scalatest.Assertions
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}

object Workflow extends LazyLogging with Eventually with RandomUtil {

  def launchWorkflowOnSimpleMethod(billingProjectName: String, workspaceName: String)(implicit token: AuthToken): String = {

    val participantId = randomIdWithPrefix("participant")
    val participantEntity = s"entity:participant_id\n$participantId"
    val configNamespace = billingProjectName
    val configName = s"${SimpleMethodConfig.configName}_$workspaceName"
    val methodName: String = uuidWithPrefix(s"${workspaceName}_method")
    val simpleMethod: Method = MethodData.SimpleMethod

    Orchestration.importMetaData(billingProjectName, workspaceName, "entities", participantEntity)

    val entityType = SimpleMethodConfig.rootEntityType

    Orchestration.methodConfigurations.createMethodConfigInWorkspace(
      billingProjectName,
      workspaceName,
      simpleMethod,
      configNamespace,
      configName,
      SimpleMethodConfig.snapshotId,
      SimpleMethodConfig.inputs,
      SimpleMethodConfig.outputs,
      entityType)

    val submissionId = Rawls.submissions.launchWorkflow(
      billingProjectName,
      workspaceName,
      configNamespace,
      configName,
      entityType,
      participantId,
      "this",
      useCallCache = false)

    // pause a min for cromwell to start work its magic
    Thread.sleep(60 * 1000)

    // Orchestration.methods.redact(configNamespace, configName, SimpleMethodConfig.snapshotId)

    submissionId
  }

  private def getSubmissionStatus(billingProject: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): String = {
    val (status, workflows) = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)
    logger.info(s"Submission $billingProject/$workspaceName/$submissionId status is $status")
    status
  }

  def waitForSubmissionIsStatus(billingProjectName: String, workspaceName: String, submissionId: String, expectedStatus: String)(implicit token: AuthToken): Unit = {
    implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(20, Minutes)), interval = scaled(Span(30, Seconds)))
    // wait for submission to complete successfully
    eventually {
      val status = getSubmissionStatus(billingProjectName, workspaceName, submissionId)
      withClue(s"Monitoring Submission $billingProjectName/$workspaceName/$submissionId. Status shouldBe $expectedStatus") {
        status shouldEqual expectedStatus
      }
    }
  }

}
