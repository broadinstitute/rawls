package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.{Method, MethodData, SimpleMethodConfig}
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}

object Submission extends LazyLogging with Eventually with RandomUtil {

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

    // pause a minute because cromwell isn't fast
    Thread.sleep(60 * 1000)

    // Orchestration.methods.redact(configNamespace, configName, SimpleMethodConfig.snapshotId)

    submissionId
  }

  private def getSubmissionStatus(billingProject: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): String = {
    val (status, workflows) = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)
    logger.info(s"Submission $billingProject/$workspaceName/$submissionId status is $status")
    status
  }

  def waitUntilSubmissionIsStatus(billingProjectName: String, workspaceName: String, submissionId: String, expectedStatus: String)(implicit token: AuthToken): Unit = {
    implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(20, Minutes)), interval = scaled(Span(30, Seconds)))
    // wait for submission status becomes expected status
    eventually {
      val actualStatus = getSubmissionStatus(billingProjectName, workspaceName, submissionId)
      withClue(s"Monitoring submission $billingProjectName/$workspaceName/$submissionId. Status shouldBe $expectedStatus") {
        actualStatus shouldEqual expectedStatus
      }
    }
  }

}
