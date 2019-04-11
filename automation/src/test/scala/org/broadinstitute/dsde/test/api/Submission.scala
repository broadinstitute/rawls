package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.broadinstitute.dsde.workbench.service.Rawls
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}


object Submission extends LazyLogging with Eventually with RandomUtil {

  def getSubmissionStatus(billingProject: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): String = {
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
