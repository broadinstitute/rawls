package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.broadinstitute.dsde.workbench.service.Rawls
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}


object Submission extends LazyLogging with Eventually with RandomUtil {

  val ABORTED_STATUS  = "Aborted"
  val DONE_STATUS = "Done"

  private val SUBMISSION_COMPLETED_STATES = List(DONE_STATUS, ABORTED_STATUS)

  def isSubmissionDone(status: String): Boolean = {
    SUBMISSION_COMPLETED_STATES.contains(status)
  }

  /**
    * Important:
    *
    * in-progress submissions have a status of "Submitted". The successful path for a submission
    *  is Submitted -> Done; aborted submissions move Submitted -> Aborting -> Aborted.
    *
    * Workflows have a Running status, but submissions do not.
    *
    * see Rawls' SubmissionStatuses object for canonical submission status values.
    */
  def getSubmissionStatus(billingProject: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): String = {
    val (status, _) = Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)
    status
  }

  /*
   * wait for submission complete. max wait time is 20 minutes
   */
  def waitUntilSubmissionComplete(billingProjectName: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): Unit = {
    implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(20, Minutes)), interval = scaled(Span(30, Seconds)))

    eventually {
      val actualStatus = getSubmissionStatus(billingProjectName, workspaceName, submissionId)
      withClue(s"Monitoring submission $billingProjectName/$workspaceName/$submissionId until finish") {
        isSubmissionDone(actualStatus) shouldBe true
      }
    }
  }

}
