package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.service.Rawls
import org.broadinstitute.dsde.workbench.service.test.RandomUtil
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Minutes, Seconds, Span}


object Submission extends LazyLogging with Eventually with RandomUtil {

  val ABORTED_STATUS  = "Aborted"
  val DONE_STATUS = "Done"

  private val SUBMISSION_COMPLETED_STATES = List(DONE_STATUS, ABORTED_STATUS)

  // N.B. currently not used anywhere
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
    val (status, _) = getSubmissionStatusAndWorkflowIds(billingProject, workspaceName, submissionId)
    status
  }

  def getSubmissionStatusAndWorkflowIds(billingProject: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): (String, List[String]) = {
    Rawls.submissions.getSubmissionStatus(billingProject, workspaceName, submissionId)
  }

  /*
   * wait for submission complete. max wait time is 20 minutes
   */
  def waitUntilSubmissionComplete(billingProjectName: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): Unit = {
    implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(20, Minutes)), interval = scaled(Span(30, Seconds)))

    eventually {
      val (actualStatus, workflowIds) = getSubmissionStatusAndWorkflowIds(billingProjectName, workspaceName, submissionId)
      withClue(s"Monitoring submission [$billingProjectName/$workspaceName/$submissionId] until finish; " +
                     s"actual status was [$actualStatus] with first 10 workflow ids [${workflowIds.take(10).mkString(", ")}]. " +
                     s"Scalatest-generated error message is:") {
        SUBMISSION_COMPLETED_STATES should contain (actualStatus)
      }
    }
  }

}
