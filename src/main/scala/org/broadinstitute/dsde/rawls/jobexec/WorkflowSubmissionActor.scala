package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses
import org.broadinstitute.dsde.rawls.util.FutureSupport
import slick.dbio.DBIOAction

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration


object WorkflowSubmissionActor {
  def props(datasource: SlickDataSource,
            batchSize: Int,
            credential: Credential,
            pollInterval: FiniteDuration): Props = {
    Props(new WorkflowSubmissionActor(datasource, batchSize, credential, pollInterval))
  }

  sealed trait WorkflowSubmissionMessage
  case object LookForWorkflows extends WorkflowSubmissionMessage
  case class SubmitWorkflowBatch(workflowIds: Seq[Long]) extends WorkflowSubmissionMessage

}

class WorkflowSubmissionActor(val datasource: SlickDataSource,
                              val batchSize: Int,
                              val credential: Credential,
                              val pollInterval: FiniteDuration) extends Actor with WorkflowSubmission with LazyLogging {

  import context._

  scheduleNextWorkflowQuery

  override def receive = {
    case LookForWorkflows =>
      getUnlaunchedWorkflowBatch() map {
        case Some(msg) => self ! msg
        case None => scheduleNextWorkflowQuery
      }
    case SubmitWorkflowBatch(workflowIds) =>
      self ! submitWorkflowBatch(workflowIds)

    case Status.Failure(t) => throw t // an error happened in some future, let the supervisor handle it
  }

  def scheduleNextWorkflowQuery: Cancellable = {
    system.scheduler.scheduleOnce(pollInterval, self, LookForWorkflows)
  }
}

trait WorkflowSubmission extends FutureSupport with LazyLogging {
  val datasource: SlickDataSource
  val batchSize: Int
  val credential: Credential
  val pollInterval: FiniteDuration

  import datasource.dataAccess.driver.api._

  def getUnlaunchedWorkflowBatch() (implicit executionContext: ExecutionContext): Future[Option[WorkflowSubmissionMessage]] = {
    val unlaunchedWfOptF = datasource.inTransaction { dataAccess =>
      //grab a bunch of unsubmitted workflows
      dataAccess.workflowQuery.findUnsubmittedWorkflows().take(batchSize).result map { wfRecs =>
        if(wfRecs.nonEmpty) wfRecs
        else {
          //they should also all have the same submission ID
          val wfsWithASingleSubmission = wfRecs.filter( _.submissionId == wfRecs.head.submissionId )
          //instead: on update, if we don't get the right number back, roll back the txn
          dataAccess.workflowQuery.batchUpdateStatus(wfsWithASingleSubmission.map(_.id), WorkflowStatuses.Launching)
          wfsWithASingleSubmission
        }
      }
    }

    //if we find any, next step is to submit them. otherwise, look again.
    unlaunchedWfOptF.map {
      case workflowRecs if workflowRecs.nonEmpty => Some(SubmitWorkflowBatch(workflowRecs.map(_.id)))
      case _ => None
    }
  }

  def submitWorkflowBatch(workflowIds: Seq[Long]): WorkflowSubmissionMessage = {
    //TODO: execution service dao submitWorkflows only takes a single wdl. is this right?
    LookForWorkflows
  }
}
