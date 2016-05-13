package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickWorkspaceContext, ExecutionServiceDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkspaceRecord, DataAccess, WorkflowRecord}
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.FutureSupport
import slick.dbio.DBIOAction
import spray.http.OAuth2BearerToken

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration


object WorkflowSubmissionActor {
  def props(datasource: SlickDataSource,
            googleServicesDAO: GoogleServicesDAO,
            executionServiceDAO: ExecutionServiceDAO,
            batchSize: Int,
            credential: Credential,
            pollInterval: FiniteDuration): Props = {
    Props(new WorkflowSubmissionActor(datasource, googleServicesDAO, executionServiceDAO, batchSize, credential, pollInterval))
  }

  sealed trait WorkflowSubmissionMessage
  case object LookForWorkflows extends WorkflowSubmissionMessage
  case class SubmitWorkflowBatch(workflowIds: Seq[Long]) extends WorkflowSubmissionMessage

}

class WorkflowSubmissionActor(val datasource: SlickDataSource,
                              val googleServicesDAO: GoogleServicesDAO,
                              val executionServiceDAO: ExecutionServiceDAO,
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
  val googleServicesDAO: GoogleServicesDAO
  val executionServiceDAO: ExecutionServiceDAO
  val batchSize: Int
  val credential: Credential
  val pollInterval: FiniteDuration

  import datasource.dataAccess.driver.api._

  def getUnlaunchedWorkflowBatch()(implicit executionContext: ExecutionContext): Future[Option[WorkflowSubmissionMessage]] = {
    val unlaunchedWfOptF = datasource.inTransaction { dataAccess =>
      //grab a bunch of unsubmitted workflows
      dataAccess.workflowQuery.findUnsubmittedWorkflows().take(batchSize).result map { wfRecs =>
        if (wfRecs.nonEmpty) wfRecs
        else {
          //they should also all have the same submission ID
          val wfsWithASingleSubmission = wfRecs.filter(_.submissionId == wfRecs.head.submissionId)
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

  def loadWorkflows(wfRecs: Seq[WorkflowRecord]): Future[Seq[Option[Workflow]]] = {
    datasource.inTransaction { dataAccess =>
      DBIO.sequence( wfRecs.map(dataAccess.workflowQuery.loadWorkflow) )
    }
  }

  def submitWorkflowBatch(workflowIds: Seq[Long]): WorkflowSubmissionMessage = {


    val wfRecsFuture: Future[Seq[WorkflowRecord]] = datasource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result
    }

    val submitThingsFuture = for {
      wfRecs <- wfRecsFuture
      assert(wfRecs.forall( _.submissionId == wfRecs.head.submissionId))
      execSvcWfOpts <- buildWorkflowOptions(wfRecs.head.submissionId)
      unpackedWfs <- loadWorkflows(wfRecs)
      assert(unpackedWfs.forall(_.isDefined))
      toSubmit = unpackedWfs.map(_.get)
    } yield {
      (execSvcWfOpts, toSubmit)
    }

    submitThingsFuture map {
      case (wfOpts: ExecutionServiceWorkflowOptions, unpackedWfs: Seq[Workflow]) =>
        val wfInputsBatch = unpackedWfs map { wf =>
          val methodProps = wf.inputResolutions map {
            case svv: SubmissionValidationValue if svv.value.isDefined =>
              svv.inputName -> svv.value.get
          }
          MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
        }
        //why is toJson going red? it's right in this import...
        import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat
        executionServiceDAO.submitWorkflows("WHERE'S THE WDL?", wfInputsBatch, Option(wfOpts.toJson.toString), getUserInfo(credential))

        //TODO: if cromwell submission fails, set workflow to Failed rather than WorkflowFailure


    }
    LookForWorkflows
  }

  //Returns a tuple of some weird things we need later.
  def getSomeThings(submissionId: UUID): Future[(Option[RawlsUser], Option[WorkspaceRecord], RawlsBillingProject)] = {
    datasource.inTransaction { dataAccess =>
      for {
        subRec <- dataAccess.submissionQuery.findById(submissionId).result
        userOpt <- dataAccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(subRec.head.submitterId)))
        wsOpt <- dataAccess.workspaceQuery.findByIdQuery(subRec.head.workspaceId).result
        workspace <- wsOpt
        billingProject <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspace.namespace))
      } yield {
        (userOpt, wsOpt.headOption, billingProject)
      }
    }
  }

  def buildWorkflowOptions(submissionId: UUID): Future[ExecutionServiceWorkflowOptions] = {
    for {
      (userOpt, workspaceOpt, billingProject) <- getSomeThings(submissionId)
      user <- userOpt
      workspace <- workspaceOpt
      tokenOpt <- googleServicesDAO.getToken(user)
      token <- tokenOpt
    } yield {
      ExecutionServiceWorkflowOptions(
        s"gs://${workspace.bucketName}/${submissionId}",
        workspace.namespace,
        user.userEmail.value,
        token,
        billingProject.cromwellAuthBucketUrl
      )
    }
  }

  //FIXME: I lifted this from the bottom of SubmissionMonitorActor. Maybe it should be centralised?
  private def getUserInfo(cred: Credential) = {
    val expiresInSeconds: Long = Option(cred.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      cred.refreshToken()
    }
    UserInfo("", OAuth2BearerToken(cred.getAccessToken), Option(cred.getExpiresInSeconds).map(_.toLong).getOrElse(0), "")
  }
  
}
