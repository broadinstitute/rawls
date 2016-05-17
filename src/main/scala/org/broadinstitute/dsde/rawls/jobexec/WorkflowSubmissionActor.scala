package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{MethodWiths, FutureSupport}
import _root_.slick.dbio.DBIOAction
import _root_.slick.dbio.Effect.Read
import spray.http.{StatusCodes, OAuth2BearerToken}
import spray.json._
import spray.httpx.SprayJsonSupport._


import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Failure}


object WorkflowSubmissionActor {
  def props(dataSource: SlickDataSource,
            methodRepoDAO: MethodRepoDAO,
            googleServicesDAO: GoogleServicesDAO,
            executionServiceDAO: ExecutionServiceDAO,
            batchSize: Int,
            credential: Credential,
            pollInterval: FiniteDuration): Props = {
    Props(new WorkflowSubmissionActor(dataSource, methodRepoDAO, googleServicesDAO, executionServiceDAO, batchSize, credential, pollInterval))
  }

  sealed trait WorkflowSubmissionMessage
  case object ScheduleNextWorkflowQuery extends WorkflowSubmissionMessage
  case object LookForWorkflows extends WorkflowSubmissionMessage
  case class SubmitWorkflowBatch(workflowIds: Seq[Long]) extends WorkflowSubmissionMessage

}

class WorkflowSubmissionActor(val dataSource: SlickDataSource,
                              val methodRepoDAO: MethodRepoDAO,
                              val googleServicesDAO: GoogleServicesDAO,
                              val executionServiceDAO: ExecutionServiceDAO,
                              val batchSize: Int,
                              val credential: Credential,
                              val pollInterval: FiniteDuration) extends Actor with WorkflowSubmission with LazyLogging {

  import context._

  self ! ScheduleNextWorkflowQuery

  override def receive = {
    case ScheduleNextWorkflowQuery =>
      scheduleNextWorkflowQuery

    case LookForWorkflows =>
      getUnlaunchedWorkflowBatch() pipeTo self

    case SubmitWorkflowBatch(workflowIds) =>
      submitWorkflowBatch(workflowIds) pipeTo self

    case Status.Failure(t) =>
      logger.error(t.getMessage)
      scheduleNextWorkflowQuery
  }

  def scheduleNextWorkflowQuery: Cancellable = {
    system.scheduler.scheduleOnce(pollInterval, self, LookForWorkflows)
  }
}

trait WorkflowSubmission extends FutureSupport with LazyLogging with MethodWiths {
  val dataSource: SlickDataSource
  val methodRepoDAO: MethodRepoDAO
  val googleServicesDAO: GoogleServicesDAO
  val executionServiceDAO: ExecutionServiceDAO
  val batchSize: Int
  val credential: Credential
  val pollInterval: FiniteDuration

  import dataSource.dataAccess.driver.api._

  //Get a blob of unlaunched workflows, flip their status, and queue them for submission.
  def getUnlaunchedWorkflowBatch()(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {
    val unlaunchedWfOptF = dataSource.inTransaction { dataAccess =>
      //grab a bunch of unsubmitted workflows
      dataAccess.workflowQuery.findUnsubmittedWorkflows().take(batchSize).result map { wfRecs =>
        if (wfRecs.nonEmpty) wfRecs
        else {
          //they should also all have the same submission ID
          val wfsWithASingleSubmission = wfRecs.filter(_.submissionId == wfRecs.head.submissionId)
          //instead: on update, if we don't get the right number back, roll back the txn
          //TODO: biden has an incoming change here, with optimistic locking
          dataAccess.workflowQuery.batchUpdateStatus(wfsWithASingleSubmission, WorkflowStatuses.Launching)
          wfsWithASingleSubmission
        }
      }
    }

    //if we find any, next step is to submit them. otherwise, look again.
    unlaunchedWfOptF.map {
      case workflowRecs if workflowRecs.nonEmpty => SubmitWorkflowBatch(workflowRecs.map(_.id))
      case _ => LookForWorkflows
    }
  }

  def buildWorkflowOpts(workspace: WorkspaceRecord, submissionId: UUID, user: RawlsUser, token: String, billingProject: RawlsBillingProject) = {
    ExecutionServiceWorkflowOptions(
      s"gs://${workspace.bucketName}/${submissionId}",
      workspace.namespace,
      user.userEmail.value,
      token,
      billingProject.cromwellAuthBucketUrl
    )
  }

  def getWdl(methodConfig: MethodConfiguration)(implicit executionContext: ExecutionContext): ReadWriteAction[String] = {
    withMethod(methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, getUserInfo(credential)) { method =>
      withWdl(method) { wdl =>
        DBIO.successful(wdl)
      }
    }
  }

  def getWorkflowRecordBatch(workflowIds: Seq[Long], dataAccess: DataAccess)(implicit executionContext: ExecutionContext) = {
    dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result map { wfRecs =>
      val submissionId = wfRecs.head.submissionId
      if( !wfRecs.forall(_.submissionId == submissionId) ) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, "Workflow batch has different submissions!"))
      }
      wfRecs
    }
  }

  //submit the batch of workflows with the given ids
  def submitWorkflowBatch(workflowIds: Seq[Long])(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {

    val workflowBatchFuture = dataSource.inTransaction { dataAccess =>
      for {
      //Load a bunch of things we'll need to reconstruct information:
      //The list of workflows in this submission
        wfRecs <- getWorkflowRecordBatch(workflowIds, dataAccess)
        submissionId = wfRecs.head.submissionId

        unpackedWfOpts <- DBIO.sequence(wfRecs.map(dataAccess.workflowQuery.loadWorkflow)) //reify to real workflows
        _ = assert(unpackedWfOpts.size == wfRecs.size) //there should be the right number of them
        _ = assert(unpackedWfOpts.forall(_.isDefined)) //and they should all have been found
        unpackedWfs = for {wfOpt <- unpackedWfOpts; wf <- wfOpt} yield {
          wf
        }

        //The submission itself
        subRecs <- dataAccess.submissionQuery.findById(submissionId).result
        submissionRec = subRecs.head

        //The workspace
        wsRecs <- dataAccess.workspaceQuery.findByIdQuery(submissionRec.workspaceId).result
        workspaceRec = wsRecs.head

        //The workspace's billing project
        bpOpt <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRec.namespace))
        billingProject = bpOpt.get

        //The person who submitted the submission, and their token
        submitterOpt <- dataAccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(submissionRec.submitterId)))
        submitter = submitterOpt.get
        tokenOpt <- DBIO.from(googleServicesDAO.getToken(submitter))
        token = tokenOpt.get

        //The wdl
        mcOpt <- dataAccess.methodConfigurationQuery.loadMethodConfigurationById(submissionRec.methodConfigurationId)
        methodConfig = mcOpt.get
        wdl <- getWdl(methodConfig)
      } yield {

        val wfOpts = buildWorkflowOpts(workspaceRec, submissionId, submitter, token, billingProject)

        val wfInputsBatch = unpackedWfs map { wf =>
          val methodProps = wf.inputResolutions map {
            case svv: SubmissionValidationValue if svv.value.isDefined =>
              svv.inputName -> svv.value.get
          }
          MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
        }

        //yield the things we're going to submit to Cromwell
        (wdl, wfRecs, wfInputsBatch, wfOpts)
      }
    }

    import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat
    val cromwellSubmission = for {
      (wdl, workflowRecs, wfInputsBatch, wfOpts) <- workflowBatchFuture
      workflowSubmitResult <- executionServiceDAO.submitWorkflows(wdl, wfInputsBatch, Option(wfOpts.toJson.toString), getUserInfo(credential))
    } yield {
      workflowRecs.zip(workflowSubmitResult)
    }

    cromwellSubmission flatMap { results =>
      dataSource.inTransaction { dataAccess =>
        //save successes as submitted workflows and hook up their cromwell ids
        val successes = results collect {
          case (wfRec, Left(success: ExecutionServiceStatus)) =>
            val updatedWfRec = wfRec.copy(externalId = Option(success.id), status = success.status)
            dataAccess.workflowQuery.updateWorkflowRecord(updatedWfRec)
        }

        //save error messages into failures and flip them to Failed
        val failures = results collect {
          case (wfRec, Right(failure: ExecutionServiceFailure)) =>
            //TODO: batchify this saveMessages?
            dataAccess.workflowQuery.saveMessages(Seq(AttributeString(failure.message)), wfRec.id) andThen
            dataAccess.workflowQuery.updateStatus(wfRec, WorkflowStatuses.Failed)
        }

        DBIO.seq((successes ++ failures):_*)
      } map { _ => ScheduleNextWorkflowQuery }
    } recoverWith {
      //If any of this fails, set all workflows to failed with whatever message we have.
      case t: Throwable =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result flatMap { wfRecs =>
            dataAccess.workflowQuery.batchUpdateStatus(wfRecs, WorkflowStatuses.Failed)
          } andThen
          DBIO.sequence(workflowIds map { id => dataAccess.workflowQuery.saveMessages(Seq(AttributeString(t.getMessage)), id) })
        } map { _ => throw t }
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
