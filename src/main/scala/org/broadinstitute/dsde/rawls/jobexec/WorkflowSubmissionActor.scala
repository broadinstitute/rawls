package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{MethodWiths, FutureSupport}
import _root_.slick.dbio.DBIOAction
import _root_.slick.dbio.Effect.Read
import spray.http.OAuth2BearerToken
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

  scheduleNextWorkflowQuery

  override def receive = {
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
          dataAccess.workflowQuery.batchUpdateStatus(wfsWithASingleSubmission.map(_.id), WorkflowStatuses.Launching)
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

  def getWdl(methodConfig: MethodConfiguration): ReadWriteAction[String] = {
    withMethod(methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, getUserInfo(credential)) { method =>
      withWdl(method) { wdl =>
        DBIO.successful(wdl)
      }
    }
  }

  //submit the batch of workflows with the given ids
  def submitWorkflowBatch(workflowIds: Seq[Long]): Future[WorkflowSubmissionMessage] = {

    val workflowBatchFuture = dataSource.inTransaction { dataAccess =>
      for {
      //Load a bunch of things we'll need to reconstruct information:
      //The list of workflows in this submission
        wfRecs <- dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result
        submissionId = wfRecs.head.submissionId
        _ = assert(wfRecs.forall(_.submissionId == submissionId)) //sanity check they're all in the same submission
        unpackedWfOpts <- DBIO.sequence(wfRecs.map(dataAccess.workflowQuery.loadWorkflow)) //reify to real workflows
        _ = assert(unpackedWfOpts.size == wfRecs.size) //there should be the right number of them
        _ = assert(unpackedWfOpts.forall(_.isDefined)) //and they should all have been found
        unpackedWfs = for {wfOpt <- unpackedWfOpts; wf <- wfOpt} yield {
          wf
        }

        //The submission itself
        subRecs <- dataAccess.submissionQuery.findById(submissionId).result
        submissionRec <- subRecs.headOption

        //The workspace
        wsRecs <- dataAccess.workspaceQuery.findByIdQuery(submissionRec.workspaceId).result
        workspaceRec <- wsRecs.headOption

        //The workspace's billing project
        bpOpt <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRec.namespace))
        billingProject <- bpOpt

        //The person who submitted the submission, and their token
        submitterOpt <- dataAccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(submissionRec.submitterId)))
        submitter <- submitterOpt
        tokenOpt <- DBIO.from(googleServicesDAO.getToken(submitter))
        token <- tokenOpt

        //The wdl
        mcOpt <- dataAccess.methodConfigurationQuery.loadMethodConfigurationById(submissionRec.methodConfigurationId)
        methodConfig <- mcOpt
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
        (wdl, wfRecs.map(_.id), wfInputsBatch, wfOpts)
      }
    }

    val cromwellSubmission = for {
      (wdl: String, workflowRecIds: Seq[Long], wfInputsBatch: Seq[String], wfOpts: Option[String]) <- workflowBatchFuture
      workflowSubmitResult <- executionServiceDAO.submitWorkflows(wdl, wfInputsBatch, Option(wfOpts.toJson.toString), getUserInfo(credential))
    } yield {
      workflowRecIds.zip(workflowSubmitResult)
    }

    cromwellSubmission flatMap { results =>
      dataSource.inTransaction { dataAccess =>
        //save successes as submitted workflows and hook up their cromwell ids
        val successes = results collect {
          case (wfRecId, Left(success: ExecutionServiceStatus)) =>
            //TODO: batchify this update?
            dataAccess.workflowQuery.findWorkflowById(wfRecId).map(u => u.externalid).update(success.id)
            wfRecId
        }
        dataAccess.workflowQuery.batchUpdateStatus(successes, WorkflowStatuses.Submitted)

        //save error messages into failures and flip them to Failed
        val failures = results collect {
          case (wfRecId, Right(failure: ExecutionServiceFailure)) =>
            //TODO: batchify this saveMessages?
            dataAccess.workflowQuery.saveMessages(Seq(AttributeString(failure.message)), wfRecId)
            wfRecId
        }
        dataAccess.workflowQuery.batchUpdateStatus(failures, WorkflowStatuses.Failed)
      } map { _ => LookForWorkflows }
    } recover {
      //If any of this fails, set all workflows to failed with whatever message we have.
      case t: Throwable =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.workflowQuery.batchUpdateStatus(workflowIds, WorkflowStatuses.Failed)
          DBIO.sequence(workflowIds map { id => dataAccess.workflowQuery.saveMessages(Seq(AttributeString(t.getMessage)), id) })
        }
        throw t
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
