package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickWorkspaceContext, ExecutionServiceDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{MethodWiths, FutureSupport}
import slick.dbio.DBIOAction
import spray.http.OAuth2BearerToken
import spray.json._
import spray.httpx.SprayJsonSupport._


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

trait WorkflowSubmission extends FutureSupport with LazyLogging with MethodWiths {
  val datasource: SlickDataSource
  val googleServicesDAO: GoogleServicesDAO
  val executionServiceDAO: ExecutionServiceDAO
  val batchSize: Int
  val credential: Credential
  val pollInterval: FiniteDuration

  import datasource.dataAccess.driver.api._

  //Get a blob of unlaunched workflows, flip their status, and queue them for submission.
  def getUnlaunchedWorkflowBatch()(implicit executionContext: ExecutionContext): Future[Option[WorkflowSubmissionMessage]] = {
    val unlaunchedWfOptF = datasource.inTransaction { dataAccess =>
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
      case workflowRecs if workflowRecs.nonEmpty => Some(SubmitWorkflowBatch(workflowRecs.map(_.id)))
      case _ => None
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
  def submitWorkflowBatch(workflowIds: Seq[Long]): WorkflowSubmissionMessage = {
    //pull out the records, but don't reify them into workflows yet as we need their submission ID
    val wfRecsFuture: Future[Seq[WorkflowRecord]] = datasource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result
    }

    datasource.inTransaction { dataAccess =>
      val oop = for {
        //Load a bunch of things we'll need to reconstruct information:
        //The list of workflows in this submission
        wfRecs <- dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result
        submissionId = wfRecs.head.submissionId
        assert(wfRecs.forall( _.submissionId == submissionId)) //sanity check they're all in the same submission
        unpackedWfOpts <- DBIO.sequence( wfRecs.map(dataAccess.workflowQuery.loadWorkflow) ) //reify to real workflows
        assert(unpackedWfOpts.size == wfRecs.size) //there should be the right number of them
        assert(unpackedWfOpts.forall(_.isDefined)) //and they should all have been found
        unpackedWfs = for { wfOpt <- unpackedWfOpts; wf <- wfOpt } yield { wf }

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
        import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat
        executionServiceDAO.submitWorkflows(wdl, wfInputsBatch, Option(wfOpts.toJson.toString), getUserInfo(credential))
        //TODO: what now?
      }

      oop

    }

    //TODO: if cromwell submission fails, set workflow to Failed rather than WorkflowFailure
    LookForWorkflows
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
