package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, MethodWiths, addJitter}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.http.StatusCodes
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object WorkflowSubmissionActor {
  def props(dataSource: SlickDataSource,
            methodRepoDAO: MethodRepoDAO,
            googleServicesDAO: GoogleServicesDAO,
            executionServiceCluster: ExecutionServiceCluster,
            batchSize: Int,
            credential: Credential,
            processInterval: FiniteDuration,
            pollInterval: FiniteDuration,
            maxActiveWorkflowsTotal: Int,
            maxActiveWorkflowsPerUser: Int,
            runtimeOptions: Option[JsValue],
            workbenchMetricBaseName: String): Props = {
    Props(new WorkflowSubmissionActor(dataSource, methodRepoDAO, googleServicesDAO, executionServiceCluster, batchSize, credential, processInterval, pollInterval, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, runtimeOptions, workbenchMetricBaseName))
  }

  case class WorkflowBatch(workflowIds: Seq[Long], submissionRec: SubmissionRecord, workspaceRec: WorkspaceRecord)

  sealed trait WorkflowSubmissionMessage
  case object ScheduleNextWorkflow extends WorkflowSubmissionMessage
  case object ProcessNextWorkflow extends WorkflowSubmissionMessage
  case object LookForWorkflows extends WorkflowSubmissionMessage
  case class SubmitWorkflowBatch(workflowBatch: WorkflowBatch) extends WorkflowSubmissionMessage

}

class WorkflowSubmissionActor(val dataSource: SlickDataSource,
                              val methodRepoDAO: MethodRepoDAO,
                              val googleServicesDAO: GoogleServicesDAO,
                              val executionServiceCluster: ExecutionServiceCluster,
                              val batchSize: Int,
                              val credential: Credential,
                              val processInterval: FiniteDuration,
                              val pollInterval: FiniteDuration,
                              val maxActiveWorkflowsTotal: Int,
                              val maxActiveWorkflowsPerUser: Int,
                              val runtimeOptions: Option[JsValue],
                              override val workbenchMetricBaseName: String) extends Actor with WorkflowSubmission with LazyLogging {

  import context._

  self ! ScheduleNextWorkflow

  override def receive = {
    case ScheduleNextWorkflow =>
      scheduleNextWorkflowQuery(addJitter(pollInterval))

    case ProcessNextWorkflow =>
      scheduleNextWorkflowQuery(processInterval)

    case LookForWorkflows =>
      getUnlaunchedWorkflowBatch() pipeTo self

    case SubmitWorkflowBatch(workflowBatch) =>
      submitWorkflowBatch(workflowBatch) pipeTo self

    case Status.Failure(t) =>
      logger.error(t.getMessage)
      self ! ScheduleNextWorkflow
  }

  def scheduleNextWorkflowQuery(delay: FiniteDuration): Cancellable = {
    system.scheduler.scheduleOnce(delay, self, LookForWorkflows)
  }
}

trait WorkflowSubmission extends FutureSupport with LazyLogging with MethodWiths with RawlsInstrumented {
  val dataSource: SlickDataSource
  val methodRepoDAO: MethodRepoDAO
  val googleServicesDAO: GoogleServicesDAO
  val executionServiceCluster: ExecutionServiceCluster
  val batchSize: Int
  val credential: Credential
  val maxActiveWorkflowsTotal: Int
  val maxActiveWorkflowsPerUser: Int
  val runtimeOptions: Option[JsValue]

  import dataSource.dataAccess.driver.api._

  //Get a blob of unlaunched workflows, flip their status, and queue them for submission.
  def getUnlaunchedWorkflowBatch()(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {
    val workflowRecsToLaunch = dataSource.inTransaction { dataAccess =>
      for {
        runningCount <- dataAccess.workflowQuery.countWorkflows(WorkflowStatuses.runningStatuses)
        reservedWorkflowRecs <- reserveWorkflowBatch(dataAccess, runningCount)
      } yield reservedWorkflowRecs
    }

    //flip the workflows to Launching in a separate txn.
    //if this optimistic-lock-exceptions with another txn, this one will barf and we'll reschedule when we pipe it back to ourselves
    workflowRecsToLaunch flatMap {
      case Some((wfRecs, submissionRec, workspaceRec)) if wfRecs.nonEmpty =>
        // implicitly passed to WorkflowComponent.batchUpdateStatus
        implicit val wfStatusCounter = workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceRec.toWorkspaceName, submissionRec.id))
        dataSource.inTransaction { dataAccess =>
          dataAccess.workflowQuery.batchUpdateStatus(wfRecs, WorkflowStatuses.Launching) map { _ =>
            SubmitWorkflowBatch(WorkflowBatch(wfRecs.map(_.id), submissionRec, workspaceRec))
          }
        }
      case _ => Future.successful(ScheduleNextWorkflow)
    } recover {
      // if we found some but another actor reserved the first look again immediately
      case _: RawlsConcurrentModificationException => ProcessNextWorkflow
    }
  }

  def reserveWorkflowBatch(dataAccess: DataAccess, activeCount: Int)(implicit executionContext: ExecutionContext): ReadWriteAction[Option[(Seq[WorkflowRecord], SubmissionRecord, WorkspaceRecord)]] = {
    if (activeCount > maxActiveWorkflowsTotal) {
      logger.warn(s"There are $activeCount active workflows which is beyond the total active cap of $maxActiveWorkflowsTotal. Workflows will not be submitted.")
      DBIO.successful(None)
    } else {
      for {
        // we exclude workflows submitted by users that have exceeded the max workflows per user
        excludedUsers <- dataAccess.workflowQuery.listSubmittersWithMoreWorkflowsThan(maxActiveWorkflowsPerUser, WorkflowStatuses.runningStatuses)
        workflowRecs <- dataAccess.workflowQuery.findQueuedWorkflows(excludedUsers.map { case (submitter, count) => submitter }, SubmissionStatuses.terminalStatuses :+ SubmissionStatuses.Aborting).take(batchSize).result
        reservedRecs <- if (workflowRecs.isEmpty) {
            DBIO.successful(None)
          } else {
            // they should also all have the same submission ID
            val submissionId = workflowRecs.head.submissionId
            val filteredWorkflowRecs = workflowRecs.filter(_.submissionId == submissionId)
            for {
              submissionRec <- dataAccess.submissionQuery.findById(submissionId).result.map(_.head)
              workspaceRec <- dataAccess.workspaceQuery.findByIdQuery(submissionRec.workspaceId).result.map(_.head)
            } yield Some((filteredWorkflowRecs, submissionRec, workspaceRec))
          }
      } yield reservedRecs
    }
  }

  def buildWorkflowOpts(workspace: WorkspaceRecord, submissionId: UUID, user: RawlsUser, token: String, billingProject: RawlsBillingProject, useCallCache: Boolean, workflowFailureMode: Option[WorkflowFailureMode]) = {
    ExecutionServiceWorkflowOptions(
      s"gs://${workspace.bucketName}/${submissionId}",
      workspace.namespace,
      user.userEmail.value,
      token,
      billingProject.cromwellAuthBucketUrl,
      s"gs://${workspace.bucketName}/${submissionId}/workflow.logs",
      runtimeOptions,
      useCallCache,
      workflowFailureMode
    )
  }

  def getWdl(methodConfig: MethodConfiguration, userCredentials: Credential)(implicit executionContext: ExecutionContext): ReadWriteAction[String] = {
    withMethod(methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, UserInfo.buildFromTokens(userCredentials)) { method =>
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

  def reifyWorkflowRecords(workflowRecs: Seq[WorkflowRecord], dataAccess: DataAccess)(implicit executionContext: ExecutionContext) = {
    DBIO.sequence(workflowRecs map { wfRec =>
      dataAccess.workflowQuery.loadWorkflow(wfRec) map( _.getOrElse(
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Failed to load workflow id ${wfRec.id}"))
      ))
    })
  }

  def execServiceFailureMessages(failure: ExecutionServiceFailure): Seq[AttributeString] = {
    Seq(AttributeString(failure.message)) ++ failure.errors.getOrElse(JsArray()).elements.map( {
      case err:JsString => AttributeString(err.convertTo[String]) //otherwise spray escapes the quotes
      case err:JsValue => AttributeString(err.toString)
    })
  }

  //submit the batch of workflows with the given ids
  def submitWorkflowBatch(batch: WorkflowBatch)(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {

    val WorkflowBatch(workflowIds, submissionRec, workspaceRec) = batch
    // implicitly passed to WorkflowComponent methods which update status
    implicit val wfStatusCounter = workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceRec.toWorkspaceName, submissionRec.id))

    val workflowBatchFuture = dataSource.inTransaction { dataAccess =>
      for {
        //Load a bunch of things we'll need to reconstruct information:
        //The list of workflows in this submission
        wfRecs <- getWorkflowRecordBatch(workflowIds, dataAccess)
        workflowBatch <- reifyWorkflowRecords(wfRecs, dataAccess)

        //The workspace's billing project
        billingProject <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRec.namespace)).map(_.get)

        //The person who submitted the submission, and their token
        submitter <- dataAccess.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(submissionRec.submitterId))).map(_.get)
        userCredentials <- DBIO.from(googleServicesDAO.getUserCredentials(submitter)).map(_.getOrElse(throw new RawlsException(s"cannot find credentials for $submitter")))

        //The wdl
        methodConfig <- dataAccess.methodConfigurationQuery.loadMethodConfigurationById(submissionRec.methodConfigurationId).map(_.get)
        wdl <- getWdl(methodConfig, userCredentials)
      } yield {
        val wfOpts = buildWorkflowOpts(workspaceRec, submissionRec.id, submitter, userCredentials.getRefreshToken, billingProject, submissionRec.useCallCache, WorkflowFailureModes.withNameOpt(submissionRec.workflowFailureMode))

        val wfInputsBatch = workflowBatch map { wf =>
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
      workflowSubmitResult <- executionServiceCluster.submitWorkflows(workflowRecs, wdl, wfInputsBatch, Option(wfOpts.toJson.toString), UserInfo.buildFromTokens(credential))
    } yield {
      // call to submitWorkflows returns a tuple:
      val executionServiceKey = workflowSubmitResult._1
      val executionServiceResults = workflowSubmitResult._2

      (executionServiceKey, workflowRecs.zip(executionServiceResults))
    }

    //Second txn to update workflows to Launching.
    //If this txn fails we'll just end up rescheduling the next workflow query and will restart this function from the top.
    //Since the first txn didn't do any writes to the db it won't be left in a weird halfway state.
    cromwellSubmission flatMap { case (executionServiceKey, results) =>
      dataSource.inTransaction { dataAccess =>
        //save successes as submitted workflows and hook up their cromwell ids
        val successUpdates = results collect {
          case (wfRec, Left(success: ExecutionServiceStatus)) =>
            val updatedWfRec = wfRec.copy(externalId = Option(success.id), status = success.status, executionServiceKey = Option(executionServiceKey.toString))
            dataAccess.workflowQuery.updateWorkflowRecord(updatedWfRec)
        }

        //save error messages into failures and flip them to Failed
        val failures = results collect {
          case (wfRec, Right(failure: ExecutionServiceFailure)) => (wfRec, failure)
        }
        val failureMessages = failures map { case (wfRec, failure) => dataAccess.workflowQuery.saveMessages(execServiceFailureMessages(failure), wfRec.id) }
        val failureStatusUpd = dataAccess.workflowQuery.batchUpdateStatusAndExecutionServiceKey(failures.map(_._1), WorkflowStatuses.Failed, executionServiceKey)

        DBIO.seq((successUpdates ++ failureMessages :+ failureStatusUpd):_*)
      } map { _ => ProcessNextWorkflow }
    } recoverWith {
      //If any of this fails, set all workflows to failed with whatever message we have.
      case t: Throwable =>
        dataSource.inTransaction { dataAccess =>
          val message = Option(t.getMessage).getOrElse(t.getClass.getName)
          dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result flatMap { wfRecs =>
            dataAccess.workflowQuery.batchUpdateStatus(wfRecs, WorkflowStatuses.Failed)
          } andThen
          DBIO.sequence(workflowIds map { id => dataAccess.workflowQuery.saveMessages(Seq(AttributeString(message)), id) })
        } map { _ => throw t }
    }
  }
}
