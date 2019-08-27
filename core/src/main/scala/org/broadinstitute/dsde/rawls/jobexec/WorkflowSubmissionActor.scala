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
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, util}
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object WorkflowSubmissionActor {
  def props(dataSource: SlickDataSource,
            methodRepoDAO: MethodRepoDAO,
            googleServicesDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            dosResolver: DosResolver,
            executionServiceCluster: ExecutionServiceCluster,
            batchSize: Int,
            credential: Credential,
            processInterval: FiniteDuration,
            pollInterval: FiniteDuration,
            maxActiveWorkflowsTotal: Int,
            maxActiveWorkflowsPerUser: Int,
            runtimeOptions: Option[JsValue],
            trackDetailedSubmissionMetrics: Boolean,
            workbenchMetricBaseName: String,
            requesterPaysRole: String,
            useWorkflowCollectionField: Boolean,
            useWorkflowCollectionLabel: Boolean,
            defaultBackend: CromwellBackend,
            methodConfigResolver: MethodConfigResolver): Props = {
    Props(new WorkflowSubmissionActor(dataSource, methodRepoDAO, googleServicesDAO, samDAO, dosResolver, executionServiceCluster, batchSize, credential, processInterval, pollInterval, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, runtimeOptions, trackDetailedSubmissionMetrics, workbenchMetricBaseName, requesterPaysRole, useWorkflowCollectionField, useWorkflowCollectionLabel, defaultBackend, methodConfigResolver))
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
                              val samDAO: SamDAO,
                              val dosResolver: DosResolver,
                              val executionServiceCluster: ExecutionServiceCluster,
                              val batchSize: Int,
                              val credential: Credential,
                              val processInterval: FiniteDuration,
                              val pollInterval: FiniteDuration,
                              val maxActiveWorkflowsTotal: Int,
                              val maxActiveWorkflowsPerUser: Int,
                              val runtimeOptions: Option[JsValue],
                              val trackDetailedSubmissionMetrics: Boolean,
                              override val workbenchMetricBaseName: String,
                              val requesterPaysRole: String,
                              val useWorkflowCollectionField: Boolean,
                              val useWorkflowCollectionLabel: Boolean,
                              val defaultBackend: CromwellBackend,
                              val methodConfigResolver: MethodConfigResolver) extends Actor with WorkflowSubmission with LazyLogging {

  import context._

  override def preStart(): Unit = {
    super.preStart()
    scheduleNextWorkflowQuery(addJitter(0 seconds, pollInterval))
  }

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
  val samDAO: SamDAO
  val dosResolver: DosResolver
  val executionServiceCluster: ExecutionServiceCluster
  val batchSize: Int
  val credential: Credential
  val maxActiveWorkflowsTotal: Int
  val maxActiveWorkflowsPerUser: Int
  val runtimeOptions: Option[JsValue]
  val trackDetailedSubmissionMetrics: Boolean
  val requesterPaysRole: String
  val useWorkflowCollectionField: Boolean
  val useWorkflowCollectionLabel: Boolean
  val defaultBackend: CromwellBackend
  val methodConfigResolver: MethodConfigResolver

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
        implicit val wfStatusCounter = (status: WorkflowStatus) =>
          if (trackDetailedSubmissionMetrics) Option(workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceRec.toWorkspaceName, submissionRec.id))(status))
          else None
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

  def buildWorkflowOpts(workspace: WorkspaceRecord, submissionId: UUID, userEmail: RawlsUserEmail, petSAJson: String, billingProject: RawlsBillingProject, useCallCache: Boolean, workflowFailureMode: Option[WorkflowFailureMode]) = {
    val petSAEmail = petSAJson.parseJson.asJsObject.getFields("client_email").headOption match {
      case Some(JsString(value)) => value
      case Some(x) => throw new RawlsException(s"unexpected json value for client_email [$x] in service account key")
      case None => throw new RawlsException(s"client_email missing for service account key json")
    }

    ExecutionServiceWorkflowOptions(
      s"gs://${workspace.bucketName}/${submissionId}",
      workspace.namespace,
      userEmail.value,
      petSAEmail,
      petSAJson,
      billingProject.cromwellAuthBucketUrl,
      s"gs://${workspace.bucketName}/${submissionId}/workflow.logs",
      runtimeOptions,
      useCallCache,
      billingProject.cromwellBackend.getOrElse(defaultBackend),
      workflowFailureMode,
      google_labels = Map("terra-submission-id" -> s"terra-${submissionId.toString}")
    )
  }

  def getWdl(methodConfig: MethodConfiguration, userInfo: UserInfo)(implicit executionContext: ExecutionContext): Future[String] = {
    dataSource.inTransaction { dataAccess => //this is a transaction that makes no database calls, but the sprawling stack of withFoos was too hard to unpick :(
      withMethod(methodConfig.methodRepoMethod, userInfo) { method =>
        withWdl(method) { wdl =>
          DBIO.successful(wdl)
        }
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

  private def resolveDosUriServiceAccounts(dosUris: Set[String])(implicit executionContext: ExecutionContext): Future[Set[String]] = {
    Future.traverse(dosUris) { dosUri =>
      dosResolver.dosServiceAccountEmail(dosUri)
    }.map { emails =>
      emails.collect {
        case Some(email) => email
      }
    }
  }

  private def collectDosUris(workflowBatch: Seq[Workflow]): Set[String] = {
    val dosUris = for {
      workflow <- workflowBatch
      inputResolutions <- workflow.inputResolutions
      attribute <- inputResolutions.value.toSeq // toSeq makes the for comp work
      dosAttributeValue <- attribute match {
        case AttributeString(s) if s.matches(dosResolver.dosUriPattern) => Seq(s)
        case AttributeValueList(valueList) => valueList.collect {
          case AttributeString(s) if s.value.matches(dosResolver.dosUriPattern) => s
        }
        case _ => Seq.empty
      }
    } yield {
      dosAttributeValue
    }
    dosUris.toSet
  }

  //submit the batch of workflows with the given ids
  def submitWorkflowBatch(batch: WorkflowBatch)(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {

    val WorkflowBatch(workflowIds, submissionRec, workspaceRec) = batch

    // implicitly passed to WorkflowComponent methods which update status
    implicit val wfStatusCounter = (status: WorkflowStatus) =>
      if (trackDetailedSubmissionMetrics) Option(workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceRec.toWorkspaceName, submissionRec.id))(status))
      else None

    //split out the db transaction from the calls to external services
    //DON'T make http calls in here!
    val dbThingsFuture = dataSource.inTransaction { dataAccess =>
      for {
        //Load a bunch of things we'll need to reconstruct information:
        //The list of workflows in this submission
        wfRecs <- getWorkflowRecordBatch(workflowIds, dataAccess)
        workflowBatch <- reifyWorkflowRecords(wfRecs, dataAccess)

        //The workspace's billing project
        billingProject <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRec.namespace)).map(_.get)

        //the method configuration, in order to get the wdl
        methodConfig <- dataAccess.methodConfigurationQuery.loadMethodConfigurationById(submissionRec.methodConfigurationId).map(_.get)
      } yield {
        (wfRecs, workflowBatch, billingProject, methodConfig)
      }
    }

    val workflowBatchFuture = for {
      //yank things from the db. note this future has already started running and we're just waiting on it here
      (wfRecs, workflowBatch, billingProject, methodConfig) <- dbThingsFuture

      petSAJson <- samDAO.getPetServiceAccountKeyForUser(billingProject.projectName.value, RawlsUserEmail(submissionRec.submitterEmail))
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)

      wdl <- getWdl(methodConfig, petUserInfo)
    } yield {

      val wfOpts = buildWorkflowOpts(workspaceRec, submissionRec.id, RawlsUserEmail(submissionRec.submitterEmail), petSAJson, billingProject, submissionRec.useCallCache, WorkflowFailureModes.withNameOpt(submissionRec.workflowFailureMode))
        val submissionAndWorkspaceLabels = Map("submission-id" -> submissionRec.id.toString,  "workspace-id" -> workspaceRec.id.toString)
        val wfLabels = workspaceRec.workflowCollection match {
          case Some(workflowCollection) if useWorkflowCollectionLabel => submissionAndWorkspaceLabels + ("caas-collection-name" -> workflowCollection)
          case _ => submissionAndWorkspaceLabels
        }
        val wfCollection = if (useWorkflowCollectionField) workspaceRec.workflowCollection else None

      val wfInputsBatch = workflowBatch map { wf =>
        val methodProps = wf.inputResolutions map {
          case svv: SubmissionValidationValue if svv.value.isDefined =>
            svv.inputName -> svv.value.get
        }
        methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
      }

      //yield the things we're going to submit to Cromwell
      (wdl, wfRecs, wfInputsBatch, wfOpts, wfLabels, wfCollection, collectDosUris(workflowBatch), petUserInfo)
    }


    import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat
    val cromwellSubmission = for {
      (wdl, workflowRecs, wfInputsBatch, wfOpts, wfLabels, wfCollection, dosUris, petUserInfo) <- workflowBatchFuture
      dosServiceAccounts <- resolveDosUriServiceAccounts(dosUris)
      _ <- if (dosServiceAccounts.isEmpty) Future.successful(false) else googleServicesDAO.addPolicyBindings(RawlsBillingProjectName(wfOpts.google_project), Map(requesterPaysRole -> dosServiceAccounts.map("serviceAccount:"+_).toList))
      // Should labels be an Option? It's not optional for rawls (but then wfOpts are options too)
      workflowSubmitResult <- executionServiceCluster.submitWorkflows(workflowRecs, wdl, wfInputsBatch, Option(wfOpts.toJson.toString), Option(wfLabels), wfCollection, petUserInfo)
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
