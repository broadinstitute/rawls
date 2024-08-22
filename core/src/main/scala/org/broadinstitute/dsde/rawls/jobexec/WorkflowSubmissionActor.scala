package org.broadinstitute.dsde.rawls.jobexec

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsResolver
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor._
import org.broadinstitute.dsde.rawls.metrics.{BardService, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.metrics.logEvents.SubmissionEvent
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{addJitter, FutureSupport, MethodWiths}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object WorkflowSubmissionActor {
  def props(dataSource: SlickDataSource,
            methodRepoDAO: MethodRepoDAO,
            googleServicesDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            dosResolver: DrsResolver,
            executionServiceCluster: ExecutionServiceCluster,
            batchSize: Int,
            processInterval: FiniteDuration,
            pollInterval: FiniteDuration,
            maxActiveWorkflowsTotal: Int,
            maxActiveWorkflowsPerUser: Int,
            defaultRuntimeOptions: Option[JsValue],
            trackDetailedSubmissionMetrics: Boolean,
            workbenchMetricBaseName: String,
            requesterPaysRole: String,
            useWorkflowCollectionField: Boolean,
            useWorkflowCollectionLabel: Boolean,
            defaultNetworkCromwellBackend: CromwellBackend,
            highSecurityNetworkCromwellBackend: CromwellBackend,
            methodConfigResolver: MethodConfigResolver,
            bardService: BardService
  ): Props =
    Props(
      new WorkflowSubmissionActor(
        dataSource,
        methodRepoDAO,
        googleServicesDAO,
        samDAO,
        dosResolver,
        executionServiceCluster,
        batchSize,
        processInterval,
        pollInterval,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        defaultRuntimeOptions,
        trackDetailedSubmissionMetrics,
        workbenchMetricBaseName,
        requesterPaysRole,
        useWorkflowCollectionField,
        useWorkflowCollectionLabel,
        defaultNetworkCromwellBackend,
        highSecurityNetworkCromwellBackend,
        methodConfigResolver,
        bardService
      )
    )

  case class WorkflowBatch(workflowIds: Seq[Long], submissionRec: SubmissionRecord, workspaceRec: WorkspaceRecord)

  sealed trait WorkflowSubmissionMessage
  case object ScheduleNextWorkflow extends WorkflowSubmissionMessage
  case object ProcessNextWorkflow extends WorkflowSubmissionMessage
  case object LookForWorkflows extends WorkflowSubmissionMessage
  case class SubmitWorkflowBatch(workflowBatch: WorkflowBatch) extends WorkflowSubmissionMessage

}

//noinspection TypeAnnotation
class WorkflowSubmissionActor(val dataSource: SlickDataSource,
                              val methodRepoDAO: MethodRepoDAO,
                              val googleServicesDAO: GoogleServicesDAO,
                              val samDAO: SamDAO,
                              val drsResolver: DrsResolver,
                              val executionServiceCluster: ExecutionServiceCluster,
                              val batchSize: Int,
                              val processInterval: FiniteDuration,
                              val pollInterval: FiniteDuration,
                              val maxActiveWorkflowsTotal: Int,
                              val maxActiveWorkflowsPerUser: Int,
                              val defaultRuntimeOptions: Option[JsValue],
                              val trackDetailedSubmissionMetrics: Boolean,
                              override val workbenchMetricBaseName: String,
                              val requesterPaysRole: String,
                              val useWorkflowCollectionField: Boolean,
                              val useWorkflowCollectionLabel: Boolean,
                              val defaultNetworkCromwellBackend: CromwellBackend,
                              val highSecurityNetworkCromwellBackend: CromwellBackend,
                              val methodConfigResolver: MethodConfigResolver,
                              val bardService: BardService
) extends Actor
    with WorkflowSubmission
    with LazyLogging {

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

  def scheduleNextWorkflowQuery(delay: FiniteDuration): Cancellable =
    system.scheduler.scheduleOnce(delay, self, LookForWorkflows)
}

//noinspection ScalaUnnecessaryParentheses,TypeAnnotation,RedundantBlock,DuplicatedCode,ScalaUnusedSymbol
trait WorkflowSubmission extends FutureSupport with LazyLogging with MethodWiths with RawlsInstrumented {
  val dataSource: SlickDataSource
  val methodRepoDAO: MethodRepoDAO
  val googleServicesDAO: GoogleServicesDAO
  val samDAO: SamDAO
  val drsResolver: DrsResolver
  val executionServiceCluster: ExecutionServiceCluster
  val batchSize: Int
  val maxActiveWorkflowsTotal: Int
  val maxActiveWorkflowsPerUser: Int
  val defaultRuntimeOptions: Option[JsValue]
  val trackDetailedSubmissionMetrics: Boolean
  val requesterPaysRole: String
  val useWorkflowCollectionField: Boolean
  val useWorkflowCollectionLabel: Boolean
  val defaultNetworkCromwellBackend: CromwellBackend
  val highSecurityNetworkCromwellBackend: CromwellBackend
  val methodConfigResolver: MethodConfigResolver
  val bardService: BardService

  import dataSource.dataAccess.driver.api._

  // Get a blob of unlaunched workflows, flip their status, and queue them for submission.
  def getUnlaunchedWorkflowBatch()(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {
    val workflowRecsToLaunch = dataSource.inTransaction { dataAccess =>
      for {
        runningCount <- dataAccess.workflowQuery.countWorkflows(WorkflowStatuses.runningStatuses)
        reservedWorkflowRecs <- reserveWorkflowBatch(dataAccess, runningCount)
      } yield reservedWorkflowRecs
    }

    // flip the workflows to Launching in a separate txn.
    // if this optimistic-lock-exceptions with another txn, this one will barf and we'll reschedule when we pipe it back to ourselves
    workflowRecsToLaunch flatMap {
      case Some((wfRecs, submissionRec, workspaceRec)) if wfRecs.nonEmpty =>
        // implicitly passed to WorkflowComponent.batchUpdateStatus
        implicit val wfStatusCounter = (status: WorkflowStatus) =>
          if (trackDetailedSubmissionMetrics)
            Option(
              workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceRec.toWorkspaceName, submissionRec.id))(
                status
              )
            )
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

  def reserveWorkflowBatch(dataAccess: DataAccess, activeCount: Int)(implicit
    executionContext: ExecutionContext
  ): ReadWriteAction[Option[(Seq[WorkflowRecord], SubmissionRecord, WorkspaceRecord)]] =
    if (activeCount > maxActiveWorkflowsTotal) {
      logger.warn(
        s"There are $activeCount active workflows which is beyond the total active cap of $maxActiveWorkflowsTotal. Workflows will not be submitted."
      )
      DBIO.successful(None)
    } else {
      for {
        // we exclude workflows submitted by users that have exceeded the max workflows per user
        excludedUsersMap <- dataAccess.workflowQuery
          .listSubmittersWithMoreWorkflowsThan(maxActiveWorkflowsPerUser, WorkflowStatuses.runningStatuses)
          .map(_.toMap)
        excludedUsers = excludedUsersMap.keys.toSeq
        excludedStatuses = SubmissionStatuses.terminalStatuses :+ SubmissionStatuses.Aborting
        workflowRecs: Seq[WorkflowRecord] <- dataAccess.workflowQuery
          .findQueuedWorkflows(excludedUsers, excludedStatuses)
          .take(batchSize)
          .result
        reservedRecs <-
          if (workflowRecs.isEmpty) {
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

  def getRuntimeOptions(googleProjectId: String, bucketName: String)(implicit
    executionContext: ExecutionContext
  ): Future[Option[JsValue]] = {

    def updateRuntimeOptions(zones: List[String]): Option[JsValue] =
      defaultRuntimeOptions match {
        case Some(defaultRuntimeAttrs) =>
          val runtimeAttrsObj = defaultRuntimeAttrs.asJsObject
          Option(JsObject(runtimeAttrsObj.fields + ("zones" -> JsString(zones.mkString(" ")))))
        case None => Option(JsObject("zones" -> JsString(zones.mkString(" "))))
      }

    for {
      regionOption <- googleServicesDAO.getRegionForRegionalBucket(bucketName, None)
      runtimeOptions <- {
        regionOption match {
          case Some(region) =>
            googleServicesDAO
              .getComputeZonesForRegion(GoogleProjectId(googleProjectId), region)
              .map(updateRuntimeOptions)
          case None =>
            // if the location is `multi-region` we want the default zones from config
            Future.successful(defaultRuntimeOptions)
        }
      }
    } yield runtimeOptions
  }

  def buildWorkflowOpts(workspace: WorkspaceRecord,
                        submission: SubmissionRecord,
                        userEmail: RawlsUserEmail,
                        petSAJson: String,
                        billingProject: RawlsBillingProject,
                        useCallCache: Boolean,
                        deleteIntermediateOutputFiles: Boolean,
                        useReferenceDisks: Boolean,
                        memoryRetryMultiplier: Double,
                        workflowFailureMode: Option[WorkflowFailureMode],
                        runtimeOptions: Option[JsValue],
                        ignoreEmptyOutputs: Boolean,
                        monitoringScript: Option[String],
                        monitoringImage: Option[String],
                        monitoringImageScript: Option[String]
  ): ExecutionServiceWorkflowOptions = {
    val petSAEmail = petSAJson.parseJson.asJsObject.getFields("client_email").headOption match {
      case Some(JsString(value)) => value
      case Some(x) => throw new RawlsException(s"unexpected json value for client_email [$x] in service account key")
      case None    => throw new RawlsException(s"client_email missing for service account key json")
    }

    ExecutionServiceWorkflowOptions(
      submission.submissionRoot,
      // Intermediate/final output separation: location 2/2
      // Final outputs are moved to the directory specified
      // Cromwell `/outputs` endpoint and Terra data table use this location
      Option(s"gs://${workspace.bucketName}/submissions/final-outputs/${submission.id}"),
      Option("move"),
      workspace.googleProjectId,
      userEmail.value,
      petSAEmail,
      petSAJson,
      s"${submission.submissionRoot}/workflow.logs",
      runtimeOptions,
      useCallCache,
      deleteIntermediateOutputFiles,
      useReferenceDisks,
      memoryRetryMultiplier,
      highSecurityNetworkCromwellBackend,
      workflowFailureMode,
      google_labels = Map("terra-submission-id" -> s"terra-${submission.id.toString}"),
      ignoreEmptyOutputs,
      monitoringScript,
      monitoringImage,
      monitoringImageScript
    )
  }

  def getWdl(methodConfig: MethodConfiguration, userInfo: UserInfo)(implicit
    executionContext: ExecutionContext
  ): Future[WDL] =
    dataSource.inTransaction {
      _ => // this is a transaction that makes no database calls, but the sprawling stack of withFoos was too hard to unpick :(
        withMethod(methodConfig.methodRepoMethod, userInfo) { wdl: WDL =>
          DBIO.successful(wdl)
        }
    }

  def getWorkflowRecordBatch(workflowIds: Seq[Long], dataAccess: DataAccess)(implicit
    executionContext: ExecutionContext
  ) =
    dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result map { wfRecs =>
      val submissionId = wfRecs.head.submissionId
      if (!wfRecs.forall(_.submissionId == submissionId)) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.InternalServerError, "Workflow batch has different submissions!")
        )
      }
      wfRecs
    }

  def reifyWorkflowRecords(workflowRecs: Seq[WorkflowRecord], dataAccess: DataAccess)(implicit
    executionContext: ExecutionContext
  ) =
    DBIO.sequence(workflowRecs map { wfRec =>
      dataAccess.workflowQuery.loadWorkflow(wfRec) map (_.getOrElse(
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.InternalServerError, s"Failed to load workflow id ${wfRec.id}")
        )
      ))
    })

  def execServiceFailureMessages(failure: ExecutionServiceFailure): Seq[AttributeString] =
    Seq(AttributeString(failure.message)) ++ failure.errors.getOrElse(JsArray()).elements.map {
      case err: JsString => AttributeString(err.convertTo[String]) // otherwise spray escapes the quotes
      case err: JsValue  => AttributeString(err.toString)
    }

  private def resolveDrsUriServiceAccounts(drsUris: Set[String], userInfo: UserInfo)(implicit
    executionContext: ExecutionContext
  ): Future[Set[String]] =
    Future
      .traverse(drsUris) { drsUri =>
        drsResolver.drsServiceAccountEmail(drsUri, userInfo)
      }
      .map { emails =>
        val collected = emails.collect { case Some(email) =>
          email
        }
        logger.debug(s"resolveDrsUriServiceAccounts found ${collected.size} emails for ${drsUris.size} DRS URIs")
        collected
      }

  private def collectDosUris(workflowBatch: Seq[Workflow]): Set[String] = {
    val dosUris = for {
      workflow <- workflowBatch
      inputResolutions <- workflow.inputResolutions
      attribute <- inputResolutions.value.toSeq // toSeq makes the for comp work
      dosAttributeValue <- attribute match {
        case AttributeString(s) if s.matches(DrsResolver.dosDrsUriPattern) => Seq(s)
        case AttributeValueList(valueList) =>
          valueList.collect {
            case AttributeString(s) if s.value.matches(DrsResolver.dosDrsUriPattern) => s
          }
        case _ => Seq.empty
      }
    } yield dosAttributeValue
    dosUris.toSet
  }

  // submit the batch of workflows with the given ids
  def submitWorkflowBatch(
    batch: WorkflowBatch
  )(implicit executionContext: ExecutionContext): Future[WorkflowSubmissionMessage] = {

    val WorkflowBatch(workflowIds, submissionRec, workspaceRec) = batch

    logger.info(
      s"Submitting batch of ${workflowIds.size} workflows for submission ${submissionRec.id} in workspace ${workspaceRec.namespace}/${workspaceRec.name} (${workspaceRec.id})"
    )

    // implicitly passed to WorkflowComponent methods which update status
    implicit val wfStatusCounter = (status: WorkflowStatus) =>
      if (trackDetailedSubmissionMetrics)
        Option(
          workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceRec.toWorkspaceName, submissionRec.id))(
            status
          )
        )
      else None

    // split out the db transaction from the calls to external services
    // DON'T make http calls in here!
    val dbThingsFuture = dataSource.inTransaction { dataAccess =>
      for {
        // Load a bunch of things we'll need to reconstruct information:
        // The list of workflows in this submission
        wfRecs <- getWorkflowRecordBatch(workflowIds, dataAccess)
        workflowBatch <- reifyWorkflowRecords(wfRecs, dataAccess)

        // The workspace's billing project
        billingProject <- dataAccess.rawlsBillingProjectQuery
          .load(RawlsBillingProjectName(workspaceRec.namespace))
          .map(_.get)

        // the method configuration, in order to get the wdl
        methodConfig <- dataAccess.methodConfigurationQuery
          .loadMethodConfigurationById(submissionRec.methodConfigurationId)
          .map(_.get)
      } yield (wfRecs, workflowBatch, billingProject, methodConfig)
    }

    val workflowBatchFuture = for {
      // yank things from the db. note this future has already started running and we're just waiting on it here
      (wfRecs, workflowBatch, billingProject, methodConfig) <- dbThingsFuture
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(GoogleProjectId(workspaceRec.googleProjectId),
                                                         RawlsUserEmail(submissionRec.submitterEmail)
      )
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
      wdl <- getWdl(methodConfig, petUserInfo)
      updatedRuntimeOptions <- getRuntimeOptions(workspaceRec.googleProjectId, workspaceRec.bucketName)
    } yield {

      val wfOpts = buildWorkflowOpts(
        workspace = workspaceRec,
        submission = submissionRec,
        userEmail = RawlsUserEmail(submissionRec.submitterEmail),
        petSAJson = petSAJson,
        billingProject = billingProject,
        useCallCache = submissionRec.useCallCache,
        deleteIntermediateOutputFiles = submissionRec.deleteIntermediateOutputFiles,
        useReferenceDisks = submissionRec.useReferenceDisks,
        memoryRetryMultiplier = submissionRec.memoryRetryMultiplier,
        workflowFailureMode = WorkflowFailureModes.withNameOpt(submissionRec.workflowFailureMode),
        runtimeOptions = updatedRuntimeOptions,
        ignoreEmptyOutputs = submissionRec.ignoreEmptyOutputs,
        monitoringScript = submissionRec.monitoringScript,
        monitoringImage = submissionRec.monitoringImage,
        monitoringImageScript = submissionRec.monitoringImageScript
      )
      val submissionAndWorkspaceLabels =
        Map("submission-id" -> submissionRec.id.toString, "workspace-id" -> workspaceRec.id.toString)
      val wfLabels = workspaceRec.workflowCollection match {
        case Some(workflowCollection) if useWorkflowCollectionLabel =>
          submissionAndWorkspaceLabels + ("caas-collection-name" -> workflowCollection)
        case _ => submissionAndWorkspaceLabels
      }
      val wfCollection = if (useWorkflowCollectionField) workspaceRec.workflowCollection else None

      val wfInputsBatch = workflowBatch map { wf =>
        val methodProps = wf.inputResolutions collect {
          case svv: SubmissionValidationValue if svv.value.isDefined =>
            svv.inputName -> svv.value.get
        }
        methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
      }

      // yield the things we're going to submit to Cromwell
      val dosUris = collectDosUris(workflowBatch)
      logger.debug(
        s"collectDosUris found ${dosUris.size} DOS URIs in batch of size ${workflowBatch.size} for submission ${submissionRec.id}. First 20 are: ${dosUris
            .take(20)}"
      )
      (wdl, wfRecs, wfInputsBatch, wfOpts, wfLabels, wfCollection, dosUris, petUserInfo, methodConfig)
    }

    import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat
    val cromwellSubmission = for {
      (wdl, workflowRecs, wfInputsBatch, wfOpts, wfLabels, wfCollection, dosUris, petUserInfo, methodConfig) <-
        workflowBatchFuture
      dosServiceAccounts <- resolveDrsUriServiceAccounts(dosUris, petUserInfo)
      // For Jade, HCA, anyone who doesn't use Bond, we won't get an SA back and the following line is a no-op
      // We still call DRSHub for those because we can verify the user has permission on the DRS object as
      // early as possible, rather than letting the workflow(s) launch and fail
      // AEN 2020-09-08 [WA-325]
      _ <-
        if (dosServiceAccounts.isEmpty) Future.successful(false)
        else
          googleServicesDAO.addPolicyBindings(GoogleProjectId(wfOpts.google_project),
                                              Map(requesterPaysRole -> dosServiceAccounts.map("serviceAccount:" + _))
          )
      // Should labels be an Option? It's not optional for rawls (but then wfOpts are options too)
      workflowSubmitResult <- executionServiceCluster.submitWorkflows(workflowRecs,
                                                                      wdl,
                                                                      wfInputsBatch,
                                                                      Option(wfOpts.toJson.toString),
                                                                      Option(wfLabels),
                                                                      wfCollection,
                                                                      petUserInfo
      )
    } yield {
      // call to submitWorkflows returns a tuple:
      val executionServiceKey = workflowSubmitResult._1
      val executionServiceResults = workflowSubmitResult._2

      // Emit metric for time until Cromwell has processed workflow.
      val elapsedTime = System.currentTimeMillis() - submissionRec.submissionDate.getTime
      workflowToCromwellLatency.update(elapsedTime, TimeUnit.MILLISECONDS)

      (executionServiceKey, workflowRecs.zip(executionServiceResults), methodConfig, petUserInfo)
    }

    // Second txn to update workflows to Launching.
    // If this txn fails we'll just end up rescheduling the next workflow query and will restart this function from the top.
    // Since the first txn didn't do any writes to the db it won't be left in a weird halfway state.
    cromwellSubmission flatMap { case (executionServiceKey, results, methodConfig, petUserInfo) =>
      dataSource.inTransaction { dataAccess =>
        // save successes as submitted workflows and hook up their cromwell ids
        val successUpdates = results collect { case (wfRec, Left(success: ExecutionServiceStatus)) =>
          val updatedWfRec = wfRec.copy(externalId = Option(success.id),
                                        status = success.status,
                                        executionServiceKey = Option(executionServiceKey.toString)
          )
          dataAccess.workflowQuery.updateWorkflowRecord(updatedWfRec)
        }

        // save error messages into failures and flip them to Failed
        val failures = results collect { case (wfRec, Right(failure: ExecutionServiceFailure)) =>
          (wfRec, failure)
        }
        val failureMessages = failures map { case (wfRec, failure) =>
          dataAccess.workflowQuery.saveMessages(execServiceFailureMessages(failure), wfRec.id)
        }
        val failureStatusUpd = dataAccess.workflowQuery.batchUpdateStatusAndExecutionServiceKey(failures.map(_._1),
                                                                                                WorkflowStatuses.Failed,
                                                                                                executionServiceKey
        )
        val submissionEvent = SubmissionEvent(
          submissionRec.id.toString,
          workspaceRec.id.toString,
          methodConfig.toId,
          methodConfig.namespace,
          methodConfig.name,
          methodConfig.methodRepoMethod.methodUri,
          methodConfig.methodRepoMethod.repo.scheme,
          methodConfig.methodConfigVersion,
          methodConfig.dataReferenceName.map(_.toString()),
          workflowIds,
          results
            .filter(_._2.isLeft)
            .flatMap(
              _._2.swap.toOption.map(_.id)
            ), // This is because the Left is the success, breaking Scala's Either convention
          submissionRec.rootEntityType,
          submissionRec.useCallCache,
          submissionRec.useReferenceDisks,
          submissionRec.memoryRetryMultiplier,
          submissionRec.ignoreEmptyOutputs,
          submissionRec.userComment
        )
        bardService.sendEvent(submissionEvent, petUserInfo)

        DBIO.seq((successUpdates ++ failureMessages :+ failureStatusUpd): _*)
      } map { _ => ProcessNextWorkflow }
    } recoverWith {
      // If any of this fails, set all workflows to failed with whatever message we have.
      case t: Throwable =>
        dataSource.inTransaction { dataAccess =>
          val message = Option(t.getMessage).getOrElse(t.getClass.getName)
          dataAccess.workflowQuery.findWorkflowByIds(workflowIds).result flatMap { wfRecs =>
            dataAccess.workflowQuery.batchUpdateStatus(wfRecs, WorkflowStatuses.Failed)
          } andThen
            DBIO.sequence(workflowIds map { id =>
              dataAccess.workflowQuery.saveMessages(Seq(AttributeString(message)), id)
            })
        } map { _ => throw t }
    }
  }
}
