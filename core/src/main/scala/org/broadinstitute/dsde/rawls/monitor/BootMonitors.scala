package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource, _}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionSupervisor, WorkflowSubmissionActor, SubmissionMonitorConfig}
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkflowStatuses}
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def bootMonitors(system: ActorSystem,
                   conf: Config,
                   slickDataSource: SlickDataSource,
                   gcsDAO: GoogleServicesDAO,
                   samDAO: SamDAO,
                   pubSubDAO: GooglePubSubDAO,
                   methodRepoDAO: MethodRepoDAO,
                   dosResolver: DosResolver,
                   shardedExecutionServiceCluster: ExecutionServiceCluster,
                   maxActiveWorkflowsTotal: Int,
                   maxActiveWorkflowsPerUser: Int,
                   userServiceConstructor: (UserInfo) => UserService,
                   projectTemplate: ProjectTemplate,
                   metricsPrefix: String,
                   requesterPaysRole: String,
                   useWorkflowCollectionField: Boolean,
                   useWorkflowCollectionLabel: Boolean): Unit = {
    //Reset "Launching" workflows to "Queued"
    resetLaunchingWorkflows(slickDataSource)

    //Boot billing project creation monitor
    startCreatingBillingProjectMonitor(system, slickDataSource, gcsDAO, samDAO, projectTemplate, requesterPaysRole)

    //Boot submission monitor supervisor
    val submissionmonitorConfigRoot = conf.getConfig("submissionmonitor")
    val submissionMonitorConfig = SubmissionMonitorConfig(util.toScalaDuration(submissionmonitorConfigRoot.getDuration("submissionPollInterval")), submissionmonitorConfigRoot.getBoolean("trackDetailedSubmissionMetrics"))
    startSubmissionMonitorSupervisor(system, submissionMonitorConfig, slickDataSource, samDAO, gcsDAO, shardedExecutionServiceCluster, metricsPrefix)

    //Boot workflow submission actors
    startWorkflowSubmissionActors(system, conf, slickDataSource, gcsDAO, samDAO, methodRepoDAO, dosResolver, shardedExecutionServiceCluster, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, metricsPrefix, requesterPaysRole, useWorkflowCollectionField, useWorkflowCollectionLabel)

    //Boot bucket deletion monitor
    startBucketDeletionMonitor(system, slickDataSource, gcsDAO)
  }

  private def startCreatingBillingProjectMonitor(system: ActorSystem, slickDataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, samDAO: SamDAO, projectTemplate: ProjectTemplate, requesterPaysRole: String): Unit = {
    system.actorOf(CreatingBillingProjectMonitor.props(slickDataSource, gcsDAO, samDAO, projectTemplate, requesterPaysRole))
  }

  private def startSubmissionMonitorSupervisor(system: ActorSystem, submissionMonitorConfig: SubmissionMonitorConfig, slickDataSource: SlickDataSource, samDAO: SamDAO, gcsDAO: GoogleServicesDAO, shardedExecutionServiceCluster: ExecutionServiceCluster, metricsPrefix: String) = {
    system.actorOf(SubmissionSupervisor.props(
      shardedExecutionServiceCluster,
      slickDataSource,
      samDAO,
      gcsDAO,
      gcsDAO.getBucketServiceAccountCredential,
      submissionMonitorConfig,
      workbenchMetricBaseName = metricsPrefix
    ), "rawls-submission-supervisor")
  }

  private def startWorkflowSubmissionActors(system: ActorSystem,
                                            conf: Config,
                                            slickDataSource: SlickDataSource,
                                            gcsDAO: GoogleServicesDAO,
                                            samDAO: SamDAO,
                                            methodRepoDAO: MethodRepoDAO,
                                            dosResolver: DosResolver,
                                            shardedExecutionServiceCluster: ExecutionServiceCluster,
                                            maxActiveWorkflowsTotal: Int,
                                            maxActiveWorkflowsPerUser: Int,
                                            metricsPrefix: String,
                                            requesterPaysRole: String,
                                            useWorkflowCollectionField: Boolean,
                                            useWorkflowCollectionLabel: Boolean) = {
    for(i <- 0 until conf.getInt("executionservice.parallelSubmitters")) {
      system.actorOf(WorkflowSubmissionActor.props(
        slickDataSource,
        methodRepoDAO,
        gcsDAO,
        samDAO,
        dosResolver,
        shardedExecutionServiceCluster,
        conf.getInt("executionservice.batchSize"),
        gcsDAO.getBucketServiceAccountCredential,
        util.toScalaDuration(conf.getDuration("executionservice.processInterval")),
        util.toScalaDuration(conf.getDuration("executionservice.pollInterval")),
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        Try(conf.getObject("executionservice.defaultRuntimeOptions").render(ConfigRenderOptions.concise()).parseJson).toOption,
        conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
        metricsPrefix,
        requesterPaysRole,
        useWorkflowCollectionField,
        useWorkflowCollectionLabel
      ))
    }
  }

  private def startBucketDeletionMonitor(system: ActorSystem, slickDataSource: SlickDataSource, gcsDAO: GoogleServicesDAO) = {
    system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO, 10 seconds, 6 hours))
  }

  private def resetLaunchingWorkflows(dataSource: SlickDataSource) = {
    Await.result(dataSource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.batchUpdateStatus(WorkflowStatuses.Launching, WorkflowStatuses.Queued)
    }, 10 seconds)
  }
}