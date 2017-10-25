package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.HttpGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionSupervisor, WorkflowSubmissionActor}
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkflowStatuses}
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try

// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def bootMonitors(system: ActorSystem, conf: Config, slickDataSource: SlickDataSource, gcsDAO: HttpGoogleServicesDAO,
                   pubSubDAO: HttpGooglePubSubDAO, methodRepoDAO: HttpMethodRepoDAO, shardedExecutionServiceCluster: ExecutionServiceCluster,
                   maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, bucketDeletionMonitor: ActorRef,
                   userServiceConstructor: (UserInfo) => UserService, projectTemplate: ProjectTemplate, metricsPrefix: String): Unit = {
    //TODO: once bucketDeletionMonitor is broken out and db-triggered, it can be handled the same way as the below monitors
    restartMonitors(slickDataSource, gcsDAO, bucketDeletionMonitor)

    //Boot billing project creation monitor
    startCreatingBillingProjectMonitor(system, slickDataSource, gcsDAO, projectTemplate)

    //Boot google group sync monitor
    val gcsConfig = conf.getConfig("gcs")
    startGoogleGroupSyncMonitor(system, gcsConfig, pubSubDAO, userServiceConstructor)

    //Boot submission monitor
    val submissionMonitorConfig = conf.getConfig("submissionmonitor")
    startSubmissionMonitor(system, submissionMonitorConfig, slickDataSource, gcsDAO, shardedExecutionServiceCluster, metricsPrefix)

    //Boot workflow submission actors
    startWorkflowSubmissionActors(system, conf, slickDataSource, gcsDAO, methodRepoDAO, shardedExecutionServiceCluster, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, metricsPrefix)
  }

  private def startCreatingBillingProjectMonitor(system: ActorSystem, slickDataSource: SlickDataSource, gcsDAO: HttpGoogleServicesDAO, projectTemplate: ProjectTemplate): Unit = {
    system.actorOf(CreatingBillingProjectMonitor.props(slickDataSource, gcsDAO, projectTemplate))
  }

  private def startGoogleGroupSyncMonitor(system: ActorSystem, gcsConfig: Config, pubSubDAO: HttpGooglePubSubDAO, userServiceConstructor: (UserInfo) => UserService) = {
    system.actorOf(GoogleGroupSyncMonitorSupervisor.props(
      util.toScalaDuration(gcsConfig.getDuration("groupMonitor.pollInterval")),
      util.toScalaDuration(gcsConfig.getDuration("groupMonitor.pollIntervalJitter")),
      pubSubDAO,
      gcsConfig.getString("groupMonitor.topicName"),
      gcsConfig.getString("groupMonitor.subscriptionName"),
      gcsConfig.getInt("groupMonitor.workerCount"),
      userServiceConstructor))
  }

  private def startSubmissionMonitor(system: ActorSystem, submissionMonitorConfig: Config, slickDataSource: SlickDataSource,
                                     gcsDAO: HttpGoogleServicesDAO, shardedExecutionServiceCluster: ExecutionServiceCluster, metricsPrefix: String) = {
    system.actorOf(SubmissionSupervisor.props(
      shardedExecutionServiceCluster,
      slickDataSource,
      gcsDAO.getBucketServiceAccountCredential,
      util.toScalaDuration(submissionMonitorConfig.getDuration("submissionPollInterval")),
      submissionMonitorConfig.getBoolean("trackDetailedSubmissionMetrics"),
      workbenchMetricBaseName = metricsPrefix
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")
  }

  private def startWorkflowSubmissionActors(system: ActorSystem, conf: Config, slickDataSource: SlickDataSource, gcsDAO: HttpGoogleServicesDAO, methodRepoDAO: MethodRepoDAO, shardedExecutionServiceCluster: ExecutionServiceCluster, maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, metricsPrefix: String) = {
    for(i <- 0 until conf.getInt("executionservice.parallelSubmitters")) {
      system.actorOf(WorkflowSubmissionActor.props(
        slickDataSource,
        methodRepoDAO,
        gcsDAO,
        shardedExecutionServiceCluster,
        conf.getInt("executionservice.batchSize"),
        gcsDAO.getBucketServiceAccountCredential,
        util.toScalaDuration(conf.getDuration("executionservice.processInterval")),
        util.toScalaDuration(conf.getDuration("executionservice.pollInterval")),
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        Try(conf.getObject("executionservice.defaultRuntimeOptions").render(ConfigRenderOptions.concise()).parseJson).toOption,
        workbenchMetricBaseName = metricsPrefix
      ))
    }
  }

  def restartMonitors(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, bucketDeletionMonitor: ActorRef): Unit = {
    startBucketDeletionMonitor(dataSource, bucketDeletionMonitor)
    resetLaunchingWorkflows(dataSource)
  }

  private def startBucketDeletionMonitor(dataSource: SlickDataSource, bucketDeletionMonitor: ActorRef) = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.pendingBucketDeletionQuery.list() map { _.map { pbd =>
          bucketDeletionMonitor ! DeleteBucket(pbd.bucket)
        }
      }
    } onFailure {
      case t: Throwable => logger.error("Error starting bucket deletion monitor", t)
    }
  }

  private def resetLaunchingWorkflows(dataSource: SlickDataSource) = {
    Await.result(dataSource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.batchUpdateStatus(WorkflowStatuses.Launching, WorkflowStatuses.Queued)
    }, 10 seconds)
  }
}