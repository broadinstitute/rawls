package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import cats.effect.IO
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus.{optionValueReader, toFicusConfig}
import org.broadinstitute.dsde.rawls.billing.{
  BillingProfileManagerDAO,
  BillingProjectLifecycle,
  BillingRepository,
  BpmBillingProjectLifecycle,
  GoogleBillingProjectLifecycle
}
import org.broadinstitute.dsde.rawls.coordination.{
  CoordinatedDataSourceAccess,
  CoordinatedDataSourceActor,
  DataSourceAccess,
  UncoordinatedDataSourceAccess
}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsResolver
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{
  MethodConfigResolver,
  SubmissionMonitorConfig,
  SubmissionSupervisor,
  WorkflowSubmissionActor
}
import org.broadinstitute.dsde.rawls.model.{CromwellBackend, RawlsRequestContext, WorkflowStatuses}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationActor
import org.broadinstitute.dsde.rawls.monitor.workspace.WorkspaceResourceMonitor
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.{
  BPMBillingProjectDeleteRunner,
  CloneWorkspaceContainerRunner,
  LandingZoneCreationStatusRunner
}
import org.broadinstitute.dsde.rawls.util
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

//noinspection ScalaUnnecessaryParentheses,ScalaUnusedSymbol,TypeAnnotation
// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def bootMonitors(system: ActorSystem,
                   conf: Config,
                   slickDataSource: SlickDataSource,
                   gcsDAO: HttpGoogleServicesDAO,
                   googleIamDAO: GoogleIamDAO,
                   samDAO: SamDAO,
                   notificationDAO: NotificationDAO,
                   pubSubDAO: GooglePubSubDAO,
                   importServicePubSubDAO: GooglePubSubDAO,
                   importServiceDAO: HttpImportServiceDAO,
                   workspaceManagerDAO: WorkspaceManagerDAO,
                   billingProfileManagerDAO: BillingProfileManagerDAO,
                   googleStorage: GoogleStorageService[IO],
                   googleStorageTransferService: GoogleStorageTransferService[IO],
                   methodRepoDAO: MethodRepoDAO,
                   drsResolver: DrsResolver,
                   entityService: RawlsRequestContext => EntityService,
                   workspaceService: RawlsRequestContext => WorkspaceService,
                   shardedExecutionServiceCluster: ExecutionServiceCluster,
                   maxActiveWorkflowsTotal: Int,
                   maxActiveWorkflowsPerUser: Int,
                   projectTemplate: ProjectTemplate,
                   metricsPrefix: String,
                   requesterPaysRole: String,
                   useWorkflowCollectionField: Boolean,
                   useWorkflowCollectionLabel: Boolean,
                   defaultNetworkCromwellBackend: CromwellBackend,
                   highSecurityNetworkCromwellBackend: CromwellBackend,
                   methodConfigResolver: MethodConfigResolver
  ): Unit = {
    // Reset "Launching" workflows to "Queued"
    resetLaunchingWorkflows(slickDataSource)

    // Boot billing project creation monitor
    startCreatingBillingProjectMonitor(system, slickDataSource, gcsDAO, samDAO, projectTemplate, requesterPaysRole)

    // Boot data source access
    val dataSourceAccess = startDataSourceAccess(system, conf, slickDataSource)

    // Boot submission monitor supervisor
    val submissionmonitorConfigRoot = conf.getConfig("submissionmonitor")
    val submissionMonitorConfig = SubmissionMonitorConfig(
      util.toScalaDuration(submissionmonitorConfigRoot.getDuration("submissionPollInterval")),
      submissionmonitorConfigRoot.getBoolean("trackDetailedSubmissionMetrics"),
      submissionmonitorConfigRoot.getInt("attributeUpdatesPerWorkflow"),
      submissionmonitorConfigRoot.getBoolean("enableEmailNotifications")
    )
    startSubmissionMonitorSupervisor(
      system,
      submissionMonitorConfig,
      dataSourceAccess,
      samDAO,
      gcsDAO,
      notificationDAO,
      shardedExecutionServiceCluster,
      metricsPrefix
    )

    // Boot workflow submission actors
    startWorkflowSubmissionActors(
      system,
      conf,
      slickDataSource,
      gcsDAO,
      samDAO,
      methodRepoDAO,
      drsResolver,
      shardedExecutionServiceCluster,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      metricsPrefix,
      requesterPaysRole,
      useWorkflowCollectionField,
      useWorkflowCollectionLabel,
      defaultNetworkCromwellBackend,
      highSecurityNetworkCromwellBackend,
      methodConfigResolver
    )

    // Boot bucket deletion monitor
    startBucketDeletionMonitor(system, slickDataSource, gcsDAO)

    val workspaceBillingAccountMonitorConfigRoot = conf.getConfig("workspace-billing-account-monitor")
    val workspaceBillingAccountMonitorConfig = BillingAccountSynchronizerConfig(
      util.toScalaDuration(workspaceBillingAccountMonitorConfigRoot.getDuration("pollInterval")),
      util.toScalaDuration(workspaceBillingAccountMonitorConfigRoot.getDuration("initialDelay"))
    )
    // Boot workspace billing account monitor
    startBillingAccountChangeSynchronizer(system, workspaceBillingAccountMonitorConfig, slickDataSource, gcsDAO, samDAO)

    val cloneWorkspaceFileTransferMonitorConfigRoot = conf.getConfig("clone-workspace-file-transfer-monitor")
    val cloneWorkspaceFileTransferMonitorConfig = CloneWorkspaceFileTransferMonitorConfig(
      util.toScalaDuration(cloneWorkspaceFileTransferMonitorConfigRoot.getDuration("pollInterval")),
      util.toScalaDuration(cloneWorkspaceFileTransferMonitorConfigRoot.getDuration("initialDelay"))
    )
    startCloneWorkspaceFileTransferMonitor(system, cloneWorkspaceFileTransferMonitorConfig, slickDataSource, gcsDAO)

    // Boot entity statistics cache monitor
    if (conf.getBoolean("entityStatisticsCache.enabled")) {
      startEntityStatisticsCacheMonitor(
        system,
        slickDataSource,
        util.toScalaDuration(conf.getDuration("entityStatisticsCache.timeoutPerWorkspace")),
        util.toScalaDuration(conf.getDuration("entityStatisticsCache.standardPollInterval")),
        util.toScalaDuration(conf.getDuration("entityStatisticsCache.workspaceCooldown")),
        metricsPrefix
      )
    }

    val avroUpsertMonitorConfig = AvroUpsertMonitorConfig(
      util.toScalaDuration(conf.getDuration("avroUpsertMonitor.pollInterval")),
      util.toScalaDuration(conf.getDuration("avroUpsertMonitor.pollJitter")),
      conf.getString("avroUpsertMonitor.importRequestPubSubTopic"),
      conf.getString("avroUpsertMonitor.importRequestPubSubSubscription"),
      conf.getString("avroUpsertMonitor.updateImportStatusPubSubTopic"),
      conf.getInt("avroUpsertMonitor.ackDeadlineSeconds"),
      conf.getInt("avroUpsertMonitor.batchSize"),
      conf.getInt("avroUpsertMonitor.workerCount")
    )

    // Boot the avro upsert monitor to read and process messages in the specified PubSub topic
    startAvroUpsertMonitor(system,
                           entityService,
                           gcsDAO,
                           samDAO,
                           googleStorage,
                           pubSubDAO,
                           importServicePubSubDAO,
                           importServiceDAO,
                           avroUpsertMonitorConfig,
                           slickDataSource
    )

    startWorkspaceMigrationActor(system,
                                 conf,
                                 gcsDAO,
                                 googleIamDAO,
                                 slickDataSource,
                                 workspaceService,
                                 googleStorage,
                                 googleStorageTransferService,
                                 samDAO
    )

    startWorkspaceResourceMonitor(
      system,
      conf,
      slickDataSource,
      samDAO,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      gcsDAO
    )

  }

  private def startCreatingBillingProjectMonitor(system: ActorSystem,
                                                 slickDataSource: SlickDataSource,
                                                 gcsDAO: GoogleServicesDAO,
                                                 samDAO: SamDAO,
                                                 projectTemplate: ProjectTemplate,
                                                 requesterPaysRole: String
  ): Unit =
    system.actorOf(
      CreatingBillingProjectMonitor.props(slickDataSource, gcsDAO, samDAO, projectTemplate, requesterPaysRole)
    )

  private def startDataSourceAccess(system: ActorSystem,
                                    conf: Config,
                                    slickDataSource: SlickDataSource
  ): DataSourceAccess = {
    val coordinatedAccessConfigRoot = conf.getConfig("data-source.coordinated-access")
    if (coordinatedAccessConfigRoot.getBoolean("enabled")) {
      val startTimeout = util.toScalaDuration(coordinatedAccessConfigRoot.getDuration("start-timeout"))
      val waitTimeout = util.toScalaDuration(coordinatedAccessConfigRoot.getDuration("wait-timeout"))
      val askTimeout = util.toScalaDuration(coordinatedAccessConfigRoot.getDuration("ask-timeout"))
      val dataSourceActor = system.actorOf(CoordinatedDataSourceActor.props(), "rawls-coordinated-store-access")
      val dataSourceAccess = new CoordinatedDataSourceAccess(
        slickDataSource = slickDataSource,
        dataSourceActor = dataSourceActor,
        starTimeout = startTimeout,
        waitTimeout = waitTimeout,
        askTimeout = askTimeout
      )
      logger.info(
        "Started coordinated data source access " +
          s"with timeouts (start / wait / ask) = ($startTimeout / $waitTimeout / $askTimeout)"
      )
      dataSourceAccess
    } else {
      val dataSourceAccess = new UncoordinatedDataSourceAccess(slickDataSource)
      logger.info("Running with uncoordinated data source access")
      dataSourceAccess
    }
  }

  private def startSubmissionMonitorSupervisor(system: ActorSystem,
                                               submissionMonitorConfig: SubmissionMonitorConfig,
                                               storeAccess: DataSourceAccess,
                                               samDAO: SamDAO,
                                               gcsDAO: GoogleServicesDAO,
                                               notificationDAO: NotificationDAO,
                                               shardedExecutionServiceCluster: ExecutionServiceCluster,
                                               metricsPrefix: String
  ) =
    system.actorOf(
      SubmissionSupervisor.props(
        shardedExecutionServiceCluster,
        storeAccess,
        samDAO,
        gcsDAO,
        notificationDAO,
        gcsDAO.getBucketServiceAccountCredential,
        submissionMonitorConfig,
        workbenchMetricBaseName = metricsPrefix
      ),
      "rawls-submission-supervisor"
    )

  private def startWorkflowSubmissionActors(system: ActorSystem,
                                            conf: Config,
                                            slickDataSource: SlickDataSource,
                                            gcsDAO: GoogleServicesDAO,
                                            samDAO: SamDAO,
                                            methodRepoDAO: MethodRepoDAO,
                                            drsResolver: DrsResolver,
                                            shardedExecutionServiceCluster: ExecutionServiceCluster,
                                            maxActiveWorkflowsTotal: Int,
                                            maxActiveWorkflowsPerUser: Int,
                                            metricsPrefix: String,
                                            requesterPaysRole: String,
                                            useWorkflowCollectionField: Boolean,
                                            useWorkflowCollectionLabel: Boolean,
                                            defaultNetworkCromwellBackend: CromwellBackend,
                                            highSecurityNetworkCromwellBackend: CromwellBackend,
                                            methodConfigResolver: MethodConfigResolver
  ) =
    for (i <- 0 until conf.getInt("executionservice.parallelSubmitters"))
      system.actorOf(
        WorkflowSubmissionActor.props(
          slickDataSource,
          methodRepoDAO,
          gcsDAO,
          samDAO,
          drsResolver,
          shardedExecutionServiceCluster,
          conf.getInt("executionservice.batchSize"),
          gcsDAO.getBucketServiceAccountCredential,
          util.toScalaDuration(conf.getDuration("executionservice.processInterval")),
          util.toScalaDuration(conf.getDuration("executionservice.pollInterval")),
          maxActiveWorkflowsTotal,
          maxActiveWorkflowsPerUser,
          Try(
            conf.getObject("executionservice.defaultRuntimeOptions").render(ConfigRenderOptions.concise()).parseJson
          ).toOption,
          conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
          metricsPrefix,
          requesterPaysRole,
          useWorkflowCollectionField,
          useWorkflowCollectionLabel,
          defaultNetworkCromwellBackend,
          highSecurityNetworkCromwellBackend,
          methodConfigResolver
        )
      )

  private def startBucketDeletionMonitor(system: ActorSystem,
                                         slickDataSource: SlickDataSource,
                                         gcsDAO: GoogleServicesDAO
  ) =
    system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO, 10 seconds, 6 hours))

  private def startBillingAccountChangeSynchronizer(
    system: ActorSystem,
    workspaceBillingAccountMonitorConfig: BillingAccountSynchronizerConfig,
    slickDataSource: SlickDataSource,
    gcsDAO: GoogleServicesDAO,
    samDAO: SamDAO
  ) =
    system.spawn(
      BillingAccountChangeSynchronizer(slickDataSource,
                                       gcsDAO,
                                       samDAO,
                                       workspaceBillingAccountMonitorConfig.initialDelay,
                                       workspaceBillingAccountMonitorConfig.pollInterval
      ),
      name = "BillingAccountChangeSynchronizer"
    )

  private def startCloneWorkspaceFileTransferMonitor(
    system: ActorSystem,
    cloneWorkspaceFileTransferMonitorConfig: CloneWorkspaceFileTransferMonitorConfig,
    slickDataSource: SlickDataSource,
    gcsDAO: GoogleServicesDAO
  ) =
    system.actorOf(
      CloneWorkspaceFileTransferMonitor
        .props(slickDataSource,
               gcsDAO,
               cloneWorkspaceFileTransferMonitorConfig.initialDelay,
               cloneWorkspaceFileTransferMonitorConfig.pollInterval
        )
        .withDispatcher("clone-workspace-file-transfer-monitor-dispatcher")
    )

  private def startEntityStatisticsCacheMonitor(system: ActorSystem,
                                                slickDataSource: SlickDataSource,
                                                timeoutPerWorkspace: Duration,
                                                standardPollInterval: FiniteDuration,
                                                workspaceCooldown: FiniteDuration,
                                                workbenchMetricBaseName: String
  ) =
    system.actorOf(
      EntityStatisticsCacheMonitor.props(slickDataSource,
                                         timeoutPerWorkspace,
                                         standardPollInterval,
                                         workspaceCooldown,
                                         workbenchMetricBaseName
      )
    )

  private def startAvroUpsertMonitor(system: ActorSystem,
                                     entityService: RawlsRequestContext => EntityService,
                                     googleServicesDAO: GoogleServicesDAO,
                                     samDAO: SamDAO,
                                     googleStorage: GoogleStorageService[IO],
                                     googlePubSubDAO: GooglePubSubDAO,
                                     importServicePubSubDAO: GooglePubSubDAO,
                                     importServiceDAO: HttpImportServiceDAO,
                                     avroUpsertMonitorConfig: AvroUpsertMonitorConfig,
                                     dataSource: SlickDataSource
  ) =
    system.actorOf(
      AvroUpsertMonitorSupervisor.props(
        entityService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        googlePubSubDAO,
        importServicePubSubDAO,
        importServiceDAO,
        avroUpsertMonitorConfig,
        dataSource
      )
    )

  private def startWorkspaceResourceMonitor(
    system: ActorSystem,
    config: Config,
    dataSource: SlickDataSource,
    samDAO: SamDAO,
    workspaceManagerDAO: WorkspaceManagerDAO,
    billingProfileManagerDAO: BillingProfileManagerDAO,
    gcsDAO: GoogleServicesDAO
  ) = {
    val billingRepo = new BillingRepository(dataSource)

    system.actorOf(
      WorkspaceResourceMonitor.props(
        config,
        dataSource,
        Map(
          JobType.AzureLandingZoneResult ->
            new LandingZoneCreationStatusRunner(samDAO, workspaceManagerDAO, billingRepo, gcsDAO),
          JobType.CloneWorkspaceContainerResult ->
            new CloneWorkspaceContainerRunner(samDAO, workspaceManagerDAO, dataSource, gcsDAO),
          JobType.BpmBillingProjectDelete -> new BPMBillingProjectDeleteRunner(
            samDAO,
            gcsDAO,
            workspaceManagerDAO,
            billingRepo,
            new BpmBillingProjectLifecycle(samDAO,
                                           billingRepo,
                                           billingProfileManagerDAO,
                                           workspaceManagerDAO,
                                           WorkspaceManagerResourceMonitorRecordDao(dataSource)
            )
          )
        )
      )
    )
  }

  private def startWorkspaceMigrationActor(system: ActorSystem,
                                           config: Config,
                                           gcsDao: HttpGoogleServicesDAO,
                                           googleIamDAO: GoogleIamDAO,
                                           dataSource: SlickDataSource,
                                           workspaceService: RawlsRequestContext => WorkspaceService,
                                           storageService: GoogleStorageService[IO],
                                           storageTransferService: GoogleStorageTransferService[IO],
                                           samDao: SamDAO
  ) =
    config.as[Option[WorkspaceMigrationActor.Config]]("workspace-migration").foreach { actorConfig =>
      system.spawn(
        WorkspaceMigrationActor(
          actorConfig,
          dataSource,
          workspaceService,
          storageService,
          storageTransferService,
          gcsDao,
          googleIamDAO,
          samDao
        ).behavior,
        "WorkspaceMigrationActor"
      )
    }

  private def resetLaunchingWorkflows(dataSource: SlickDataSource) =
    Await.result(dataSource.inTransaction { dataAccess =>
                   dataAccess.workflowQuery.batchUpdateStatus(WorkflowStatuses.Launching, WorkflowStatuses.Queued)
                 },
                 10 seconds
    )
}
