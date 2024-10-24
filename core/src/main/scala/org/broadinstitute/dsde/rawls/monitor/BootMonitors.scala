package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import cats.effect.IO
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus.{optionValueReader, toFicusConfig}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingProjectDeletion, BillingRepository}
import org.broadinstitute.dsde.rawls.config.{FastPassConfig, RawlsConfigManager}
import org.broadinstitute.dsde.rawls.coordination.{
  CoordinatedDataSourceAccess,
  CoordinatedDataSourceActor,
  DataSourceAccess,
  UncoordinatedDataSourceAccess
}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsResolver
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.fastpass.FastPassMonitor
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{
  MethodConfigResolver,
  SubmissionMonitorConfig,
  SubmissionSupervisor,
  WorkflowSubmissionActor
}
import org.broadinstitute.dsde.rawls.metrics.BardService
import org.broadinstitute.dsde.rawls.model.{
  CromwellBackend,
  RawlsRequestContext,
  WorkflowStatuses,
  WorkspaceCloudPlatform
}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationActor
import org.broadinstitute.dsde.rawls.monitor.workspace.WorkspaceResourceMonitor
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone.WorkspaceCloningRunner
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.WorkspaceDeletionRunner
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.WsmDeletionAction
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.{
  BPMBillingProjectDeleteRunner,
  CloneWorkspaceContainerRunner,
  LandingZoneCreationStatusRunner
}
import org.broadinstitute.dsde.rawls.util
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceService}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.jdk.DurationConverters.JavaDurationOps
import scala.language.postfixOps
import scala.util.Try

//noinspection ScalaUnnecessaryParentheses,ScalaUnusedSymbol,TypeAnnotation
// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def bootMonitors(system: ActorSystem,
                   appConfigManager: RawlsConfigManager,
                   slickDataSource: SlickDataSource,
                   gcsDAO: GoogleServicesDAO,
                   googleIamDAO: GoogleIamDAO,
                   googleStorageDAO: GoogleStorageDAO,
                   samDAO: SamDAO,
                   notificationDAO: NotificationDAO,
                   pubSubDAO: GooglePubSubDAO,
                   cwdsDAO: CwdsDAO,
                   workspaceManagerDAO: WorkspaceManagerDAO,
                   billingProfileManagerDAO: BillingProfileManagerDAO,
                   leonardoDAO: LeonardoDAO,
                   workspaceRepository: WorkspaceRepository,
                   googleStorage: GoogleStorageService[IO],
                   googleStorageTransferService: GoogleStorageTransferService[IO],
                   methodRepoDAO: MethodRepoDAO,
                   drsResolver: DrsResolver,
                   entityService: RawlsRequestContext => EntityService,
                   entityQueryTimeout: Duration,
                   workspaceService: RawlsRequestContext => WorkspaceService,
                   shardedExecutionServiceCluster: ExecutionServiceCluster,
                   maxActiveWorkflowsTotal: Int,
                   maxActiveWorkflowsPerUser: Int,
                   metricsPrefix: String,
                   requesterPaysRole: String,
                   useWorkflowCollectionField: Boolean,
                   useWorkflowCollectionLabel: Boolean,
                   defaultNetworkCromwellBackend: CromwellBackend,
                   highSecurityNetworkCromwellBackend: CromwellBackend,
                   methodConfigResolver: MethodConfigResolver,
                   bardService: BardService
  ): Unit = {

    if (appConfigManager.cloudProvider == WorkspaceCloudPlatform.Gcp) {
      // these monitors are only appropriate for GCP control plane

      // Reset "Launching" workflows to "Queued"
      resetLaunchingWorkflows(slickDataSource)

      // Boot data source access
      val dataSourceAccess = startDataSourceAccess(system, appConfigManager.conf, slickDataSource)

      // Boot submission monitor supervisor
      val submissionmonitorConfigRoot = appConfigManager.conf.getConfig("submissionmonitor")
      val submissionMonitorConfig = SubmissionMonitorConfig(
        util.toScalaDuration(submissionmonitorConfigRoot.getDuration("submissionPollInterval")),
        util.toScalaDuration(submissionmonitorConfigRoot.getDuration("submissionPollExpiration")),
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
        entityQueryTimeout,
        metricsPrefix
      )

      // Boot workflow submission actors
      startWorkflowSubmissionActors(
        system,
        appConfigManager.conf,
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
        methodConfigResolver,
        bardService
      )

      // Boot bucket deletion monitor
      startBucketDeletionMonitor(system, slickDataSource, gcsDAO)

      val workspaceBillingAccountMonitorConfigRoot =
        appConfigManager.conf.getConfig("workspace-billing-account-monitor")
      val workspaceBillingAccountMonitorConfig = BillingAccountSynchronizerConfig(
        util.toScalaDuration(workspaceBillingAccountMonitorConfigRoot.getDuration("pollInterval")),
        util.toScalaDuration(workspaceBillingAccountMonitorConfigRoot.getDuration("initialDelay"))
      )
      // Boot workspace billing account monitor
      startBillingAccountChangeSynchronizer(system,
                                            workspaceBillingAccountMonitorConfig,
                                            slickDataSource,
                                            gcsDAO,
                                            samDAO
      )

      // Boot entity statistics cache monitor
      if (appConfigManager.conf.getBoolean("entityStatisticsCache.enabled")) {
        startEntityStatisticsCacheMonitor(
          system,
          slickDataSource,
          util.toScalaDuration(appConfigManager.conf.getDuration("entityStatisticsCache.timeoutPerWorkspace")),
          util.toScalaDuration(appConfigManager.conf.getDuration("entityStatisticsCache.standardPollInterval")),
          util.toScalaDuration(appConfigManager.conf.getDuration("entityStatisticsCache.workspaceCooldown")),
          metricsPrefix
        )
      }

      val avroUpsertMonitorConfig = AvroUpsertMonitorConfig(
        util.toScalaDuration(appConfigManager.conf.getDuration("avroUpsertMonitor.pollInterval")),
        util.toScalaDuration(appConfigManager.conf.getDuration("avroUpsertMonitor.pollJitter")),
        appConfigManager.conf.getString("avroUpsertMonitor.importRequestPubSubTopic"),
        appConfigManager.conf.getString("avroUpsertMonitor.importRequestPubSubSubscription"),
        appConfigManager.conf.getString("avroUpsertMonitor.updateCwdsPubSubTopic"),
        appConfigManager.conf.getInt("avroUpsertMonitor.ackDeadlineSeconds"),
        appConfigManager.conf.getInt("avroUpsertMonitor.batchSize"),
        appConfigManager.conf.getInt("avroUpsertMonitor.workerCount")
      )

      // Boot the avro upsert monitor to read and process messages in the specified PubSub topic
      startAvroUpsertMonitor(system,
                             entityService,
                             gcsDAO,
                             samDAO,
                             googleStorage,
                             pubSubDAO,
                             cwdsDAO,
                             avroUpsertMonitorConfig,
                             slickDataSource
      )

      startMultiregonalBucketMigrationActor(system,
                                            appConfigManager.conf,
                                            gcsDAO,
                                            googleIamDAO,
                                            slickDataSource,
                                            workspaceService,
                                            googleStorage,
                                            googleStorageTransferService,
                                            samDAO
      )

      startFastPassMonitor(system, appConfigManager.conf, slickDataSource, googleIamDAO, googleStorageDAO)
    }

    val cloneWorkspaceFileTransferMonitorConfigRoot =
      appConfigManager.conf.getConfig("clone-workspace-file-transfer-monitor")
    val cloneWorkspaceFileTransferMonitorConfig = CloneWorkspaceFileTransferMonitorConfig(
      util.toScalaDuration(cloneWorkspaceFileTransferMonitorConfigRoot.getDuration("pollInterval")),
      util.toScalaDuration(cloneWorkspaceFileTransferMonitorConfigRoot.getDuration("initialDelay"))
    )
    startCloneWorkspaceFileTransferMonitor(system, cloneWorkspaceFileTransferMonitorConfig, slickDataSource, gcsDAO)

    startWorkspaceResourceMonitor(
      system,
      appConfigManager.conf,
      slickDataSource,
      samDAO,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      gcsDAO,
      leonardoDAO,
      workspaceRepository
    )

  }

  private def startFastPassMonitor(system: ActorSystem,
                                   conf: Config,
                                   slickDataSource: SlickDataSource,
                                   googleIamDAO: GoogleIamDAO,
                                   googleStorageDAO: GoogleStorageDAO
  ): Unit = {
    val fastPassConfig = FastPassConfig.apply(conf)

    val fastPassMonitor = system.actorOf(
      FastPassMonitor
        .props(
          slickDataSource,
          googleIamDAO,
          googleStorageDAO
        )
        .withDispatcher("fast-pass-monitor-dispatcher"),
      "fast-pass-monitor"
    )

    if (fastPassConfig.enabled) {
      system.scheduler.scheduleAtFixedRate(
        10 seconds,
        fastPassConfig.monitorCleanupPeriod.toScala,
        fastPassMonitor,
        FastPassMonitor.DeleteExpiredGrants
      )
    }
  }

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
                                               entityQueryTimeout: Duration,
                                               metricsPrefix: String
  ) =
    system.actorOf(
      SubmissionSupervisor.props(
        shardedExecutionServiceCluster,
        storeAccess,
        samDAO,
        gcsDAO,
        notificationDAO,
        submissionMonitorConfig,
        entityQueryTimeout,
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
                                            methodConfigResolver: MethodConfigResolver,
                                            bardService: BardService
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
          methodConfigResolver,
          bardService
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
                                     cwdsDAO: CwdsDAO,
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
        cwdsDAO,
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
    gcsDAO: GoogleServicesDAO,
    leonardoDAO: LeonardoDAO,
    workspaceRepository: WorkspaceRepository
  ) = {
    val billingRepo = new BillingRepository(dataSource)

    val leoService = new LeonardoService(leonardoDAO)(system)
    val wsmDeletionAction = new WsmDeletionAction(workspaceManagerDAO)(system)
    val monitorRecordDao = WorkspaceManagerResourceMonitorRecordDao(dataSource)
    val workspaceDeletionRunner = new WorkspaceDeletionRunner(samDAO,
                                                              workspaceManagerDAO,
                                                              workspaceRepository,
                                                              leoService,
                                                              wsmDeletionAction,
                                                              gcsDAO,
                                                              monitorRecordDao
    )
    val workspaceCloneRunner = new WorkspaceCloningRunner(
      samDAO,
      gcsDAO,
      leonardoDAO,
      workspaceManagerDAO,
      monitorRecordDao,
      workspaceRepository
    )
    system.actorOf(
      WorkspaceResourceMonitor.props(
        config,
        dataSource,
        Map(
          JobType.WorkspaceDeleteInit -> workspaceDeletionRunner,
          JobType.LeoAppDeletionPoll -> workspaceDeletionRunner,
          JobType.LeoRuntimeDeletionPoll -> workspaceDeletionRunner,
          JobType.WSMWorkspaceDeletionPoll -> workspaceDeletionRunner,
          JobType.AzureLandingZoneResult ->
            new LandingZoneCreationStatusRunner(samDAO, workspaceManagerDAO, billingRepo, gcsDAO),
          JobType.CloneWorkspaceContainerResult ->
            new CloneWorkspaceContainerRunner(samDAO, workspaceManagerDAO, dataSource, gcsDAO),
          JobType.BpmBillingProjectDelete -> new BPMBillingProjectDeleteRunner(
            samDAO,
            gcsDAO,
            workspaceManagerDAO,
            billingRepo,
            new BillingProjectDeletion(samDAO, billingRepo, billingProfileManagerDAO)
          )
        ) ++ JobType.cloneJobTypes.map(jobType => jobType -> workspaceCloneRunner).toMap
      )
    )
  }

  private def startMultiregonalBucketMigrationActor(system: ActorSystem,
                                                    config: Config,
                                                    gcsDAO: GoogleServicesDAO,
                                                    googleIamDAO: GoogleIamDAO,
                                                    dataSource: SlickDataSource,
                                                    workspaceService: RawlsRequestContext => WorkspaceService,
                                                    storageService: GoogleStorageService[IO],
                                                    storageTransferService: GoogleStorageTransferService[IO],
                                                    samDAO: SamDAO
  ) =
    config.as[Option[MultiregionalBucketMigrationActor.Config]]("multiregional-bucket-migration").foreach {
      actorConfig =>
        system.spawn(
          MultiregionalBucketMigrationActor(
            actorConfig,
            dataSource,
            workspaceService,
            storageService,
            storageTransferService,
            gcsDAO,
            googleIamDAO,
            samDAO
          ).behavior,
          "MultiregionalBucketMigrationActor"
        )

    }

  private def resetLaunchingWorkflows(dataSource: SlickDataSource) =
    Await.result(dataSource.inTransaction { dataAccess =>
                   dataAccess.workflowQuery.batchUpdateStatus(WorkflowStatuses.Launching, WorkflowStatuses.Queued)
                 },
                 10 seconds
    )
}
