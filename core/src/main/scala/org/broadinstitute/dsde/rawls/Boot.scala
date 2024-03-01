package org.broadinstitute.dsde.rawls

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.effect._
import cats.implicits._
import com.codahale.metrics.SharedMetricRegistries
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.gson.GsonFactory
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.opentelemetry.trace.{TraceConfiguration, TraceExporter}
import com.readytalk.metrics.{StatsDReporter, WorkbenchStatsD}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.{ContextPropagators, TextMapPropagator}
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.instrumentation.resources.{ContainerResource, HostResource}
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.BatchSpanProcessor
import io.opentelemetry.sdk.{resources, OpenTelemetrySdk}
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.semconv.ResourceAttributes
import io.sentry.{Hint, Sentry, SentryEvent, SentryOptions}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.rawls.billing._
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.HttpDataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.{HttpResourceBufferDAO, ResourceBufferDAO}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.{
  HttpWorkspaceManagerClientProvider,
  HttpWorkspaceManagerDAO
}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, StructuredLogger}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.drs.{DrsHubResolver, MarthaResolver}
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.{HttpGoogleAccessContextManagerDAO, HttpGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.{CachingWDLParser, NonCachingWDLParser, WDLParser}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.{
  MultiCloudWorkspaceAclManager,
  MultiCloudWorkspaceService,
  RawlsWorkspaceAclManager,
  WorkspaceRepository,
  WorkspaceService
}
import org.broadinstitute.dsde.workbench.dataaccess.PubSubNotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{
  GoogleCredentialModes,
  HttpGoogleBigQueryDAO,
  HttpGoogleIamDAO,
  HttpGoogleStorageDAO
}
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.oauth2.{ClientId, ClientSecret, OpenIDConnectConfiguration}
import org.http4s.Uri
import org.http4s.blaze.client.BlazeClientBuilder

import java.io.{ByteArrayInputStream, FileInputStream, StringReader}
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.jdk.DurationConverters.JavaDurationOps
import scala.jdk.CollectionConverters._
import scala.language.{higherKinds, postfixOps}

object Boot extends IOApp with LazyLogging {
  override def run(
    args: List[String]
  ): IO[ExitCode] =
    startup() *> ExitCode.Success.pure[IO]

  private def startup(): IO[Unit] = {
    implicit val log4CatsLogger = Slf4jLogger.getLogger[IO]

    Sentry.init((options: SentryOptions) => setupSentry(options))

    // version.conf is generated by sbt
    val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
    val gcsConfig = conf.getConfig("gcs")

    // we need an ActorSystem to host our application in
    implicit val system: ActorSystem = ActorSystem("rawls")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    instantiateOpenTelemetry(conf)

    val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcProfile]("slick", conf))

    val liquibaseConf = conf.getConfig("liquibase")
    val liquibaseChangeLog = liquibaseConf.getString("changelog")
    val initWithLiquibase = liquibaseConf.getBoolean("initWithLiquibase")

    val changelogParams = Map("gcs:appsDomain" -> gcsConfig.getString("appsDomain"))

    if (initWithLiquibase) {
      slickDataSource.initWithLiquibase(liquibaseChangeLog, changelogParams)
    }

    val metricsConf = conf.getConfig("metrics")
    val metricsPrefix = {
      val basePrefix = metricsConf.getString("prefix")
      metricsConf.getBooleanOption("includeHostname") match {
        case Some(true) =>
          val hostname = InetAddress.getLocalHost().getHostName()
          basePrefix + "." + hostname
        case _ => basePrefix
      }
    }

    if (metricsConf.getBooleanOption("enabled").getOrElse(false)) {
      metricsConf.getObjectOption("reporters") match {
        case Some(configObject) =>
          configObject.entrySet.asScala.map(_.toTuple).foreach {
            case ("statsd", conf: ConfigObject) =>
              val statsDConf = conf.toConfig
              startStatsDReporter(statsDConf.getString("host"),
                                  statsDConf.getInt("port"),
                                  statsDConf.getDuration("period"),
                                  apiKey = statsDConf.getStringOption("apiKey")
              )
            case ("statsd-sidecar", conf: ConfigObject) =>
              // Capability for apiKey-less additional statsd target, intended for statsd-exporter sidecar
              val statsDConf = conf.toConfig
              startStatsDReporter(statsDConf.getString("host"),
                                  statsDConf.getInt("port"),
                                  statsDConf.getDuration("period")
              )
            case (other, _) =>
              logger.warn(s"Unknown metrics backend: $other")
          }
        case None => logger.info("No metrics reporters defined")
      }
    } else {
      logger.info("Metrics reporting is disabled.")
    }

    val jsonFactory = GsonFactory.getDefaultInstance
    val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("secrets")))
    val clientEmail = gcsConfig.getString("serviceClientEmail")

    val serviceProject = gcsConfig.getString("serviceProject")
    val appName = gcsConfig.getString("appName")
    val pathToPem = gcsConfig.getString("pathToPem")

    val accessContextManagerDAO = new HttpGoogleAccessContextManagerDAO(
      clientEmail,
      pathToPem,
      appName,
      serviceProject,
      workbenchMetricBaseName = metricsPrefix
    )

    initAppDependencies[IO](conf, appName, metricsPrefix).use { appDependencies =>
      val gcsDAO = new HttpGoogleServicesDAO(
        clientSecrets,
        clientEmail,
        gcsConfig.getString("subEmail"),
        gcsConfig.getString("pathToPem"),
        gcsConfig.getString("appsDomain"),
        gcsConfig.getString("groupsPrefix"),
        gcsConfig.getString("appName"),
        serviceProject,
        gcsConfig.getString("billingPemEmail"),
        gcsConfig.getString("pathToBillingPem"),
        gcsConfig.getString("billingEmail"),
        gcsConfig.getString("billingGroupEmail"),
        googleStorageService = appDependencies.googleStorageService,
        workbenchMetricBaseName = metricsPrefix,
        proxyNamePrefix = gcsConfig.getStringOr("proxyNamePrefix", ""),
        terraBucketReaderRole = gcsConfig.getString("terraBucketReaderRole"),
        terraBucketWriterRole = gcsConfig.getString("terraBucketWriterRole"),
        accessContextManagerDAO = accessContextManagerDAO,
        resourceBufferJsonFile = gcsConfig.getString("pathToResourceBufferJson")
      )

      val pubSubDAO = new HttpGooglePubSubDAO(
        clientEmail,
        pathToPem,
        appName,
        serviceProject,
        workbenchMetricBaseName = metricsPrefix
      )

      // Import service uses a different project for its pubsub topic
      val importServicePubSubDAO = new HttpGooglePubSubDAO(
        clientEmail,
        pathToPem,
        appName,
        conf.getString("avroUpsertMonitor.updateImportStatusPubSubProject"),
        workbenchMetricBaseName = metricsPrefix
      )

      val importServiceDAO = new HttpImportServiceDAO(conf.getString("avroUpsertMonitor.server"))

      val pathToBqJson = gcsConfig.getString("pathToBigQueryJson")
      val bqJsonFileSource = scala.io.Source.fromFile(pathToBqJson)
      val bqJsonCreds =
        try bqJsonFileSource.mkString
        finally bqJsonFileSource.close()

      val bigQueryDAO = new HttpGoogleBigQueryDAO(
        appName,
        Json(bqJsonCreds),
        metricsPrefix
      )

      val samConfig = conf.getConfig("sam")
      val samDAO = new HttpSamDAO(
        samConfig.getString("server"),
        gcsDAO.getBucketServiceAccountCredential,
        toScalaDuration(samConfig.getDuration("timeout"))
      )

      enableServiceAccount(gcsDAO, samDAO)

      system.registerOnTermination {
        slickDataSource.databaseConfig.db.shutdown
      }

      val executionServiceConfig = conf.getConfig("executionservice")
      val submissionTimeout = org.broadinstitute.dsde.rawls.util.toScalaDuration(
        executionServiceConfig.getDuration("workflowSubmissionTimeout")
      )

      val executionServiceServers: Set[ClusterMember] = executionServiceConfig
        .getObject("readServers")
        .entrySet()
        .asScala
        .map { entry =>
          val (strName, strHostname) = entry.toTuple
          ClusterMember(
            ExecutionServiceId(strName),
            new HttpExecutionServiceDAO(
              strHostname.unwrapped.toString,
              metricsPrefix
            )
          )
        }
        .toSet

      val executionServiceSubmitServers: Set[ClusterMember] =
        executionServiceConfig
          .getObject("submitServers")
          .entrySet()
          .asScala
          .map { entry =>
            val (strName, strHostname) = entry.toTuple
            ClusterMember(
              ExecutionServiceId(strName),
              new HttpExecutionServiceDAO(
                strHostname.unwrapped.toString,
                metricsPrefix
              )
            )
          }
          .toSet

      val cromiamDAO: ExecutionServiceDAO =
        new HttpExecutionServiceDAO(executionServiceConfig.getString("cromiamUrl"), metricsPrefix)
      val shardedExecutionServiceCluster: ExecutionServiceCluster =
        new ShardedHttpExecutionServiceCluster(
          executionServiceServers,
          executionServiceSubmitServers,
          slickDataSource
        )
      val requesterPaysRole = gcsConfig.getString("requesterPaysRole")

      val notificationPubSubDAO = new org.broadinstitute.dsde.workbench.google.HttpGooglePubSubDAO(
        clientEmail,
        pathToPem,
        appName,
        serviceProject,
        workbenchMetricBaseName = metricsPrefix
      )

      val notificationDAO = new PubSubNotificationDAO(
        notificationPubSubDAO,
        gcsConfig.getString("notifications.topicName")
      )

      val drsResolver = if (conf.hasPath("drs")) {
        val drsResolverName = conf.getString("drs.resolver")
        drsResolverName match {
          case "martha" =>
            val marthaBaseUrl: String = conf.getString("drs.martha.baseUrl")
            val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
            new MarthaResolver(marthaUrl)
          case "drshub" =>
            val drsHubBaseUrl: String = conf.getString("drs.drshub.baseUrl")
            val drsHubUrl: String = s"$drsHubBaseUrl/api/v4/drs/resolve"
            new DrsHubResolver(drsHubUrl)
        }
      } else {
        val marthaBaseUrl: String = conf.getString("martha.baseUrl")
        val marthaUrl: String = s"$marthaBaseUrl/martha_v3"
        new MarthaResolver(marthaUrl)
      }

      val servicePerimeterConfig = ServicePerimeterServiceConfig(conf)
      val servicePerimeterService = new ServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterConfig)

      val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig.apply(conf)
      val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
        new HttpBillingProfileManagerClientProvider(conf.getStringOption("billingProfileManager.baseUrl")),
        multiCloudWorkspaceConfig
      )

      val genomicsServiceConstructor: RawlsRequestContext => GenomicsService =
        GenomicsService.constructor(slickDataSource, gcsDAO)
      val submissionCostService: SubmissionCostService =
        SubmissionCostService.constructor(
          gcsConfig.getString("billingExportTableName"),
          gcsConfig.getString("billingExportDatePartitionColumn"),
          gcsConfig.getString("serviceProject"),
          gcsConfig.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )

      val methodRepoDAO = new HttpMethodRepoDAO(
        MethodRepoConfig.apply[Agora.type](conf.getConfig("agora")),
        MethodRepoConfig.apply[Dockstore.type](conf.getConfig("dockstore")),
        metricsPrefix
      )

      val workspaceManagerDAO =
        new HttpWorkspaceManagerDAO(new HttpWorkspaceManagerClientProvider(conf.getString("workspaceManager.baseUrl")))

      val dataRepoDAO =
        new HttpDataRepoDAO(conf.getString("dataRepo.terraInstanceName"), conf.getString("dataRepo.terraInstance"))

      val userServiceConstructor: RawlsRequestContext => UserService =
        UserService.constructor(
          slickDataSource,
          gcsDAO,
          samDAO,
          appDependencies.bigQueryServiceFactory,
          bqJsonCreds,
          requesterPaysRole,
          servicePerimeterService,
          RawlsBillingAccountName(gcsConfig.getString("adminRegisterBillingAccountId")),
          billingProfileManagerDAO,
          workspaceManagerDAO,
          notificationDAO
        )

      val maxActiveWorkflowsTotal =
        conf.getInt("executionservice.maxActiveWorkflowsPerServer")
      val maxActiveWorkflowsPerUser = maxActiveWorkflowsTotal / conf.getInt(
        "executionservice.activeWorkflowHogFactor"
      )
      val useWorkflowCollectionField =
        conf.getBoolean("executionservice.useWorkflowCollectionField")
      val useWorkflowCollectionLabel =
        conf.getBoolean("executionservice.useWorkflowCollectionLabel")
      val defaultNetworkCromwellBackend: CromwellBackend =
        CromwellBackend(conf.getString("executionservice.defaultNetworkBackend"))
      val highSecurityNetworkCromwellBackend: CromwellBackend =
        CromwellBackend(conf.getString("executionservice.highSecurityNetworkBackend"))

      val wdlParsingConfig = WDLParserConfig(conf.getConfig("wdl-parsing"))
      def cromwellSwaggerClient = new CromwellSwaggerClient(wdlParsingConfig.serverBasePath)

      def wdlParser: WDLParser =
        if (wdlParsingConfig.useCache)
          new CachingWDLParser(wdlParsingConfig, cromwellSwaggerClient)
        else new NonCachingWDLParser(wdlParsingConfig, cromwellSwaggerClient)

      val methodConfigResolver = new MethodConfigResolver(wdlParser)

      val healthMonitor = system.actorOf(
        HealthMonitor
          .props(
            slickDataSource,
            gcsDAO,
            pubSubDAO,
            methodRepoDAO,
            samDAO,
            billingProfileManagerDAO,
            workspaceManagerDAO,
            executionServiceServers.map(c => c.key -> c.dao).toMap,
            groupsToCheck = Seq(gcsDAO.adminGroupName, gcsDAO.curatorGroupName),
            topicsToCheck = Seq(gcsConfig.getString("notifications.topicName")),
            bucketsToCheck = Seq.empty
          )
          .withDispatcher("health-monitor-dispatcher"),
        "health-monitor"
      )
      logger.info("Starting health monitor...")
      system.scheduler.schedule(
        10 seconds,
        1 minute,
        healthMonitor,
        HealthMonitor.CheckAll
      )

      val statusServiceConstructor: () => StatusService = () => StatusService.constructor(healthMonitor)

      val workspaceServiceConfig = WorkspaceServiceConfig.apply(conf)

      val bondConfig = conf.getConfig("bond")
      val bondApiDAO: BondApiDAO = new HttpBondApiDAO(bondConfig.getString("baseUrl"))
      val requesterPaysSetupService: RequesterPaysSetupService =
        new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole)

      val entityQueryTimeout = conf.getDuration("entities.queryTimeout")

      // create the entity manager.
      val entityManager = EntityManager.defaultEntityManager(
        slickDataSource,
        workspaceManagerDAO,
        dataRepoDAO,
        samDAO,
        appDependencies.bigQueryServiceFactory,
        DataRepoEntityProviderConfig(conf.getConfig("dataRepoEntityProvider")),
        conf.getBoolean("entityStatisticsCache.enabled"),
        entityQueryTimeout,
        metricsPrefix
      )

      val resourceBufferConfig = ResourceBufferConfig(conf.getConfig("resourceBuffer"))
      val resourceBufferDAO: ResourceBufferDAO =
        new HttpResourceBufferDAO(resourceBufferConfig, gcsDAO.getResourceBufferServiceAccountCredential)
      val resourceBufferService = new ResourceBufferService(resourceBufferDAO, resourceBufferConfig)
      val resourceBufferSaEmail = resourceBufferConfig.saEmail

      val leonardoConfig = LeonardoConfig(conf.getConfig("leonardo"))
      val leonardoDAO: LeonardoDAO =
        new HttpLeonardoDAO(leonardoConfig);

      val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService =
        MultiCloudWorkspaceService.constructor(
          slickDataSource,
          workspaceManagerDAO,
          billingProfileManagerDAO,
          samDAO,
          multiCloudWorkspaceConfig,
          leonardoDAO,
          metricsPrefix
        )

      val fastPassConfig = FastPassConfig.apply(conf)
      val fastPassServiceConstructor: (RawlsRequestContext, SlickDataSource) => FastPassService =
        FastPassService.constructor(
          fastPassConfig,
          appDependencies.httpGoogleIamDAO,
          appDependencies.httpGoogleStorageDAO,
          gcsDAO,
          samDAO,
          terraBillingProjectOwnerRole = gcsConfig.getString("terraBillingProjectOwnerRole"),
          terraWorkspaceCanComputeRole = gcsConfig.getString("terraWorkspaceCanComputeRole"),
          terraWorkspaceNextflowRole = gcsConfig.getString("terraWorkspaceNextflowRole"),
          terraBucketReaderRole = gcsConfig.getString("terraBucketReaderRole"),
          terraBucketWriterRole = gcsConfig.getString("terraBucketWriterRole")
        )

      val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService = WorkspaceService.constructor(
        slickDataSource,
        methodRepoDAO,
        cromiamDAO,
        shardedExecutionServiceCluster,
        conf.getInt("executionservice.batchSize"),
        workspaceManagerDAO,
        methodConfigResolver,
        gcsDAO,
        samDAO,
        notificationDAO,
        userServiceConstructor,
        genomicsServiceConstructor,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        workbenchMetricBaseName = metricsPrefix,
        submissionCostService,
        workspaceServiceConfig,
        requesterPaysSetupService,
        entityManager,
        resourceBufferService,
        resourceBufferSaEmail,
        servicePerimeterService,
        googleIamDao = appDependencies.httpGoogleIamDAO,
        terraBillingProjectOwnerRole = gcsConfig.getString("terraBillingProjectOwnerRole"),
        terraWorkspaceCanComputeRole = gcsConfig.getString("terraWorkspaceCanComputeRole"),
        terraWorkspaceNextflowRole = gcsConfig.getString("terraWorkspaceNextflowRole"),
        terraBucketReaderRole = gcsConfig.getString("terraBucketReaderRole"),
        terraBucketWriterRole = gcsConfig.getString("terraBucketWriterRole"),
        new RawlsWorkspaceAclManager(samDAO),
        new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, slickDataSource),
        fastPassServiceConstructor
      )

      val entityServiceConstructor: RawlsRequestContext => EntityService = EntityService.constructor(
        slickDataSource,
        samDAO,
        workbenchMetricBaseName = metricsPrefix,
        entityManager,
        conf.getInt("entities.pageSizeLimit")
      )

      val snapshotServiceConstructor: RawlsRequestContext => SnapshotService = SnapshotService.constructor(
        slickDataSource,
        samDAO,
        workspaceManagerDAO,
        conf.getString("dataRepo.terraInstanceName"),
        dataRepoDAO
      )

      val spendReportingBigQueryService =
        appDependencies.bigQueryServiceFactory.getServiceFromJson(bqJsonCreds,
                                                                  GoogleProject(gcsConfig.getString("serviceProject"))
        )
      val spendReportingServiceConfig = SpendReportingServiceConfig(
        gcsConfig.getString("billingExportTableName"),
        gcsConfig.getString("billingExportTimePartitionColumn"),
        gcsConfig.getConfig("spendReporting").getInt("maxDateRange"),
        metricsPrefix
      )

      val workspaceManagerResourceMonitorRecordDao = new WorkspaceManagerResourceMonitorRecordDao(slickDataSource)
      val billingRepository = new BillingRepository(slickDataSource)
      val workspaceRepository = new WorkspaceRepository(slickDataSource)
      val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator =
        BillingProjectOrchestrator.constructor(
          samDAO,
          notificationDAO,
          billingRepository,
          new GoogleBillingProjectLifecycle(billingRepository, samDAO, gcsDAO),
          new BpmBillingProjectLifecycle(samDAO,
                                         billingRepository,
                                         billingProfileManagerDAO,
                                         workspaceManagerDAO,
                                         workspaceManagerResourceMonitorRecordDao
          ),
          workspaceManagerResourceMonitorRecordDao,
          multiCloudWorkspaceConfig
        )

      val spendReportingServiceConstructor: RawlsRequestContext => SpendReportingService =
        SpendReportingService.constructor(
          slickDataSource,
          spendReportingBigQueryService,
          billingRepository,
          billingProfileManagerDAO,
          samDAO,
          spendReportingServiceConfig
        )

      val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService =
        BucketMigrationService.constructor(slickDataSource, samDAO, gcsDAO)

      val service = new RawlsApiServiceImpl(
        multiCloudWorkspaceServiceConstructor,
        workspaceServiceConstructor,
        entityServiceConstructor,
        userServiceConstructor,
        genomicsServiceConstructor,
        snapshotServiceConstructor,
        spendReportingServiceConstructor,
        billingProjectOrchestratorConstructor,
        bucketMigrationServiceConstructor,
        statusServiceConstructor,
        shardedExecutionServiceCluster,
        ApplicationVersion(
          conf.getString("version.git.hash"),
          conf.getString("version.build.number"),
          conf.getString("version.version")
        ),
        submissionTimeout,
        conf.getLong("entityUpsert.maxContentSizeBytes"),
        metricsPrefix,
        samDAO,
        appDependencies.oidcConfiguration
      )

      if (conf.getBooleanOption("backRawls").getOrElse(false)) {
        logger.info("This instance has been marked as BACK. Booting monitors...")

        BootMonitors.bootMonitors(
          system,
          conf,
          slickDataSource,
          gcsDAO,
          appDependencies.httpGoogleIamDAO,
          appDependencies.httpGoogleStorageDAO,
          samDAO,
          notificationDAO,
          pubSubDAO,
          importServicePubSubDAO,
          importServiceDAO,
          workspaceManagerDAO,
          billingProfileManagerDAO,
          leonardoDAO,
          workspaceRepository,
          appDependencies.googleStorageService,
          appDependencies.googleStorageTransferService,
          methodRepoDAO,
          drsResolver,
          entityServiceConstructor,
          entityQueryTimeout.toScala,
          workspaceServiceConstructor,
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
      } else
        logger.info(
          "This instance has been marked as FRONT. Monitors will not be booted..."
        )

      for {
        binding <- IO.fromFuture(IO(Http().bindAndHandle(service.route, "0.0.0.0", 8080))).recover {
          case t: Throwable =>
            logger.error("FATAL - failure starting http server", t)
            throw t
        }
        _ <- IO.fromFuture(IO(binding.whenTerminated))
        _ <- IO(system.terminate())
      } yield ()
    }
  }

  private def setupSentry(options: SentryOptions): Unit = {
    def setupOptions(options: SentryOptions): Unit = {
      options.setEnableExternalConfiguration(true)
      options.setBeforeSend((event: SentryEvent, hint: Hint) => SentryEventFilter.filterEvent(event))
    }

    setupOptions(options)
  }

  /**
   * Enables the rawls service account in ldap. Allows service to service auth through the proxy.
   * @param gcsDAO
   */
  def enableServiceAccount(gcsDAO: HttpGoogleServicesDAO, samDAO: HttpSamDAO): Unit = {
    val credential = gcsDAO.getBucketServiceAccountCredential
    val serviceAccountUserInfo = UserInfo.buildFromTokens(credential)

    val registerServiceAccountFuture = samDAO.registerUser(RawlsRequestContext(serviceAccountUserInfo))

    registerServiceAccountFuture.failed.foreach {
      // this is logged as a warning because almost always the service account is already enabled
      // so this is a problem only the first time rawls is started with a new service account
      t: Throwable => logger.warn("error enabling service account", t)
    }
  }

  def startStatsDReporter(host: String,
                          port: Int,
                          period: java.time.Duration,
                          registryName: String = "default",
                          apiKey: Option[String] = None
  ): Unit = {
    logger.info(s"Starting statsd reporter writing to [$host:$port] with period [${period.toMillis} ms]")
    val reporter = StatsDReporter
      .forRegistry(SharedMetricRegistries.getOrCreate(registryName))
      .prefixedWith(apiKey.orNull)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(WorkbenchStatsD(host, port))
    reporter.start(period.toMillis, period.toMillis, TimeUnit.MILLISECONDS)
  }

  def initAppDependencies[F[_]: Logger: Async](config: Config, appName: String, metricsPrefix: String)(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
  ): cats.effect.Resource[F, AppDependencies[F]] = {
    val gcsConfig = config.getConfig("gcs")
    val serviceProject = GoogleProject(gcsConfig.getString("serviceProject"))
    val oidcConfig = config.getConfig("oidc")

    // todo: load these credentials once [CA-1806]
    val pathToCredentialJson = gcsConfig.getString("pathToCredentialJson")
    val jsonFileSource = scala.io.Source.fromFile(pathToCredentialJson)
    val jsonCreds =
      try jsonFileSource.mkString
      finally jsonFileSource.close()
    val saCredentials = ServiceAccountCredentials.fromStream(
      new ByteArrayInputStream(jsonCreds.getBytes(StandardCharsets.UTF_8))
    )

    val googleApiUri = Uri.unsafeFromString(gcsConfig.getString("google-api-uri"))
    val metadataNotificationConfig = NotificationCreaterConfig(pathToCredentialJson, googleApiUri)

    implicit val logger: StructuredLogger[F] = Slf4jLogger.getLogger[F]
    // This is for sending custom metrics to stackdriver. all custom metrics starts with `OpenCensus/rawls/`.
    // Typing in `rawls` in metrics explorer will show all rawls custom metrics.
    // As best practice, we should have all related metrics under same prefix separated by `/`
    val prometheusConfig = PrometheusConfig.apply(config)

    for {
      googleStorage <- GoogleStorageService.resource[F](pathToCredentialJson, None, Option(serviceProject))
      googleStorageTransferService <- GoogleStorageTransferService.resource(saCredentials)
      httpClient <- BlazeClientBuilder(executionContext).resource
      googleServiceHttp <- GoogleServiceHttp.withRetryAndLogging(httpClient, metadataNotificationConfig)
      topicAdmin <- GoogleTopicAdmin.fromCredentialPath(pathToCredentialJson)
      bqServiceFactory = new GoogleBigQueryServiceFactory(pathToCredentialJson)(executionContext)
      httpGoogleIamDAO = new HttpGoogleIamDAO(appName, GoogleCredentialModes.Json(jsonCreds), metricsPrefix)(
        system,
        executionContext
      )
      httpGoogleStorageDAO = new HttpGoogleStorageDAO(appName, GoogleCredentialModes.Json(jsonCreds), metricsPrefix)(
        system,
        executionContext
      )

      openIdConnect <- cats.effect.Resource.eval(
        OpenIDConnectConfiguration[F](
          oidcConfig.getString("authorityEndpoint"),
          ClientId(oidcConfig.getString("oidcClientId")),
          oidcClientSecret = oidcConfig.getAs[String]("oidcClientSecret").map(ClientSecret),
          extraGoogleClientId = oidcConfig.getAs[String]("legacyGoogleClientId").map(ClientId),
          extraAuthParams = Some("prompt=login")
        )
      )
    } yield AppDependencies[F](
      googleStorage,
      googleStorageTransferService,
      googleServiceHttp,
      topicAdmin,
      bqServiceFactory,
      httpGoogleIamDAO,
      httpGoogleStorageDAO,
      openIdConnect
    )
  }

  private def instantiateOpenTelemetry(conf: Config)(implicit system: ActorSystem): OpenTelemetry = {
    val maybeVersion = Option(getClass.getPackage.getImplementationVersion)
    val resourceBuilder =
      resources.Resource.getDefault.toBuilder
        .put(ResourceAttributes.SERVICE_NAME, "rawls")
    maybeVersion.foreach(version => resourceBuilder.put(ResourceAttributes.SERVICE_VERSION, version))
    val resource = HostResource
      .get()
      .merge(ContainerResource.get())
      .merge(resourceBuilder.build)

    val maybeTracerProvider = conf.getBooleanOption("opencensus-scala.trace.exporters.stackdriver.enabled").flatMap {
      case false => None
      case true =>
        val traceProviderBuilder = SdkTracerProvider.builder
        val projectId = conf.getString("opencensus-scala.trace.exporters.stackdriver.project-id")
        val googleTraceExporter =
          TraceExporter.createWithConfiguration(TraceConfiguration.builder().setProjectId(projectId).build())
        traceProviderBuilder.addSpanProcessor(BatchSpanProcessor.builder(googleTraceExporter).build())
        val probabilitySampler =
          Sampler.traceIdRatioBased(conf.getDouble("opencensus-scala.trace.sampling-probability"))
        traceProviderBuilder
          .setResource(resource)
          .setSampler(Sampler.parentBased(probabilitySampler))
          .build
          .some
    }

    val prometheusConfig = PrometheusConfig.apply(conf)

    val prometheusHttpServer = PrometheusHttpServer.builder().setPort(prometheusConfig.endpointPort).build()
    system.registerOnTermination(prometheusHttpServer.shutdown())
    val sdkMeterProvider =
      SdkMeterProvider.builder
        .registerMetricReader(prometheusHttpServer)
        .setResource(resource)
        .build

    val otelBuilder = OpenTelemetrySdk.builder
    maybeTracerProvider.foreach(otelBuilder.setTracerProvider)
    otelBuilder.setMeterProvider(sdkMeterProvider)
    otelBuilder.setPropagators(
      ContextPropagators.create(
        TextMapPropagator.composite(W3CTraceContextPropagator.getInstance, W3CBaggagePropagator.getInstance)
      )
    )
    otelBuilder.buildAndRegisterGlobal
  }
}

// Any resources need clean up should be put in AppDependencies
final case class AppDependencies[F[_]](googleStorageService: GoogleStorageService[F],
                                       googleStorageTransferService: GoogleStorageTransferService[F],
                                       googleServiceHttp: GoogleServiceHttp[F],
                                       topicAdmin: GoogleTopicAdmin[F],
                                       bigQueryServiceFactory: GoogleBigQueryServiceFactory,
                                       httpGoogleIamDAO: HttpGoogleIamDAO,
                                       httpGoogleStorageDAO: HttpGoogleStorageDAO,
                                       oidcConfiguration: OpenIDConnectConfiguration
)
