package org.broadinstitute.dsde.rawls

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.effect._
import cats.implicits._
import com.codahale.metrics.SharedMetricRegistries
import com.google.cloud.opentelemetry.trace.{TraceConfiguration, TraceExporter}
import com.readytalk.metrics.{StatsDReporter, WorkbenchStatsD}
import com.typesafe.config.{Config, ConfigObject}
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
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.sdk.{resources, OpenTelemetrySdk}
import io.opentelemetry.semconv.ResourceAttributes
import io.sentry.{Hint, Sentry, SentryEvent, SentryOptions}
import org.broadinstitute.dsde.rawls.billing._
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.HttpDataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.{
  HttpWorkspaceManagerClientProvider,
  HttpWorkspaceManagerDAO
}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, StructuredLogger}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.{CachingWDLParser, NonCachingWDLParser, WDLParser}
import org.broadinstitute.dsde.rawls.metrics.BardService
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor._
import org.broadinstitute.dsde.rawls.serviceFactory._
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
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.oauth2.{ClientId, OpenIDConnectConfiguration}

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters.JavaDurationOps
import scala.language.{higherKinds, postfixOps}

object Boot extends IOApp with LazyLogging {
  override def run(
    args: List[String]
  ): IO[ExitCode] =
    startup() *> ExitCode.Success.pure[IO]

  private def startup(): IO[Unit] = {
    implicit val log4CatsLogger = Slf4jLogger.getLogger[IO]

    Sentry.init((options: SentryOptions) => setupSentry(options))

    val appConfigManager = new RawlsConfigManager

    // we need an ActorSystem to host our application in
    implicit val system: ActorSystem = ActorSystem("rawls")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    instantiateOpenTelemetry(appConfigManager.conf)

    val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcProfile]("slick", appConfigManager.conf))

    val liquibaseConf = appConfigManager.conf.getConfig("liquibase")
    val liquibaseChangeLog = liquibaseConf.getString("changelog")
    val initWithLiquibase = liquibaseConf.getBoolean("initWithLiquibase")

    val changelogParams = Map(
      "gcs:appsDomain" -> appConfigManager.gcsConfig.map(_.getString("appsDomain")).getOrElse("")
    )

    if (initWithLiquibase) {
      slickDataSource.initWithLiquibase(liquibaseChangeLog, changelogParams)
    }

    val metricsConf = appConfigManager.conf.getConfig("metrics")
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

    val accessContextManagerDAO =
      AccessContextManagerFactory.createAccessContextManager(metricsPrefix, appConfigManager)

    initAppDependencies[IO](appConfigManager, appConfigManager.conf, metricsPrefix).use { appDependencies =>
      val gcsDAO = GoogleServicesDAOFactory.createGoogleServicesDAO(
        appConfigManager,
        appDependencies,
        metricsPrefix,
        accessContextManagerDAO
      )

      val pubSubDAO = PubSubDAOFactory.createPubSubDAO(
        appConfigManager,
        metricsPrefix
      )

      val cwdsDAO = CwdsDAOFactory.createCwdsDAO(appConfigManager)

      val bqJsonCreds = BigQueryCredentialsManager.getBigQueryCredentials(appConfigManager)

      val bigQueryDAO =
        BigQueryDAOFactory.createBigQueryDAO(appConfigManager, Json(bqJsonCreds), metricsPrefix)

      val samDAO = SamDAOFactory.createSamDAO(appConfigManager)
      samDAO.registerRawlsIdentity().failed.foreach {
        // this is logged as a warning because almost always the service account is already enabled
        // so this is a problem only the first time rawls is started with a new service account
        t: Throwable => logger.warn("error enabling service account", t)
      }

      system.registerOnTermination {
        slickDataSource.databaseConfig.db.shutdown
      }

      val executionServiceConfig = appConfigManager.conf.getConfig("executionservice")
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
      val requesterPaysRole =
        appConfigManager.gcsConfig.map(_.getString("requesterPaysRole")).getOrElse("")

      val notificationPubSubDAO =
        NotificationPubSubDAOFactory.createNotificationPubSubDAO(appConfigManager, metricsPrefix)

      val notificationDAO =
        NotificationDAOFactory.createNotificationDAO(appConfigManager, notificationPubSubDAO)

      val drsResolver = DrsResolverFactory.createDrsResolver(appConfigManager)

      val servicePerimeterService =
        ServicePerimeterServiceFactory.createServicePerimeter(appConfigManager, slickDataSource, gcsDAO)

      val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig.apply(appConfigManager.conf)
      val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
        new HttpBillingProfileManagerClientProvider(
          appConfigManager.conf.getStringOption("billingProfileManager.baseUrl")
        ),
        multiCloudWorkspaceConfig
      )

      val genomicsServiceConstructor: RawlsRequestContext => GenomicsService =
        GenomicsServiceFactory.createGenomicsService(appConfigManager, slickDataSource, gcsDAO)

      val submissionCostService =
        SubmissionCostServiceFactory.createSubmissionCostService(appConfigManager, bigQueryDAO)

      val methodRepoDAO =
        MethodRepoDAOFactory.createMethodRepoDAO(appConfigManager, metricsPrefix)

      val workspaceManagerDAO =
        new HttpWorkspaceManagerDAO(
          new HttpWorkspaceManagerClientProvider(appConfigManager.conf.getString("workspaceManager.baseUrl"))
        )

      val dataRepoDAO =
        new HttpDataRepoDAO(appConfigManager.conf.getString("dataRepo.terraInstanceName"),
                            appConfigManager.conf.getString("dataRepo.terraInstance")
        )

      val userServiceConstructor: RawlsRequestContext => UserService =
        UserService.constructor(
          slickDataSource,
          gcsDAO,
          samDAO,
          appDependencies.bigQueryServiceFactory,
          bqJsonCreds,
          servicePerimeterService,
          billingProfileManagerDAO,
          workspaceManagerDAO,
          notificationDAO
        )

      val maxActiveWorkflowsTotal =
        appConfigManager.conf.getInt("executionservice.maxActiveWorkflowsPerServer")
      val maxActiveWorkflowsPerUser = maxActiveWorkflowsTotal / appConfigManager.conf.getInt(
        "executionservice.activeWorkflowHogFactor"
      )
      val useWorkflowCollectionField =
        appConfigManager.conf.getBoolean("executionservice.useWorkflowCollectionField")
      val useWorkflowCollectionLabel =
        appConfigManager.conf.getBoolean("executionservice.useWorkflowCollectionLabel")
      val defaultNetworkCromwellBackend: CromwellBackend =
        CromwellBackend(appConfigManager.conf.getString("executionservice.defaultNetworkBackend"))
      val highSecurityNetworkCromwellBackend: CromwellBackend =
        CromwellBackend(appConfigManager.conf.getString("executionservice.highSecurityNetworkBackend"))

      val wdlParsingConfig = WDLParserConfig(appConfigManager.conf.getConfig("wdl-parsing"))
      def cromwellSwaggerClient = new CromwellSwaggerClient(wdlParsingConfig.serverBasePath)

      def wdlParser: WDLParser =
        if (wdlParsingConfig.useCache)
          new CachingWDLParser(wdlParsingConfig, cromwellSwaggerClient)
        else new NonCachingWDLParser(wdlParsingConfig, cromwellSwaggerClient)

      val methodConfigResolver = new MethodConfigResolver(wdlParser)

      val healthMonitor = system.actorOf(
        HealthMonitorFactory
          .createHealthMonitorProps(
            appConfigManager,
            slickDataSource,
            gcsDAO,
            pubSubDAO,
            methodRepoDAO,
            samDAO,
            billingProfileManagerDAO,
            workspaceManagerDAO,
            executionServiceServers.map(c => c.key -> c.dao).toMap
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

      val workspaceServiceConfig = WorkspaceServiceConfig.apply(appConfigManager)

      val bondApiDAO: BondApiDAO = BondApiDAOFactory.createBondApiDAO(appConfigManager)

      val requesterPaysSetupService: RequesterPaysSetupService =
        RequesterPaysSetupServiceFactory.createRequesterPaysSetup(appConfigManager,
                                                                  slickDataSource,
                                                                  gcsDAO,
                                                                  bondApiDAO,
                                                                  requesterPaysRole
        )

      val entityQueryTimeout = appConfigManager.conf.getDuration("entities.queryTimeout")

      // create the entity manager.
      val entityManager = EntityManager.defaultEntityManager(
        slickDataSource,
        workspaceManagerDAO,
        dataRepoDAO,
        samDAO,
        appDependencies.bigQueryServiceFactory,
        DataRepoEntityProviderConfig(appConfigManager.conf.getConfig("dataRepoEntityProvider")),
        appConfigManager.conf.getBoolean("entityStatisticsCache.enabled"),
        entityQueryTimeout,
        metricsPrefix
      )

      val resourceBufferDAO: ResourceBufferDAO =
        ResourceBufferDAOFactory.createResourceBuffer(appConfigManager, gcsDAO)

      val resourceBufferService =
        ResourceBufferServiceFactory.createResourceBufferService(appConfigManager, resourceBufferDAO)

      val leonardoConfig = LeonardoConfig(appConfigManager.conf.getConfig("leonardo"))
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

      val fastPassServiceConstructor: (RawlsRequestContext, SlickDataSource) => FastPassService =
        FastPassServiceConstructorFactory.createCloudFastPassService(
          appConfigManager,
          appDependencies,
          gcsDAO,
          samDAO
        )

      val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService = WorkspaceService.constructor(
        slickDataSource,
        methodRepoDAO,
        cromiamDAO,
        shardedExecutionServiceCluster,
        appConfigManager.conf.getInt("executionservice.batchSize"),
        workspaceManagerDAO,
        new LeonardoService(leonardoDAO),
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
        servicePerimeterService,
        googleIamDao = appDependencies.httpGoogleIamDAO,
        terraBillingProjectOwnerRole =
          appConfigManager.gcsConfig.map(_.getString("terraBillingProjectOwnerRole")).getOrElse("unsupported"),
        terraWorkspaceCanComputeRole =
          appConfigManager.gcsConfig.map(_.getString("terraWorkspaceCanComputeRole")).getOrElse("unsupported"),
        terraWorkspaceNextflowRole =
          appConfigManager.gcsConfig.map(_.getString("terraWorkspaceNextflowRole")).getOrElse("unsupported"),
        terraBucketReaderRole =
          appConfigManager.gcsConfig.map(_.getString("terraBucketReaderRole")).getOrElse("unsupported"),
        terraBucketWriterRole =
          appConfigManager.gcsConfig.map(_.getString("terraBucketWriterRole")).getOrElse("unsupported"),
        new RawlsWorkspaceAclManager(samDAO),
        new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, slickDataSource),
        fastPassServiceConstructor
      )

      val entityServiceConstructor: RawlsRequestContext => EntityService = EntityService.constructor(
        slickDataSource,
        samDAO,
        workbenchMetricBaseName = metricsPrefix,
        entityManager,
        appConfigManager.conf.getInt("entities.pageSizeLimit")
      )

      val snapshotServiceConstructor: RawlsRequestContext => SnapshotService = SnapshotService.constructor(
        slickDataSource,
        samDAO,
        workspaceManagerDAO,
        appConfigManager.conf.getString("dataRepo.terraInstanceName"),
        dataRepoDAO
      )

      val spendReportingBigQueryService = appDependencies.bigQueryServiceFactory.getServiceFromJson(
        bqJsonCreds,
        GoogleProject(appConfigManager.gcsConfig.map(_.getString("serviceProject")).getOrElse("unsupported"))
      )

      val spendReportingServiceConfig = SpendReportingServiceConfig(
        appConfigManager.gcsConfig.map(_.getString("billingExportTableName")).getOrElse("unsupported"),
        appConfigManager.gcsConfig.map(_.getString("billingExportTimePartitionColumn")).getOrElse("unsupported"),
        appConfigManager.gcsConfig.map(_.getConfig("spendReporting").getInt("maxDateRange")).getOrElse(90),
        metricsPrefix
      )

      val workspaceManagerResourceMonitorRecordDao = new WorkspaceManagerResourceMonitorRecordDao(slickDataSource)
      val billingRepository = new BillingRepository(slickDataSource)
      val workspaceRepository = new WorkspaceRepository(slickDataSource)
      val billingProjectDeletion = new BillingProjectDeletion(samDAO, billingRepository, billingProfileManagerDAO)
      val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator =
        BillingProjectOrchestrator.constructor(
          samDAO,
          notificationDAO,
          billingRepository,
          new GoogleBillingProjectLifecycle(billingRepository, billingProfileManagerDAO, samDAO, gcsDAO),
          new AzureBillingProjectLifecycle(samDAO,
                                           billingRepository,
                                           billingProfileManagerDAO,
                                           workspaceManagerDAO,
                                           workspaceManagerResourceMonitorRecordDao
          ),
          billingProjectDeletion,
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
        BucketMigrationServiceFactory.createBucketMigrationService(appConfigManager, slickDataSource, samDAO, gcsDAO)

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
          appConfigManager.conf.getString("version.git.hash"),
          appConfigManager.conf.getString("version.build.number"),
          appConfigManager.conf.getString("version.version")
        ),
        submissionTimeout,
        appConfigManager.conf.getLong("entityUpsert.maxContentSizeBytes"),
        metricsPrefix,
        samDAO,
        appDependencies.oidcConfiguration
      )

      if (appConfigManager.conf.getBooleanOption("backRawls").getOrElse(false)) {
        logger.info("This instance has been marked as BACK. Booting monitors...")

        val bardService = new BardService(
          appConfigManager.conf.getBoolean("bard.enabled"),
          appConfigManager.conf.getString("bard.bardUrl"),
          appConfigManager.conf.getInt("bard.connectionPoolSize")
        )

        BootMonitors.bootMonitors(
          system,
          appConfigManager,
          slickDataSource,
          gcsDAO,
          appDependencies.httpGoogleIamDAO,
          appDependencies.httpGoogleStorageDAO,
          samDAO,
          notificationDAO,
          pubSubDAO,
          cwdsDAO,
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
          methodConfigResolver,
          bardService
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

  def initAppDependencies[F[_]: Logger: Async](appConfigManager: RawlsConfigManager,
                                               config: Config,
                                               metricsPrefix: String
  )(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
  ): cats.effect.Resource[F, AppDependencies[F]] = {
    val oidcConfig = config.getConfig("oidc")

    implicit val logger: StructuredLogger[F] = Slf4jLogger.getLogger[F]
    for {
      googleStorage <- GoogleStorageServiceFactory.createGoogleStorageService(appConfigManager)
      googleStorageTransferService <- StorageTransferServiceFactory.createStorageTransferService(
        appConfigManager
      )
      googleServiceHttp <- GoogleServiceHttpFactory.createGoogleServiceHttp(appConfigManager, executionContext)
      topicAdmin <- GoogleTopicAdminFactory.createGoogleTopicAdmin(appConfigManager)
      bqServiceFactory = GoogleBigQueryServiceFactory.createGoogleBigQueryServiceFactory(
        appConfigManager
      )(executionContext)
      httpGoogleIamDAO = GoogleIamDAOFactory.createGoogleIamDAO(appConfigManager, metricsPrefix)(
        executionContext,
        system
      )
      httpGoogleStorageDAO = GoogleStorageDAOFactory.createHttpGoogleStorageDAO(appConfigManager, metricsPrefix)(
        executionContext,
        system
      )

      openIdConnect <- cats.effect.Resource.eval(
        OpenIDConnectConfiguration[F](
          oidcConfig.getString("authorityEndpoint"),
          ClientId(oidcConfig.getString("oidcClientId")),
          extraAuthParams = Some("prompt=login"),
          authorityEndpointWithGoogleBillingScope =
            oidcConfig.getStringOption("authorityEndpointWithGoogleBillingScope")
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
                                       httpGoogleIamDAO: GoogleIamDAO,
                                       httpGoogleStorageDAO: GoogleStorageDAO,
                                       oidcConfiguration: OpenIDConnectConfiguration
)
