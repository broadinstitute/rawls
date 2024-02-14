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
import io.opentelemetry.sdk.{OpenTelemetrySdk, resources}
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.semconv.ResourceAttributes
import io.sentry.{Hint, Sentry, SentryEvent, SentryOptions}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.rawls.billing._
import org.broadinstitute.dsde.rawls.bucketMigration.{BucketMigrationService, BucketMigration}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.HttpDataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.{HttpResourceBufferDAO, ResourceBufferDAO}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.{HttpWorkspaceManagerClientProvider, HttpWorkspaceManagerDAO}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, StructuredLogger}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.drs.{DrsHubResolver, MarthaResolver}
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.fastpass.{FastPassService, FastPass}
import org.broadinstitute.dsde.rawls.multiCloudFactory._
import org.broadinstitute.dsde.rawls.genomics.{GenomicsService, GenomicsServiceRequest}
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, HttpGoogleAccessContextManagerDAO, HttpGooglePubSubDAO}
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
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceAclManager, MultiCloudWorkspaceService, RawlsWorkspaceAclManager, WorkspaceRepository, WorkspaceService}
import org.broadinstitute.dsde.workbench.dataaccess.PubSubNotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, HttpGoogleBigQueryDAO, HttpGoogleIamDAO, HttpGoogleStorageDAO}
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
    val appConfigManager = new MultiCloudAppConfigManager

    // we need an ActorSystem to host our application in
    implicit val system: ActorSystem = ActorSystem("rawls")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    instantiateOpenTelemetry(appConfigManager.conf)

    val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcProfile]("slick", appConfigManager.conf))

    val liquibaseConf = appConfigManager.conf.getConfig("liquibase")
    val liquibaseChangeLog = liquibaseConf.getString("changelog")
    val initWithLiquibase = liquibaseConf.getBoolean("initWithLiquibase")

    val changelogParams = Map("gcs:appsDomain" -> appConfigManager.gcsConfig.getString("appsDomain"))

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

    val jsonFactory = GsonFactory.getDefaultInstance

    val accessContextManagerDAO = MultiCloudAccessContextManagerFactory.createAccessContextManager(metricsPrefix, appConfigManager)

    initAppDependencies[IO](appConfigManager.conf, appConfigManager.gcsConfig.getString("appName"), metricsPrefix).use { appDependencies =>

      val gcsDAO = MultiCloudServicesDAOFactory.createHttpMultiCloudServicesDAO(
        appConfigManager,
        appDependencies,
        metricsPrefix,
        accessContextManagerDAO
      )

      val pubSubDAO = MultiCloudPubSubDAOFactory.createPubSubDAO(
        appConfigManager,
        metricsPrefix,
        appConfigManager.gcsConfig.getString("serviceProject")
      )

      // Import service uses a different project for its pubsub topic
      val importServicePubSubDAO = MultiCloudPubSubDAOFactory.createPubSubDAO(
        appConfigManager,
        metricsPrefix,
        appConfigManager.conf.getString("avroUpsertMonitor.updateImportStatusPubSubProject")
      )

      val importServiceDAO = MultiCloudImportServiceDAOFactory.createMultiCloudImportServiceDAO(appConfigManager)

      val bqJsonCreds = MultiCloudBigQueryCredentialsManager.getMultiCloudBucketMigrationService(appConfigManager)

      val bigQueryDAO = MultiCloudBigQueryDAOFactory.createHttpMultiCloudBigQueryDAO(appConfigManager, Json(bqJsonCreds), metricsPrefix)

      val samConfig = appConfigManager.conf.getConfig("sam")
      val samDAO = new HttpSamDAO(
        samConfig.getString("server"),
        gcsDAO.getBucketServiceAccountCredential,
        toScalaDuration(samConfig.getDuration("timeout"))
      )

      MultiCloudEnableServiceAccountFactory.createEnableServiceAccount(appConfigManager, gcsDAO, samDAO)

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
      val requesterPaysRole = appConfigManager.gcsConfig.getString("requesterPaysRole")

      val notificationPubSubDAO = MultiCloudNotificationPubSubDAOFactory.createMultiCloudNotificationPubSubDAO(
        appConfigManager,
        workbenchMetricBaseName = metricsPrefix)

      val notificationDAO = MultiCloudNotificationDAOFactory.createMultiCloudNotificationDAO(
        appConfigManager,
        notificationPubSubDAO)

      val drsResolver = MultiCloudDrsResolverFactory.createMultiCloudDrsResolver(appConfigManager)

      val servicePerimeterService = MultiCloudServicePerimeterServiceFactory.createMultiCloudNotificationPubSubDAO(
        appConfigManager,
        slickDataSource,
        gcsDAO)

      val multiCloudWorkspaceConfig = MultiCloudWorkspaceConfig.apply(appConfigManager.conf)
      val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
        new HttpBillingProfileManagerClientProvider(appConfigManager.conf.getStringOption("billingProfileManager.baseUrl")),
        multiCloudWorkspaceConfig
      )

      val genomicsServiceConstructor: RawlsRequestContext => GenomicsServiceRequest =
        MultiCloudGenomicsServiceFactory.createMultiCloudGenomicsService(appConfigManager, slickDataSource, gcsDAO)

      val submissionCostService = MultiCloudSubmissionCostServiceFactory.createMultiCloudSubmissionCostService(appConfigManager, bigQueryDAO)

      val methodRepoDAO = MultiCloudMethodRepoDAOFactory.createMultiCloudMethodRepoDAO(appConfigManager, metricsPrefix)

      val workspaceManagerDAO =
        new HttpWorkspaceManagerDAO(new HttpWorkspaceManagerClientProvider(appConfigManager.conf.getString("workspaceManager.baseUrl")))

      val dataRepoDAO =
        new HttpDataRepoDAO(appConfigManager.conf.getString("dataRepo.terraInstanceName"), appConfigManager.conf.getString("dataRepo.terraInstance"))

      val userServiceConstructor: RawlsRequestContext => UserService =
        UserService.constructor(
          slickDataSource,
          gcsDAO,
          samDAO,
          appDependencies.bigQueryServiceFactory,
          bqJsonCreds,
          requesterPaysRole,
          servicePerimeterService,
          RawlsBillingAccountName(appConfigManager.gcsConfig.getString("adminRegisterBillingAccountId")),
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
            topicsToCheck = Seq(appConfigManager.gcsConfig.getString("notifications.topicName")),
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

      val workspaceServiceConfig = WorkspaceServiceConfig.apply(appConfigManager.conf)

      val bondApiDAO: BondApiDAO = MultiCloudBondApiDAOFactory.createMultiCloudBondApiDAO(appConfigManager)

      val requesterPaysSetupService: RequesterPaysSetup =
        MultiCloudRequesterPaysSetupServiceFactory.createAccessContextManager(appConfigManager, slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole)

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

      val resourceBufferDAO: ResourceBufferDAO = MultiCloudResourceBufferDAOFactory.createResourceBuffer(appConfigManager, gcsDAO.getResourceBufferServiceAccountCredential)

      val resourceBufferService = MultiCloudResourceBufferServiceFactory.createResourceBufferService(appConfigManager,resourceBufferDAO)
      val resourceBufferSaEmail = MultiCloudResourceBufferEmailManager.getMultiCloudResourceBufferEmail(appConfigManager)

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

      val fastPassServiceConstructor: (RawlsRequestContext, SlickDataSource) => FastPass =
        MultiCloudFastPassServiceConstructorFactory.createCloudFastPassService(
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
        terraBillingProjectOwnerRole = appConfigManager.gcsConfig.getString("terraBillingProjectOwnerRole"),
        terraWorkspaceCanComputeRole = appConfigManager.gcsConfig.getString("terraWorkspaceCanComputeRole"),
        terraWorkspaceNextflowRole = appConfigManager.gcsConfig.getString("terraWorkspaceNextflowRole"),
        terraBucketReaderRole = appConfigManager.gcsConfig.getString("terraBucketReaderRole"),
        terraBucketWriterRole = appConfigManager.gcsConfig.getString("terraBucketWriterRole"),
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

      val spendReportingBigQueryService =
        appDependencies.bigQueryServiceFactory.getServiceFromJson(bqJsonCreds,
                                                                  GoogleProject(appConfigManager.gcsConfig.getString("serviceProject"))
        )
      val spendReportingServiceConfig = SpendReportingServiceConfig(
        appConfigManager.gcsConfig.getString("billingExportTableName"),
        appConfigManager.gcsConfig.getString("billingExportTimePartitionColumn"),
        appConfigManager.gcsConfig.getConfig("spendReporting").getInt("maxDateRange"),
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

      val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigration =
        MultiCloudBucketMigrationServiceFactory.createMultiCloudBucketMigrationService(appConfigManager, slickDataSource, samDAO, gcsDAO)

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

        BootMonitors.bootMonitors(
          system,
          appConfigManager.conf,
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
