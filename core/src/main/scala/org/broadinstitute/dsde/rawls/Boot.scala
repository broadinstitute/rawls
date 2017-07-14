package org.broadinstitute.dsde.rawls

import java.io.StringReader
import java.net.InetAddress
import java.util.concurrent.TimeUnit
import javax.naming.directory.AttributeInUseException

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.codahale.metrics.SharedMetricRegistries
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.jackson2.JacksonFactory
import com.readytalk.metrics.StatsDReporter
import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import slick.backend.DatabaseConfig
import slick.driver.JdbcDriver
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.HttpGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionSupervisor, WorkflowSubmissionActor}
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, UserInfo}
import org.broadinstitute.dsde.rawls.monitor._
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.rawls.util.ScalaConfig._
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.can.Http
import spray.http.StatusCodes
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object Boot extends App with LazyLogging {
  private def startup(): Unit = {
    // version.conf is generated by sbt
    val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
    val gcsConfig = conf.getConfig("gcs")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("rawls")

    val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcDriver]("slick", conf))

    val liquibaseConf = conf.getConfig("liquibase")
    val liquibaseChangeLog = liquibaseConf.getString("changelog")
    val initWithLiquibase = liquibaseConf.getBoolean("initWithLiquibase")

    val changelogParams = Map("gcs:appsDomain" -> gcsConfig.getString("appsDomain"))

    if(initWithLiquibase) {
      slickDataSource.initWithLiquibase(liquibaseChangeLog, changelogParams)
    }

    // tmp nothing just forcing a build here

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

    metricsConf.getObjectOption("reporters") match {
      case Some(configObject) =>
        configObject.entrySet.map(_.toTuple).foreach {
          case ("statsd", conf: ConfigObject) =>
            val statsDConf = conf.toConfig
            startStatsDReporter(
              statsDConf.getString("host"),
              statsDConf.getInt("port"),
              statsDConf.getDuration("period"))
          case (other, _) =>
            logger.warn(s"Unknown metrics backend: $other")
        }
      case None => logger.info("No metrics reporters defined")
    }

    val jsonFactory = JacksonFactory.getDefaultInstance
    val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("secrets")))
    val billingClientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("billingSecrets")))
    val gcsDAO = new HttpGoogleServicesDAO(
      false,
      clientSecrets,
      gcsConfig.getString("pathToPem"),
      gcsConfig.getString("appsDomain"),
      gcsConfig.getString("groupsPrefix"),
      gcsConfig.getString("appName"),
      gcsConfig.getInt("deletedBucketCheckSeconds"),
      gcsConfig.getString("serviceProject"),
      gcsConfig.getString("tokenEncryptionKey"),
      gcsConfig.getString("tokenSecretsJson"),
      billingClientSecrets,
      gcsConfig.getString("billingPemEmail"),
      gcsConfig.getString("pathToBillingPem"),
      gcsConfig.getString("billingEmail"),
      gcsConfig.getInt("bucketLogsMaxAge")
    )

    val pubSubDAO = new HttpGooglePubSubDAO(
      clientSecrets.getDetails.get("client_email").toString,
      gcsConfig.getString("pathToPem"),
      gcsConfig.getString("appName"),
      gcsConfig.getString("serviceProject")
    )

    val ldapConfig = conf.getConfig("userLdap")
    val userDirDAO = new JndiUserDirectoryDAO(
      ldapConfig.getString("providerUrl"),
      ldapConfig.getString("user"),
      ldapConfig.getString("password"),
      ldapConfig.getString("groupDn"),
      ldapConfig.getString("memberAttribute"),
      ldapConfig.getStringList("userObjectClasses").toList,
      ldapConfig.getStringList("userAttributes").toList,
      ldapConfig.getString("userDnFormat")
    )

    enableServiceAccount(gcsDAO, userDirDAO)

    system.registerOnTermination {
      slickDataSource.databaseConfig.db.shutdown
    }

    val executionServiceConfig = conf.getConfig("executionservice")
    val submissionTimeout = util.toScalaDuration(executionServiceConfig.getDuration("workflowSubmissionTimeout"))

    val executionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO] = executionServiceConfig.getObject("readServers").map {
        case (strName, strHostname) => (ExecutionServiceId(strName)->new HttpExecutionServiceDAO(strHostname.unwrapped.toString, submissionTimeout))
      }.toMap

    val executionServiceSubmitServers: Map[ExecutionServiceId, ExecutionServiceDAO] = executionServiceConfig.getObject("submitServers").map {
      case (strName, strHostname) => (ExecutionServiceId(strName)->new HttpExecutionServiceDAO(strHostname.unwrapped.toString, submissionTimeout))
    }.toMap

    val shardedExecutionServiceCluster:ExecutionServiceCluster = new ShardedHttpExecutionServiceCluster(executionServiceServers, executionServiceSubmitServers, slickDataSource)

    val submissionMonitorConfig = conf.getConfig("submissionmonitor")
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      shardedExecutionServiceCluster,
      slickDataSource,
      util.toScalaDuration(submissionMonitorConfig.getDuration("submissionPollInterval")),
      rawlsMetricBaseName = metricsPrefix
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")

    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

    val projectOwners = gcsConfig.getStringList("projectTemplate.owners")
    val projectEditors = gcsConfig.getStringList("projectTemplate.editors")
    val projectServices = gcsConfig.getStringList("projectTemplate.services")
    val projectTemplate = ProjectTemplate(Map("roles/owner" -> projectOwners, "roles/editor" -> projectEditors), projectServices)

    val notificationDAO = new PubSubNotificationDAO(pubSubDAO, gcsConfig.getString("notifications.topicName"))

    val userServiceConstructor: (UserInfo) => UserService = UserService.constructor(slickDataSource, gcsDAO, userDirDAO, pubSubDAO, gcsConfig.getString("groupMonitor.topicName"),  notificationDAO)

    system.actorOf(CreatingBillingProjectMonitor.props(slickDataSource, gcsDAO, projectTemplate))

    system.actorOf(GoogleGroupSyncMonitorSupervisor.props(
      util.toScalaDuration(gcsConfig.getDuration("groupMonitor.pollInterval")),
      util.toScalaDuration(gcsConfig.getDuration("groupMonitor.pollIntervalJitter")),
      pubSubDAO,
      gcsConfig.getString("groupMonitor.topicName"),
      gcsConfig.getString("groupMonitor.subscriptionName"),
      gcsConfig.getInt("groupMonitor.workerCount"),
      userServiceConstructor))

    BootMonitors.restartMonitors(slickDataSource, gcsDAO, submissionSupervisor, bucketDeletionMonitor)

    val genomicsServiceConstructor: (UserInfo) => GenomicsService = GenomicsService.constructor(slickDataSource, gcsDAO, userDirDAO)
    val statisticsServiceConstructor: (UserInfo) => StatisticsService = StatisticsService.constructor(slickDataSource, gcsDAO, userDirDAO)
    val agoraConfig = conf.getConfig("methodrepo")
    val methodRepoDAO = new HttpMethodRepoDAO(agoraConfig.getString("server"), agoraConfig.getString("path"))

    val maxActiveWorkflowsTotal = conf.getInt("executionservice.maxActiveWorkflowsPerServer") * executionServiceServers.size
    val maxActiveWorkflowsPerUser = maxActiveWorkflowsTotal / conf.getInt("executionservice.activeWorkflowHogFactor")
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
        rawlsMetricBaseName = metricsPrefix
      ))
    }

    val healthMonitor = system.actorOf(
      HealthMonitor.props(
        slickDataSource,
        gcsDAO,
        pubSubDAO,
        userDirDAO,
        methodRepoDAO,
        groupsToCheck = Seq(gcsDAO.adminGroupName, gcsDAO.curatorGroupName),
        topicsToCheck = Seq(gcsConfig.getString("notifications.topicName"), gcsConfig.getString("groupMonitor.topicName")),
        bucketsToCheck = Seq(gcsDAO.tokenBucketName)
      ).withDispatcher("health-monitor-dispatcher"),
      "health-monitor"
    )
    logger.info("Starting health monitor...")
    system.scheduler.schedule(10 seconds, 1 minute, healthMonitor, HealthMonitor.CheckAll)

    val statusServiceConstructor: () => StatusService = StatusService.constructor(healthMonitor)

    val service = system.actorOf(RawlsApiServiceActor.props(
      WorkspaceService.constructor(
        slickDataSource,
        methodRepoDAO,
        shardedExecutionServiceCluster,
        conf.getInt("executionservice.batchSize"),
        gcsDAO,
        notificationDAO,
        submissionSupervisor,
        bucketDeletionMonitor,
        userServiceConstructor,
        genomicsServiceConstructor,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        rawlsMetricBaseName = metricsPrefix),
      userServiceConstructor,
      genomicsServiceConstructor,
      statisticsServiceConstructor,
      statusServiceConstructor,
      ApplicationVersion(conf.getString("version.git.hash"), conf.getString("version.build.number"), conf.getString("version.version")),
      clientSecrets.getDetails.getClientId,
      submissionTimeout
    ),
      "rawls-service")

    implicit val timeout = Timeout(20.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    (IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)).onComplete {
      case Success(Http.CommandFailed(failure)) =>
        system.log.error("could not bind to port: " + failure.toString)
        system.shutdown()
      case Failure(t) =>
        system.log.error(t, "could not bind to port")
        system.shutdown()
      case _ =>
    }
  }

  /**
   * Enables the rawls service account in ldap. Allows service to service auth through the proxy.
   * @param gcsDAO
   * @param userDirDAO
   */
  def enableServiceAccount(gcsDAO: HttpGoogleServicesDAO, userDirDAO: JndiUserDirectoryDAO): Unit = {
    val enableServiceAccountFuture = for {
      serviceAccountUser <- gcsDAO.getServiceAccountRawlsUser()
      _ <- userDirDAO.createUser(serviceAccountUser.userSubjectId).recover { case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode.contains(StatusCodes.Conflict) => Unit } // if it already exists, ok
      _ <- userDirDAO.enableUser(serviceAccountUser.userSubjectId).recover { case e: AttributeInUseException => Unit } // if it is already enabled, ok
    } yield Unit

    enableServiceAccountFuture.onFailure {
      // this is logged as a warning because almost always the service account is already enabled
      // so this is a problem only the first time rawls is started with a new service account
      case t: Throwable => logger.warn("error enabling service account", t)
    }
  }

  def startStatsDReporter(host: String, port: Int, period: java.time.Duration): Unit = {
    logger.info(s"Starting statsd reporter writing to [$host:$port] with period [${period.getSeconds} seconds]")
    val reporter = StatsDReporter.forRegistry(SharedMetricRegistries.getOrCreate("default"))
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(host, port)
    reporter.start(period.getSeconds, period.getSeconds, TimeUnit.SECONDS)
  }

  startup()
}
