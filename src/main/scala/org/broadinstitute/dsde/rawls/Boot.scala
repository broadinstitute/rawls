package org.broadinstitute.dsde.rawls

import java.io.{File, StringReader}
import javax.naming.NameAlreadyBoundException
import javax.naming.directory.AttributeInUseException

import _root_.slick.backend.DatabaseConfig
import _root_.slick.driver.JdbcDriver
import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.{WorkflowSubmissionActor, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, UserInfo}
import org.broadinstitute.dsde.rawls.monitor.{BootMonitors, BucketDeletionMonitor}
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.can.Http

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

object Boot extends App with LazyLogging {
  private def toScalaDuration(javaDuration: java.time.Duration) = Duration.fromNanos(javaDuration.toNanos)

  private def startup(): Unit = {
    // version.conf is generated by sbt
    val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.parseFile(new File("/etc/rawls.conf")))

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("rawls")

    val slickDataSource = DataSource(DatabaseConfig.forConfig[JdbcDriver]("slick", conf))

    val liquibaseConf = conf.getConfig("liquibase")
    val liquibaseChangeLog = liquibaseConf.getString("changelog")
    slickDataSource.initWithLiquibase(liquibaseChangeLog)

    // For testing/migration.  Not for production code!
    //slickDataSource.initWithSlick()

    val gcsConfig = conf.getConfig("gcs")
    val jsonFactory = JacksonFactory.getDefaultInstance
    val clientSecrets = GoogleClientSecrets.load(
      jsonFactory, new StringReader(gcsConfig.getString("secrets")))
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
      gcsConfig.getString("tokenSecretsJson")
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
    val submissionTimeout = toScalaDuration(executionServiceConfig.getDuration("workflowSubmissionTimeout"))
    val executionServiceDAO = new HttpExecutionServiceDAO(
      executionServiceConfig.getString("server"),
      submissionTimeout
    )

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceDAO,
      slickDataSource
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")

    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

    BootMonitors.restartMonitors(slickDataSource, gcsDAO, submissionSupervisor, bucketDeletionMonitor)


    val userServiceConstructor: (UserInfo) => UserService = UserService.constructor(slickDataSource, gcsDAO, userDirDAO)
    val methodRepoDAO = new HttpMethodRepoDAO(conf.getConfig("methodrepo").getString("server"))

    for(i <- 0 until conf.getInt("executionservice.parallelSubmitters")) {
      system.actorOf(WorkflowSubmissionActor.props(
        slickDataSource,
        methodRepoDAO,
        gcsDAO,
        executionServiceDAO,
        conf.getInt("executionservice.batchSize"),
        gcsDAO.getBucketServiceAccountCredential,
        toScalaDuration(conf.getDuration("executionservice.pollInterval")),
        conf.getInt("executionservice.maxActiveWorkflowsTotal"),
        conf.getInt("executionservice.maxActiveWorkflowsPerUser")
      ))
    }

    val service = system.actorOf(RawlsApiServiceActor.props(
      WorkspaceService.constructor(
        slickDataSource,
        methodRepoDAO,
        executionServiceDAO,
        conf.getInt("executionservice.batchSize"),
        gcsDAO,
        submissionSupervisor,
        bucketDeletionMonitor,
        userServiceConstructor),
      userServiceConstructor,
      ApplicationVersion(conf.getString("version.git.hash"), conf.getString("version.build.number"), conf.getString("version.version")),
      clientSecrets.getDetails.getClientId,
      submissionTimeout
    ),
      "rawls-service")

    implicit val timeout = Timeout(5.seconds)
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
      _ <- userDirDAO.createUser(serviceAccountUser).recover { case e: NameAlreadyBoundException => Unit } // if it already exists, ok
      _ <- userDirDAO.enableUser(serviceAccountUser).recover { case e: AttributeInUseException => Unit } // if it is already enabled, ok
    } yield Unit

    enableServiceAccountFuture.onFailure {
      // this is logged as a warning because almost always the service account is already enabled
      // so this is a problem only the first time rawls is started with a new service account
      case t: Throwable => logger.warn("error enabling service account", t)
    }
  }

  startup()
}
