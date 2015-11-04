package org.broadinstitute.dsde.rawls

import java.io.File

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.can.Http

import scala.concurrent.duration._
import scala.util.{Failure, Success}

import scala.concurrent.ExecutionContext.Implicits.global

object Boot extends App {

  private def startup(): Unit = {
    val conf = ConfigFactory.parseFile(new File("/etc/rawls.conf"))

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("rawls")

    val orientConfig = conf.getConfig("orientdb")
    val dbUrl = s"remote:${orientConfig.getString("server")}/${orientConfig.getString("dbName")}"
    val dataSource = DataSource(dbUrl, orientConfig.getString("rootUser"), orientConfig.getString("rootPassword"), 0, 30)

    dataSource.inTransaction() { txn => txn.withGraph { graph => VertexSchema.createVertexClasses(graph.asInstanceOf[OrientGraph]) } }

    val gcsConfig = conf.getConfig("gcs")
    val gcsDAO = new HttpGoogleServicesDAO(
      false,
      gcsConfig.getString("secrets"),
      gcsConfig.getString("pathToP12"),
      gcsConfig.getString("appsDomain"),
      gcsConfig.getString("groupsPrefix"),
      gcsConfig.getString("appName"),
      gcsConfig.getInt("deletedBucketCheckSeconds"),
      gcsConfig.getString("serviceProject")
    )

    system.registerOnTermination {
      dataSource.shutdown()
    }

    val containerDAO = GraphContainerDAO(
      new GraphWorkflowDAO(new GraphSubmissionDAO()),
      new GraphWorkspaceDAO(),
      new GraphEntityDAO(),
      new GraphMethodConfigurationDAO(),
      new GraphAuthDAO(),
      new GraphSubmissionDAO()
    )

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(conf.getConfig("executionservice").getString("server")),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")

    val service = system.actorOf(RawlsApiServiceActor.props(
                    WorkspaceService.constructor(dataSource,
                                                  containerDAO,
                                                  new HttpMethodRepoDAO(conf.getConfig("methodrepo").getString("server")),
                                                  new HttpExecutionServiceDAO(conf.getConfig("executionservice").getString("server")),
                                                  gcsDAO, submissionSupervisor),
                    UserService.constructor(dataSource, gcsDAO, containerDAO)),
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

  startup()
}
