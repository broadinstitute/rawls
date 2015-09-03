package org.broadinstitute.dsde.rawls.integrationtest

import java.util.concurrent.TimeUnit
import java.util.logging.{LogManager, Logger}

import akka.util.Timeout
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.{RawHeader, Cookie}
import spray.http.{HttpHeader, ContentTypes, HttpCookie, HttpEntity}
import spray.json.{JsonWriter, _}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration.FiniteDuration

trait IntegrationTestBase extends FlatSpec with ScalatestRouteTest with Matchers with IntegrationTestConfig with StandardUserInfoDirectives {
  val timeoutDuration = new FiniteDuration(5, TimeUnit.SECONDS)
  implicit val timeout = Timeout(timeoutDuration)

  val containerDAO = GraphContainerDAO(
    new GraphWorkflowDAO(new GraphSubmissionDAO()),
    new GraphWorkspaceDAO(),
    new GraphEntityDAO(),
    new GraphMethodConfigurationDAO(),
    new GraphSubmissionDAO()
  )

  def addSecurityHeaders: RequestTransformer = {
    addHeader(RawHeader("OIDC_access_token", "accesstoken")) ~>
    addHeader(RawHeader("OIDC_access_token_expires", "123")) ~>
    addHeader(RawHeader("OIDC_CLAIM_email", "foo@bar.com"))
  }

  // convenience methods - TODO add these to unit tests too?
  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = HttpEntity(ContentTypes.`application/json`, obj.toJson.toString())
  def repeat[T](n: Int)(exp: => T) = (1 to n) map (_ => exp)

  // suppress Java logging (otherwise OrientDB will produce a ton of useless log messages)
  LogManager.getLogManager().reset()
  Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)

  def workspaceServiceWithDbName(dbName: String) = {
    // setup DB. if it already exists, drop and then re-create it.
    val dbUrl = s"remote:${orientServer}/${dbName}"
    val admin = new OServerAdmin(dbUrl).connect(orientRootUser, orientRootPassword)
    if (admin.existsDatabase()) admin.dropDatabase(dbName)
    admin.createDatabase("graph", "plocal") // storage type is 'plocal' even though this is a remote server
    val dataSource = DataSource(dbUrl, orientRootUser, orientRootPassword, 0, 30)

    dataSource.inTransaction { txn => txn.withGraph { graph => VertexSchema.createVertexClasses(graph.asInstanceOf[OrientGraph]) } }

    // NB: use mock DAO for the base, we'll test the real GCS stuff in its own spec
    val gcsDAO = MockGoogleCloudStorageDAO

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(executionServiceServer),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")

    WorkspaceService.constructor(
      dataSource,
      containerDAO,
      new HttpMethodRepoDAO(methodRepoServer),
      new HttpExecutionServiceDAO(executionServiceServer),
      gcsDAO, submissionSupervisor
    )_
  }

}
