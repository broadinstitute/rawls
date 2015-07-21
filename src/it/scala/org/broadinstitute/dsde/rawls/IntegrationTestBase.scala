package org.broadinstitute.dsde.rawls

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.logging.{Logger, LogManager}

import akka.util.Timeout
import com.google.api.client.util.store.FileDataStoreFactory
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.openam.{RawlsOpenAmConfig, RawlsOpenAmClient, StandardOpenAmDirectives}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.vault.common.openam.OpenAMResponse.AuthenticateResponse
import org.scalatest.{FlatSpec, Matchers}
import spray.http.{ContentTypes, HttpEntity, HttpCookie}
import spray.http.HttpHeaders.Cookie
import spray.json.JsonWriter
import spray.testkit.ScalatestRouteTest

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import spray.json._

trait IntegrationTestBase extends FlatSpec with ScalatestRouteTest with Matchers with IntegrationTestConfig with StandardOpenAmDirectives {
  val timeoutDuration = new FiniteDuration(5, TimeUnit.SECONDS)
  implicit val timeout = Timeout(timeoutDuration)
  val rawlsOpenAmClient = new RawlsOpenAmClient(new RawlsOpenAmConfig(ConfigFactory.parseFile(new File("/etc/rawls.conf")).getConfig("openam")))

  def getOpenAmToken: Option[AuthenticateResponse] = {
    Some(Await.result(rawlsOpenAmClient.authenticate, timeoutDuration))
  }

  lazy val openAmResponse = getOpenAmToken.get

  def addOpenAmCookie: RequestTransformer = {
    Cookie(HttpCookie("iPlanetDirectoryPro", openAmResponse.tokenId))
  }

  // convenience methods - TODO add these to unit tests too?
  def addMockOpenAmCookie = addHeader(Cookie(HttpCookie("iPlanetDirectoryPro", "test_token")))
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
    val gcsDAO = new HttpGoogleCloudStorageDAO(gcsSecretsJSON,
                    new FileDataStoreFactory(new File(gcsDataStoreRoot)),
                    gcsRedirectUrl)

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      new GraphSubmissionDAO(new GraphWorkflowDAO()),
      new HttpExecutionServiceDAO(executionServiceServer),
      new GraphWorkflowDAO(),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")

    WorkspaceService.constructor(
      dataSource,
      new GraphWorkspaceDAO(),
      new GraphEntityDAO(),
      new GraphMethodConfigurationDAO(),
      new HttpMethodRepoDAO(methodRepoServer),
      new HttpExecutionServiceDAO(executionServiceServer),
      gcsDAO, submissionSupervisor,
      new GraphSubmissionDAO(new GraphWorkflowDAO)
    )_
  }

}
