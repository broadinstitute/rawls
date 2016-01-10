package org.broadinstitute.dsde.rawls.integrationtest

import java.io.{File, StringReader}
import java.util.concurrent.TimeUnit
import java.util.logging.{LogManager, Logger}

import akka.util.Timeout
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential, GoogleAuthorizationCodeTokenRequest}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.storage.StorageScopes
import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.{RawHeader, Cookie}
import spray.http.{HttpHeader, ContentTypes, HttpCookie, HttpEntity}
import spray.json.{JsonWriter, _}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConversions._

trait IntegrationTestBase extends FlatSpec with ScalatestRouteTest with Matchers with IntegrationTestConfig with StandardUserInfoDirectives {
  val timeoutDuration = new FiniteDuration(5, TimeUnit.SECONDS)
  implicit val timeout = Timeout(timeoutDuration)
//  override implicit def executor = TestExecutionContext.testExecutionContext
  implicit val executionContext = TestExecutionContext.testExecutionContext

  val containerDAO = GraphContainerDAO(
    new GraphWorkflowDAO(new GraphSubmissionDAO()),
    new GraphWorkspaceDAO(),
    new GraphEntityDAO(),
    new GraphMethodConfigurationDAO(),
    new GraphAuthDAO(),
    new GraphBillingDAO(),
    new GraphSubmissionDAO()
  )

  val gcsDAO = new HttpGoogleServicesDAO(
    true, // use service account to manage buckets
    gcsConfig.getString("secrets"),
    gcsConfig.getString("pathToPem"),
    gcsConfig.getString("appsDomain"),
    gcsConfig.getString("groupsPrefix"),
    gcsConfig.getString("appName"),
    gcsConfig.getInt("deletedBucketCheckSeconds"),
    gcsConfig.getString("serviceProject"),
    gcsConfig.getString("tokenEncryptionKey"),
    gcsConfig.getString("tokenSecretsJson")
  )

  val userDirDAO = new JndiUserDirectoryDAO(
    ldapProviderUrl,
    ldapUser,
    ldapPassword,
    ldapGroupDn,
    ldapMemberAttribute,
    ldapUserObjectClasses,
    ldapUserObjectClasses,
    ldapUserDnFormat
  )

  def addSecurityHeaders: RequestTransformer = {

    val googleCred = gcsDAO.getBucketServiceAccountCredential
    googleCred.refreshToken()

    addHeader(RawHeader("OIDC_access_token", googleCred.getAccessToken)) ~>
    addHeader(RawHeader("OIDC_CLAIM_expires_in", String.valueOf(googleCred.getExpiresInSeconds))) ~>
    addHeader(RawHeader("OIDC_CLAIM_email", gcsDAO.clientSecrets.getDetails.get("client_email").toString)) ~>
    addHeader(RawHeader("OIDC_CLAIM_sub", "x"))
  }

  // convenience methods - TODO add these to unit tests too?
  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = HttpEntity(ContentTypes.`application/json`, obj.toJson.toString())
  def repeat[T](n: Int)(exp: => T) = (1 to n) map (_ => exp)

  // suppress Java logging (otherwise OrientDB will produce a ton of useless log messages)
  LogManager.getLogManager().reset()
  Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).setLevel(java.util.logging.Level.SEVERE)

  def workspaceServiceWithDbName(dbName: String) = {
    // setup DB. if it already exists, drop and then re-create it.
    val dbUrl = orientUrl.replace("rawlsdb", dbName).replace("rawls", dbName)   // TODO: don't hard-code these

    if (dbUrl.startsWith("remote")) {
      // "connect" is invalid for plocal
      val admin = new OServerAdmin(dbUrl).connect(orientRootUser, orientRootPassword)
      if (admin.existsDatabase()) admin.dropDatabase(dbName)
      admin.createDatabase("graph", "plocal") // storage type is 'plocal' even though this is a remote server
    }
    else {
      val db = new ODatabaseDocumentTx(dbUrl)
      if (db.exists()) {
        if (db.isClosed) db.open("admin", "admin")
        db.drop()
      }
      db.create()
    }

    val dataSource = DataSource(dbUrl, orientRootUser, orientRootPassword, 0, 30)
    dataSource.inTransaction() { txn => txn.withGraph { graph => VertexSchema.createVertexClasses(graph.asInstanceOf[OrientGraph]) } }

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(executionServiceServer),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "rawls-submission-supervisor")
    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(dataSource, containerDAO, gcsDAO))

    val userServiceConstructor = UserService.constructor(dataSource, gcsDAO, containerDAO, userDirDAO)_

    val workspaceServiceConstructor = WorkspaceService.constructor(
      dataSource,
      containerDAO,
      new HttpMethodRepoDAO(methodRepoServer),
      new HttpExecutionServiceDAO(executionServiceServer),
      gcsDAO,
      submissionSupervisor,
      bucketDeletionMonitor,
      userServiceConstructor
    )_

    (workspaceServiceConstructor, userServiceConstructor, dataSource)
  }

}
