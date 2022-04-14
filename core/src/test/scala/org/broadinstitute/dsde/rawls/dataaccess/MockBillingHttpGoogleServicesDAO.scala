package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.effect.{IO, Temporal}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.cloudbilling.model.BillingAccount
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.model._

import scala.collection.mutable
import scala.concurrent._

class MockBillingHttpGoogleServicesDAO( useServiceAccountForBuckets: Boolean,
  override val clientSecrets: GoogleClientSecrets,
  clientEmail: String,
  subEmail: String,
  pemFile: String,
  pathToCredentialJson: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int,
  serviceProject: String,
  billingPemEmail: String,
  billingPemFile: String,
  billingEmail: String,
  billingGroupEmail: String,
  billingGroupEmailAliases: List[String],
  bucketLogsMaxAge: Int,
  resourceBufferJsonFile: String)
  (implicit override val system: ActorSystem, override val materializer: Materializer, override val executionContext: ExecutionContext, override val timer: Temporal[IO])
  extends HttpGoogleServicesDAO(
    true,
    clientSecrets,
    clientEmail,
    subEmail,
    pemFile,
    appsDomain,
    12345,
    groupsPrefix,
    appName,
    deletedBucketCheckSeconds,
    serviceProject,
    billingPemEmail,
    billingPemFile,
    billingEmail,
    billingGroupEmail,
    billingGroupEmailAliases,
    billingProbeEmail = "billingprobe@deployment-manager-project.iam.gserviceaccount.com",
    bucketLogsMaxAge,
    googleStorageService = null,
    googleServiceHttp = null,
    topicAdmin = null,
    workbenchMetricBaseName = "test",
    proxyNamePrefix = "",
    deploymentMgrProject = "deployment-manager-project",
    cleanupDeploymentAfterCreating = true,
    terraBucketReaderRole = "fakeTerraBucketReader",
    terraBucketWriterRole = "fakeTerraBucketWriter",
    accessContextManagerDAO = new MockGoogleAccessContextManagerDAO,
    resourceBufferJsonFile = resourceBufferJsonFile)(system, materializer, executionContext, timer) {

  private var token: String = null

  var mockProxyGroups = mutable.Map[RawlsUser, Boolean]()

  override def getBucketServiceAccountCredential: Credential = getPreparedMockGoogleCredential()

  def getPreparedMockGoogleCredential(): MockGoogleCredential = {
    val credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)
    credential.setRefreshToken(token)
    credential.setExpiresInSeconds(1000000L) // make sure not to refresh this token
    credential
  }

  protected override def listBillingAccounts(credential: Credential)(implicit executionContext: ExecutionContext): Future[Seq[BillingAccount]] = {
    val firecloudHasThisOne = new BillingAccount()
      .setDisplayName("testBillingAccount")
      .setName("billingAccounts/firecloudHasThisOne")
      .setOpen(true)

    val firecloudDoesntHaveThisOne = new BillingAccount()
      .setDisplayName("testBillingAccount")
      .setName("billingAccounts/firecloudDoesntHaveThisOne")
      .setOpen(true)

    Future.successful(Seq(firecloudHasThisOne, firecloudDoesntHaveThisOne))
  }

  override def testDMBillingAccountAccess(billingAccountId: RawlsBillingAccountName): Future[Boolean] = {
    billingAccountId match {
      case RawlsBillingAccountName("billingAccounts/firecloudHasThisOne") => Future.successful(true)
      case RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne") => Future.successful(false)
      case _ => throw new RawlsException(s"unexpected billingAccountId $billingAccountId")
    }
  }
}
