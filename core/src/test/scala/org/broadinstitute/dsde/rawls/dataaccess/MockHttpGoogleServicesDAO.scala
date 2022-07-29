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

class MockHttpGoogleServicesDAO(
  override val clientSecrets: GoogleClientSecrets,
  clientEmail: String,
  subEmail: String,
  pemFile: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  serviceProject: String,
  billingPemEmail: String,
  billingPemFile: String,
  billingEmail: String,
  billingGroupEmail: String,
  resourceBufferJsonFile: String)(implicit override val system: ActorSystem, override val materializer: Materializer, override val executionContext: ExecutionContext, override val timer: Temporal[IO])
  extends HttpGoogleServicesDAO(
    clientSecrets,
    clientEmail,
    subEmail,
    pemFile,
    appsDomain,
    12345,
    groupsPrefix,
    appName,
    serviceProject,
    billingPemEmail,
    billingPemFile,
    billingEmail,
    billingGroupEmail,
    billingProbeEmail = "billingprobe@deployment-manager-project.iam.gserviceaccount.com",
    googleStorageService = null,
    workbenchMetricBaseName = "test",
    proxyNamePrefix = "",
    deploymentMgrProject = "deployment-manager-project",
    cleanupDeploymentAfterCreating = true,
    terraBucketReaderRole = "fakeTerraBucketReader",
    terraBucketWriterRole = "fakeTerraBucketWriter",
    accessContextManagerDAO = new MockGoogleAccessContextManagerDAO,
    resourceBufferJsonFile = resourceBufferJsonFile)(system, materializer, executionContext, timer) {

  var mockProxyGroups = mutable.Map[RawlsUser, Boolean]()

  val accessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudHasThisOne")
  val inaccessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")

  override def getBucketServiceAccountCredential: Credential = getPreparedMockGoogleCredential()

  def getPreparedMockGoogleCredential(): MockGoogleCredential = {
    val credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)
    credential.setRefreshToken(null)
    credential.setExpiresInSeconds(1000000L) // make sure not to refresh this token
    credential
  }

  protected override def listBillingAccounts(credential: Credential)(implicit executionContext: ExecutionContext): Future[List[BillingAccount]] = {
    val firecloudHasThisOne = new BillingAccount()
      .setDisplayName("testBillingAccount")
      .setName(accessibleBillingAccountName.value)
      .setOpen(true)

    val firecloudDoesntHaveThisOne = new BillingAccount()
      .setDisplayName("testBillingAccount")
      .setName(inaccessibleBillingAccountName.value)
      .setOpen(true)

    Future.successful(List(firecloudHasThisOne, firecloudDoesntHaveThisOne))
  }

  override def testDMBillingAccountAccess(billingAccountId: RawlsBillingAccountName): Future[Boolean] = {
    billingAccountId match {
      case `accessibleBillingAccountName` => Future.successful(true)
      case `inaccessibleBillingAccountName` => Future.successful(false)
      case _ => throw new RawlsException(s"unexpected billingAccountId $billingAccountId")
    }
  }
}