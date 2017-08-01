package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.client.http.HttpResponseException
import com.google.api.services.cloudbilling.model.BillingAccount
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import spray.http.StatusCodes

import scala.collection.mutable
import scala.concurrent._

class MockBillingHttpGoogleServicesDAO( useServiceAccountForBuckets: Boolean,
  override val clientSecrets: GoogleClientSecrets,
  pemFile: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int,
  serviceProject: String,
  tokenEncryptionKey: String,
  tokenClientSecretsJson: String,
  billingClientSecrets: GoogleClientSecrets,
  billingPemEmail: String,
  billingPemFile: String,
  billingEmail: String,
  bucketLogsMaxAge: Int)
  (implicit override val system: ActorSystem, implicit override val executionContext: ExecutionContext)
  extends HttpGoogleServicesDAO(
    true,
    clientSecrets,
    pemFile,
    appsDomain,
    groupsPrefix,
    appName,
    deletedBucketCheckSeconds,
    serviceProject,
    tokenEncryptionKey,
    tokenClientSecretsJson,
    billingClientSecrets,
    billingPemEmail,
    billingPemFile,
    billingEmail,
    bucketLogsMaxAge,
    workbenchMetricBaseName = "test")(system, executionContext) {

  private var token: String = null
  private var tokenDate: DateTime = null

  protected override def initTokenBucket(): Unit = {}

  var mockProxyGroups = mutable.Map[RawlsUser, Boolean]()
  override def createProxyGroup(user: RawlsUser): Future[Unit] = {
    mockProxyGroups += (user -> false)
    Future.successful(())
  }

  override def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]] = {
    Future.successful(Option(getPreparedMockGoogleCredential()))
  }

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

  protected override def credentialOwnsBillingAccount(credential: Credential, billingAccountName: String): Future[Boolean] = {
    billingAccountName match {
      case "billingAccounts/firecloudHasThisOne" => Future.successful(true)
      case "billingAccounts/firecloudDoesntHaveThisOne" => Future.successful(false)
    }
  }
}