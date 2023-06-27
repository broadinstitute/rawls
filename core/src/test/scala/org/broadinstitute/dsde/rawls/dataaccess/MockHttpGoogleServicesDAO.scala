package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.effect.{IO, Temporal}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.cloudbilling.model.{BillingAccount, ListBillingAccountsResponse}
import org.broadinstitute.dsde.rawls.google.MockGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented.GoogleCounters
import org.broadinstitute.dsde.rawls.model._

import scala.collection.mutable
import scala.concurrent._

class MockHttpGoogleServicesDAO(override val clientSecrets: GoogleClientSecrets,
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
                                resourceBufferJsonFile: String
)(implicit
  override val system: ActorSystem,
  override val materializer: Materializer,
  override val executionContext: ExecutionContext,
  override val timer: Temporal[IO]
) extends HttpGoogleServicesDAO(
      clientSecrets,
      clientEmail,
      subEmail,
      pemFile,
      appsDomain,
      groupsPrefix,
      appName,
      serviceProject,
      billingPemEmail,
      billingPemFile,
      billingEmail,
      billingGroupEmail,
      googleStorageService = null,
      workbenchMetricBaseName = "test",
      proxyNamePrefix = "",
      terraBucketReaderRole = "fakeTerraBucketReader",
      terraBucketWriterRole = "fakeTerraBucketWriter",
      accessContextManagerDAO = new MockGoogleAccessContextManagerDAO,
      resourceBufferJsonFile = resourceBufferJsonFile
    )(
      system,
      materializer,
      executionContext,
      timer
    ) {

  var mockProxyGroups = mutable.Map[RawlsUser, Boolean]()

  val accessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudHasThisOne")
  val inaccessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")
  val nonOpenBillingAccountName = RawlsBillingAccountName("billingAccounts/nonOpen")

  override def getBucketServiceAccountCredential: Credential = getPreparedMockGoogleCredential()

  def getPreparedMockGoogleCredential(): MockGoogleCredential = {
    val credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)
    credential.setRefreshToken(null)
    credential.setExpiresInSeconds(1000000L) // make sure not to refresh this token
    credential
  }

  override protected def executeGoogleListBillingAccountsRequest(credential: Credential,
                                                                 pageToken: Option[String] = None
  )(implicit counters: GoogleCounters): ListBillingAccountsResponse =
    pageToken match {
      case None =>
        val response = new ListBillingAccountsResponse()
        val firecloudHasThisOne = new BillingAccount()
          .setDisplayName("testBillingAccount")
          .setName(accessibleBillingAccountName.value)
          .setOpen(true)
        response.setBillingAccounts(java.util.List.of(firecloudHasThisOne)).setNextPageToken("endOfPage1")
      case Some("endOfPage1") =>
        val response = new ListBillingAccountsResponse()
        val firecloudDoesntHaveThisOne = new BillingAccount()
          .setDisplayName("testBillingAccount")
          .setName(inaccessibleBillingAccountName.value)
          .setOpen(true)
        response.setBillingAccounts(java.util.List.of(firecloudDoesntHaveThisOne)).setNextPageToken("endOfPage2")
      case Some("endOfPage2") =>
        val response = new ListBillingAccountsResponse()
        val nonOpenAccount = new BillingAccount()
          .setDisplayName("testBillingAccount")
          .setName(nonOpenBillingAccountName.value)
          .setOpen(false)
        response.setBillingAccounts(java.util.List.of(nonOpenAccount)).setNextPageToken("")
      case _ =>
        val response = new ListBillingAccountsResponse()
        response.setBillingAccounts(java.util.List.of()).setNextPageToken("")
    }
}
