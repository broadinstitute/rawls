package org.broadinstitute.dsde.rawls.dataaccess

import java.io.StringReader
import java.util.UUID

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.DataStoreFactory
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.Bucket
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Condition, Action}
import org.broadinstitute.dsde.rawls.RawlsException
import scala.collection.JavaConversions._
import scala.util.{Failure, Try}

// Seq[String] -> Collection<String>
import scala.concurrent.Await
import scala.concurrent.duration._
import spray.client.pipelining._

class HttpGoogleCloudStorageDAO(clientSecretsJson: String,
                                dataStoreFactory: DataStoreFactory,
                                ourBaseURL: String,
                                deletedBucketCheckSeconds: Int)(implicit system: ActorSystem) extends GoogleCloudStorageDAO {
  // modify these if we need more granular access in the future
  val gcsFullControl = "https://www.googleapis.com/auth/devstorage.full_control"
  val computeFullControl = "https://www.googleapis.com/auth/compute"
  val scopes = Seq(gcsFullControl,computeFullControl)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(clientSecretsJson))

  val flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, jsonFactory, clientSecrets, scopes).setDataStoreFactory(dataStoreFactory).build()

  override def getOurRedirectURI(callbackPath: String): String = s"${ourBaseURL}/${callbackPath}"

  override def getGoogleRedirectURI(userId: String, callbackPath: String): String = {
    flow.newAuthorizationUrl()
     .setRedirectUri(getOurRedirectURI(callbackPath))
     .setState(userId)
     .setAccessType("offline")   // enables token refresh
     .build()
  }

  override def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit = {
    if ( userId != state ) throw new RawlsException("Whoa.  We seem to be dealing with the wrong user.")
    val tokenResponse = flow.newTokenRequest(authCode)
      .setRedirectUri(getOurRedirectURI(callbackPath))
      .execute()

    flow.createAndStoreCredential(tokenResponse, userId)
  }

  override def createBucket(userId: String, projectId: String, bucketName: String): Unit = {
    val credential = getCredential(userId)
    val storage = new Storage.Builder(httpTransport, jsonFactory, credential).build()
    val bucket =  new Bucket().setName(bucketName)

    storage.buckets()
      .insert(projectId, bucket)
      .execute()
  }

  override def deleteBucket(userId: String, projectId: String, bucketName: String): Unit = {
    import system.dispatcher
    def bucketDeleteFn(): Unit = {
      val credential = getCredential(userId)
      val storage = new Storage.Builder(httpTransport, jsonFactory, credential).build()
      val bucket: Bucket = storage.buckets().get(bucketName).execute()

      //Google doesn't let you delete buckets that are full.
      //You can either remove all the objects manually, or you can set up lifecycle management on the bucket.
      //This can be used to auto-delete all objects next time the Google lifecycle manager runs (~every 24h).
      //More info: http://bit.ly/1WCYhhf and http://bit.ly/1Py6b6O
      val deleteEverythingRule = new Lifecycle.Rule()
        .setAction(new Action().setType("delete"))
        .setCondition(new Condition().setAge(0))

      val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule))
      bucket.setLifecycle(lifecycle)

      Try(storage.buckets().delete(bucketName).execute) match {
        //Google returns 409 Conflict if the bucket isn't empty.
        case Failure(gjre: GoogleJsonResponseException) if gjre.getDetails.getCode == 409 =>
          system.scheduler.scheduleOnce(deletedBucketCheckSeconds seconds)(bucketDeleteFn)
        //TODO: I am entirely unsure what other cases might want to be handled here.
        //404 means the bucket is already gone, which is fine.
        //Success is also fine, clearly.
      }
    }
    system.scheduler.scheduleOnce(deletedBucketCheckSeconds seconds)(bucketDeleteFn)
  }

  override def getACL(userId: String, bucketName: String): String = {
    import system.dispatcher
    val credential = getCredential(userId)
    val pipeline = addHeader("Authorization",s"Bearer ${credential.getAccessToken}") ~> sendReceive ~> unmarshal[String]
    Await.result(pipeline(Get(s"https://www.googleapis.com/storage/v1/b/${bucketName}/acl")),Duration.Inf)
  }

  override def putACL(userId: String, bucketName: String, acl: String): Unit = {
    import system.dispatcher
    val credential = getCredential(userId)
    val pipeline = addHeader("Authorization",s"Bearer ${credential.getAccessToken}") ~> sendReceive
    Await.result(pipeline(Put(s"https://www.googleapis.com/storage/v1/b/${bucketName}/acl",acl)),Duration.Inf)
  }

  private def getCredential(userId: String): Credential = {
    val credential = flow.loadCredential(userId)
    if ( credential == null )
      throw new IllegalStateException("Can't get user credentials")
    credential
  }
}
