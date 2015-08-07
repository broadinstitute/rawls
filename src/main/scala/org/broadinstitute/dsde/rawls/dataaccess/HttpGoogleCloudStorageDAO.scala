package org.broadinstitute.dsde.rawls.dataaccess

import java.io.StringReader
import java.util.UUID

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.DataStoreFactory
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.Bucket
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import scala.collection.JavaConversions._   // Seq[String] -> Collection<String>
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import spray.client.pipelining._

class HttpGoogleCloudStorageDAO(clientSecretsJson: String, dataStoreFactory: DataStoreFactory, ourBaseURL: String)(implicit system: ActorSystem) extends GoogleCloudStorageDAO {
  // modify these if we need more granular access in the future
  val gcsFullControl = "https://www.googleapis.com/auth/devstorage.full_control"
  val gcsReadWrite = "https://www.googleapis.com/auth/devstorage.read_write"
  val gcsReadOnly = "https://www.googleapis.com/auth/devstorage.read_only"
  val gcsStub = "https://www.googleapis.com/auth/devstorage"
  val computeFullControl = "https://www.googleapis.com/auth/compute"
  val createGroup = "https://www.googleapis.com/admin/directory/v1/groups"
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

  override def createGoogleGroup(userId: String, accessLevel: String, workspaceName: WorkspaceName): String = { //return groupId maybe?
    //println(s"""{"email": "fc-${workspaceName.namespace}_${workspaceName.name}-${accessLevel}@test.broadinstitute.org""")
    //return ""
    import system.dispatcher
    val credential = getCredential(userId)
    val pipeline = addHeader("Authorization",s"Bearer ${credential.getAccessToken}") ~> sendReceive ~> unmarshal[String]
    Await.result(pipeline(Post(s"${createGroup}", s"""{"email": "fc-${workspaceName.namespace}_${workspaceName.name}-${accessLevel}@test.broadinstitute.org", "name": "justatestgroup", "description": "JUST_A_TEST_GROUP}""")),Duration.Inf)
  }

  override def setGroupACL(userId: String, accessLevel: String, createRequest: String): Unit = {
    import system.dispatcher
    val credential = getCredential(userId)
    val pipeline = addHeader("Authorization",s"Bearer ${credential.getAccessToken}") ~> sendReceive
    Await.result(pipeline(Post(s"${gcsStub}.$accessLevel")),Duration.Inf)
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

  override def getACL(userId: String, bucketName: String): String = {
    import system.dispatcher
    createGoogleGroup(userId, "null", WorkspaceName("broad-dsde-dev", "mbemis_testws"))
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
