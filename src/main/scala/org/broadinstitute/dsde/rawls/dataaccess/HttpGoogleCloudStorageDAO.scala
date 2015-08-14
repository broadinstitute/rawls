package org.broadinstitute.dsde.rawls.dataaccess

import java.io.StringReader
import java.io.File
import java.security.PrivateKey
import java.util.UUID

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleCredential, GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.DataStoreFactory
import com.google.api.services.admin.directory.Directory.Builder
import com.google.api.services.admin.directory._
import com.google.api.services.admin.directory.model._
import com.google.api.services.storage.Storage
import com.google.api.services.storage.Storage.Builder
import com.google.api.services.storage.model.{ObjectAccessControl, StorageObject, Bucket, BucketAccessControl}
import com.google.api.services._
import com.google.common.collect.ImmutableList
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.GCSAccessLevel
import org.broadinstitute.dsde.rawls.model.GCSAccessLevel.GCSAccessLevel
import org.broadinstitute.dsde.rawls.model.{BucketAccessControls, GCSAccessLevel, BucketAccessControl, WorkspaceName}
import spray.json.JsonParser
import scala.collection.JavaConversions._   // Seq[String] -> Collection<String>
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import spray.client.pipelining._

class HttpGoogleCloudStorageDAO(clientSecretsJson: String, dataStoreFactory: DataStoreFactory, ourBaseURL: String)(implicit system: ActorSystem) extends GoogleCloudStorageDAO {
  // modify these if we need more granular access in the future
  val gcsFullControl = "https://www.googleapis.com/auth/devstorage.full_control"
  val computeFullControl = "https://www.googleapis.com/auth/compute"
  val scopes = Seq(gcsFullControl,computeFullControl)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val conf = ConfigFactory.parseFile(new File("/etc/rawls.conf"))
  val gcsConfig = conf.getConfig("gcs")

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

  override def createGoogleGroup(userId: String, accessLevel: String, workspaceName: WorkspaceName, bucketName: String) = {
    val credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientSecrets.getDetails.get("client_email").toString)
      .setServiceAccountScopes(directoryScopes)
      .setServiceAccountUser(clientSecrets.getDetails.get("sub_email").toString)
      .setServiceAccountPrivateKeyFromP12File(new java.io.File(gcsConfig.getString("pathToP12"))) //add this file to jenkins?
      .build()
    val appsDomain = gcsConfig.getString("appsDomain")
    val groupsPrefix = gcsConfig.getString("groupsPrefix")

    val directory = new Directory.Builder(httpTransport, jsonFactory, credential).setApplicationName("firecloud:rawls").build()
    val group = new Group().setEmail(s"${groupsPrefix}-${workspaceName.namespace}_${workspaceName.name}-${accessLevel}@${appsDomain}").setName(s"FireCloud ${workspaceName.namespace}/${workspaceName.name} ${accessLevel}")
    val member = new Member().setEmail(userId).setRole(GCSAccessLevel.toGoogleString(GCSAccessLevel.Owner))

    directory.groups().insert(group).execute()
    directory.members().insert(s"${groupsPrefix}-${workspaceName.namespace}_${workspaceName.name}-${accessLevel}@${appsDomain}", member).execute()
  }

  override def setGroupACL(userId: String, groupId: String, bucketName: String, groupRole: String): Unit = {
    import com.google.api.services.storage.model.BucketAccessControl

    val credential = getCredential(userId)
    val storage = new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName("firecloud:rawls").build()
    val acl = new BucketAccessControl().setEntity("group-" + groupId).setRole(groupRole).setBucket(bucketName)

    storage.bucketAccessControls().insert(bucketName, acl).execute()
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
    val storage = new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName("firecloud:rawls").build()
    val bucket =  new Bucket().setName(bucketName)

    storage.buckets()
      .insert(projectId, bucket)
      .execute()
  }

  override def getMaximumAccessLevel(userId: String, workspaceName: WorkspaceName): GCSAccessLevel = {
    val appsDomain = gcsConfig.getString("appsDomain")
    val groupsPrefix = gcsConfig.getString("groupsPrefix")
    val credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientSecrets.getDetails.get("client_email").toString)
      .setServiceAccountScopes(directoryScopes)
      .setServiceAccountUser(clientSecrets.getDetails.get("sub_email").toString)
      .setServiceAccountPrivateKeyFromP12File(new java.io.File(gcsConfig.getString("pathToP12"))) //add this file to jenkins?
      .build()

    val owner = GCSAccessLevel.toGoogleString(GCSAccessLevel.Owner)
    val reader = GCSAccessLevel.toGoogleString(GCSAccessLevel.Read)
    val writer = GCSAccessLevel.toGoogleString(GCSAccessLevel.Write)

    val directory = new Directory.Builder(httpTransport, jsonFactory, credential).setApplicationName("firecloud:rawls").build()

    try {
      directory.members().get(s"${groupsPrefix}-${workspaceName.namespace}_${workspaceName.name}-${owner}@${appsDomain}", userId).execute()
      return GCSAccessLevel.Owner
    } catch {
      case _: GoogleJsonResponseException => //no problem, just means the user wasn't a member of owners
    }

    try {
      directory.members().get(s"${groupsPrefix}-${workspaceName.namespace}_${workspaceName.name}-${writer}@${appsDomain}", userId).execute()
      return GCSAccessLevel.Write
    } catch {
      case _: GoogleJsonResponseException => //no problem, just means the user wasn't a member of writers
    }

    try {
      directory.members().get(s"${groupsPrefix}-${workspaceName.namespace}_${workspaceName.name}-${reader}@${appsDomain}", userId).execute()
      return GCSAccessLevel.Read
    } catch {
      case _: GoogleJsonResponseException => //no problem, just means the user wasn't a member of readers
    }

    GCSAccessLevel.NoAccess
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
