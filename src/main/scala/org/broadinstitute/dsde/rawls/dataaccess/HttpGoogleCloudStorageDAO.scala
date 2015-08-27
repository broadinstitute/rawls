package org.broadinstitute.dsde.rawls.dataaccess

import java.io.{File, StringReader}

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleCredential, GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.{FileDataStoreFactory, DataStoreFactory}
import com.google.api.services.admin.directory._
import com.google.api.services.admin.directory.model._
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.storage.{StorageScopes, Storage}
import com.google.api.services.storage.model.{Bucket, BucketAccessControl}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{WorkspaceACLUpdate, WorkspaceACL, WorkspaceAccessLevel, WorkspaceName}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

// Seq[String] -> Collection<String>

class HttpGoogleCloudStorageDAO(
  useServiceAccountForBuckets: Boolean,
  clientSecretsJson: String,
  dataStoreRoot: String,
  rawlsRedirectBaseUrl: String,
  p12File: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String) extends GoogleCloudStorageDAO {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  val groupAccessLevelsAscending = Seq(WorkspaceAccessLevel.Read, WorkspaceAccessLevel.Write, WorkspaceAccessLevel.Owner)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(clientSecretsJson))
  val dataStoreFactory = new FileDataStoreFactory(new File(dataStoreRoot))

  val flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, jsonFactory, clientSecrets, storageScopes).setDataStoreFactory(dataStoreFactory).build()

  override def getRawlsRedirectURI(callbackPath: String): String = s"${rawlsRedirectBaseUrl}/${callbackPath}"

  override def getGoogleRedirectURI(userId: String, callbackPath: String): String = {
    flow.newAuthorizationUrl()
     .setRedirectUri(getRawlsRedirectURI(callbackPath))
     .setState(userId)
     .setAccessType("offline")   // enables token refresh
     .build()
  }

  override def setupACL(ownerId: String, bucketName: String, workspaceName: WorkspaceName): Unit = {
    val bucketCredential = getBucketCredential(ownerId)
    // ACLs are stored internally in three Google groups.
    groupAccessLevelsAscending foreach { level => createGoogleGroupWithACL(bucketCredential, level, bucketName, workspaceName) }

    // add the owner -- may take up to 1 minute after groups are created
    // (see https://developers.google.com/admin-sdk/directory/v1/guides/manage-groups)
    val targetMember = new Member().setEmail(ownerId).setRole(groupMemberRole)
    val directory = getGroupDirectory(getGroupServiceAccountCredential)
    directory.members.insert(makeGroupId(workspaceName, WorkspaceAccessLevel.Owner), targetMember).execute()
  }

  override def teardownACL(ownerId: String, bucketName: String, workspaceName: WorkspaceName): Unit = {
    groupAccessLevelsAscending foreach { level => deleteGoogleGroup(level, bucketName, workspaceName) }
  }

  def createGoogleGroupWithACL(bucketCredential: Credential, accessLevel: WorkspaceAccessLevel, bucketName: String, workspaceName: WorkspaceName): Unit = {
    val groupId = makeGroupId(workspaceName, accessLevel)
    val group = new Group().setEmail(groupId).setName(s"FireCloud ${workspaceName.namespace}/${workspaceName.name} ${accessLevel}")

    // create group
    val serviceCredential = getGroupServiceAccountCredential
    val directory = getGroupDirectory(serviceCredential)
    directory.groups().insert(group).execute()

    // add group to ACL (requires bucket owner's credential)
    val storage = getStorage(bucketCredential)
    val acl = new BucketAccessControl().setEntity(makeGroupEntityString(groupId)).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel)).setBucket(bucketName)
    storage.bucketAccessControls().insert(bucketName, acl).execute()
  }

  def deleteGoogleGroup(accessLevel: WorkspaceAccessLevel, bucketName: String, workspaceName: WorkspaceName): Unit = {
    val groupId = makeGroupId(workspaceName, accessLevel)
    val serviceCredential = getGroupServiceAccountCredential
    val directory = getGroupDirectory(serviceCredential)
    directory.groups.delete(groupId).execute()
  }

  override def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit = {
    if ( userId != state ) throw new RawlsException("Whoa.  We seem to be dealing with the wrong user.")
    val tokenResponse = flow.newTokenRequest(authCode)
      .setRedirectUri(getRawlsRedirectURI(callbackPath))
      .execute()

    flow.createAndStoreCredential(tokenResponse, userId)
  }

  override def createBucket(ownerId: String, projectId: String, bucketName: String): Unit = {
    val credential = getBucketCredential(ownerId)
    val storage = getStorage(credential)
    val bucket =  new Bucket().setName(bucketName)
    storage.buckets().insert(projectId, bucket).execute()
  }

  override def deleteBucket(ownerId: String, projectId: String, bucketName: String): Unit = {
    val credential = getBucketCredential(ownerId)
    val storage = getStorage(credential)
    storage.buckets().delete(bucketName).execute()
  }

  override def getMaximumAccessLevel(userId: String, workspaceName: WorkspaceName): WorkspaceAccessLevel = {
    val credential = getGroupServiceAccountCredential
    val directory = getGroupDirectory(credential)

    // we use a stream because we only care about the first access level that's valid for the user (checked in descending order)
    val accessStream = Stream(groupAccessLevelsAscending.reverse: _*) map { accessLevel: WorkspaceAccessLevel =>
      Try {
        directory.members().get(makeGroupId(workspaceName, accessLevel), userId).execute()
        accessLevel
      }
    }

    // while the head of the stream is a failure, drop it and construct a new stream
    accessStream.dropWhile(_.isFailure).headOption match {
      case Some(Success(accessLevel)) => accessLevel
      case None => WorkspaceAccessLevel.NoAccess
    }
  }

  override def getACL(bucketName: String, workspaceName: WorkspaceName): WorkspaceACL = {
    val credential = getGroupServiceAccountCredential
    val directory = getGroupDirectory(credential)

    // return the maximum access level for each user
    val aclMap = groupAccessLevelsAscending map { level: WorkspaceAccessLevel =>
      val membersQuery = directory.members.list(makeGroupId(workspaceName, level)).execute()
      // NB: if there are no members in this group, Google returns null instead of an empty list. fix that here.
      val membersList = Option(membersQuery.getMembers).getOrElse(List.empty[Member].asJava)
      membersList.map(_.getEmail -> level).toMap[String, WorkspaceAccessLevel]
    } reduceRight(_ ++ _) // note that the reduction must go left-to-right so that higher access levels override lower ones

    WorkspaceACL(aclMap)
  }

  /**
   * Try to apply a list of ACL updates
   *
   * @return a map from userId to error message (an empty map means everything was successful)
   */
  override def updateACL(bucketName: String, workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]): Map[String, String] = {
    val credential = getGroupServiceAccountCredential
    val directory = getGroupDirectory(credential)

    aclUpdates map {
      case WorkspaceACLUpdate(targetUserId, targetAccessLevel) => {
        targetUserId -> updateUserAccessOrBarf(targetUserId, targetAccessLevel, workspaceName, directory)
      }
    } collect { case (user, Some(error)) => (user, error) } toMap
  }

  private def updateUserAccessOrBarf(targetUserId: String, targetAccessLevel: WorkspaceAccessLevel, workspaceName: WorkspaceName, directory: Directory): Option[String] = {
    val currentAccessLevel = getMaximumAccessLevel(targetUserId, workspaceName)
    if (currentAccessLevel != targetAccessLevel) {
      Try {
        // first remove current group membership
        if (currentAccessLevel >= WorkspaceAccessLevel.Read) {
          val currentGroup = makeGroupId(workspaceName, currentAccessLevel)
          directory.members.delete(currentGroup, targetUserId).execute()
        }
      } match {
        case Failure(_) => Some(s"Failed to change permissions for $targetUserId.")
        case Success(_) => Try {
          // now add new group membership
          if (targetAccessLevel >= WorkspaceAccessLevel.Read) {
            val targetGroup = makeGroupId(workspaceName, targetAccessLevel)
            val targetMember = new Member().setEmail(targetUserId).setRole(groupMemberRole)
            directory.members.insert(targetGroup, targetMember).execute()
          }
        } match {
          case Failure(_) => {
            // NB: if we fail here, the target user will be missing from the workspace completely.
            // however the alternatives are worse (e.g. user being in multiple roles), so this is fine for now.
            Some(s"Failed to change permissions for $targetUserId. They have been dropped from the workspace. Please try re-adding them.")
          }
          case Success(_) => None
        }
      }
    } else None
  }

  def getStorage(credential: Credential) = {
    new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getGroupDirectory(credential: Credential) = {
    new Directory.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getBucketCredential(ownerId: String): Credential = {
    if (useServiceAccountForBuckets) getBucketServiceAccountCredential
    else getUserCredential(ownerId)
  }

  def getUserCredential(userId: String): Credential = {
    val credential = flow.loadCredential(userId)
    if (credential == null) throw new IllegalStateException(s"Can't get user credentials for $userId")
    credential
  }

  def getGroupServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientSecrets.getDetails.get("client_email").toString)
      .setServiceAccountScopes(directoryScopes)
      .setServiceAccountUser(clientSecrets.getDetails.get("sub_email").toString)
      .setServiceAccountPrivateKeyFromP12File(new java.io.File(p12File))
      .build()
  }

  def getBucketServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientSecrets.getDetails.get("client_email").toString)
      .setServiceAccountScopes(storageScopes) // grant bucket-creation powers
      .setServiceAccountPrivateKeyFromP12File(new java.io.File(p12File))
      .build()
  }

  def makeGroupId(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) = {
    s"${groupsPrefix}-${workspaceName.namespace}_${workspaceName.name}-${WorkspaceAccessLevel.toCanonicalString(accessLevel)}@${appsDomain}"
  }

  def makeGroupEntityString(groupId: String) = s"group-$groupId"
}
