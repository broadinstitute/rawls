package org.broadinstitute.dsde.rawls.dataaccess

import java.io.{File, StringReader}
import java.io.StringReader
import java.io.File
import java.security.PrivateKey
import java.util.UUID

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleCredential, GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.admin.directory.model._
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.{StorageScopes, Storage}
import com.google.api.services.storage.model.{ObjectAccessControl, Bucket, BucketAccessControl}
import com.google.api.client.googleapis.json.GoogleJsonResponseException

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model._

// Seq[String] -> Collection<String>

class HttpGoogleCloudStorageDAO(
  useServiceAccountForBuckets: Boolean,
  clientSecretsJson: String,
  p12File: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int) extends GoogleCloudStorageDAO {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  val groupAccessLevelsAscending = Seq(WorkspaceAccessLevel.Read, WorkspaceAccessLevel.Write, WorkspaceAccessLevel.Owner)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(clientSecretsJson))

  val system = akka.actor.ActorSystem("system")

  override def setupACL(userInfo: UserInfo, bucketName: String, workspaceName: WorkspaceName): Unit = {
    val bucketCredential = getBucketCredential(userInfo)
    // ACLs are stored internally in three Google groups.
    groupAccessLevelsAscending foreach { level => createGoogleGroupWithACL(bucketCredential, level, bucketName, workspaceName) }

    // add the owner -- may take up to 1 minute after groups are created
    // (see https://developers.google.com/admin-sdk/directory/v1/guides/manage-groups)
    val targetMember = new Member().setEmail(userInfo.userEmail).setRole(groupMemberRole)
    val directory = getGroupDirectory(getGroupServiceAccountCredential)
    directory.members.insert(makeGroupId(workspaceName, WorkspaceAccessLevel.Owner), targetMember).execute()
  }

  override def teardownACL(bucketName: String, workspaceName: WorkspaceName): Unit = {
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

    val bucketAcl = new BucketAccessControl().setEntity(makeGroupEntityString(groupId)).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel)).setBucket(bucketName)
    storage.bucketAccessControls().insert(bucketName, bucketAcl).execute()

//    val objectAcl = new ObjectAccessControl().setEntity(makeGroupEntityString(groupId)).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel)).setBucket(bucketName)
//    storage.defaultObjectAccessControls().insert(bucketName, objectAcl).execute()
  }

  def deleteGoogleGroup(accessLevel: WorkspaceAccessLevel, bucketName: String, workspaceName: WorkspaceName): Unit = {
    val groupId = makeGroupId(workspaceName, accessLevel)
    val serviceCredential = getGroupServiceAccountCredential
    val directory = getGroupDirectory(serviceCredential)
    directory.groups.delete(groupId).execute()
  }

  override def createBucket(userInfo: UserInfo, projectId: String, bucketName: String): Unit = {
    val credential = getBucketCredential(userInfo)
    val storage = getStorage(credential)
    val bucket =  new Bucket().setName(bucketName)
    storage.buckets().insert(projectId, bucket).execute()
  }

  override def deleteBucket(userInfo: UserInfo, projectId: String, bucketName: String): Unit = {
    import system.dispatcher
    def bucketDeleteFn(): Unit = {
      val credential = getBucketCredential(userInfo)
      val storage = getStorage(credential)
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
        case _ =>
        //TODO: I am entirely unsure what other cases might want to be handled here.
        //404 means the bucket is already gone, which is fine.
        //Success is also fine, clearly.
      }
    }
    //It's possible we just created this bucket and need to immediately delete it because something else
    //has gone bad. In this case, the bucket will already be empty, so we should be able to immediately delete it.
    bucketDeleteFn()
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
      case _ => WorkspaceAccessLevel.NoAccess
    }
  }

  override def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair] = {
    val credential = getGroupServiceAccountCredential
    val directory = new Directory.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
    val groupsQuery = directory.groups().list().setUserKey(userId).execute()

    val workspaceGroups = Option(groupsQuery.getGroups).getOrElse(List.empty[Group].asJava)

    workspaceGroups.map(g => deconstructGroupId(g.getEmail)).toSeq
  }

  override def getWorkspace(userId: String, workspaceName: WorkspaceName): Seq[WorkspacePermissionsPair] = {
    getWorkspaces(userId).filter(ws => ws.workspaceName.equals(workspaceName))
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

  override def getOwners(workspaceName: WorkspaceName): Seq[String] = {
    val credential = getGroupServiceAccountCredential
    val directory = new Directory.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
    //a workspace should always have an owner, but just in case for some reason it doesn't...
    val ownersQuery = directory.members.list(makeGroupId(workspaceName, WorkspaceAccessLevel.Owner)).execute()

    Option(ownersQuery.getMembers).getOrElse(List.empty[Member].asJava).map(_.getEmail)
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

  def getBucketCredential(userInfo: UserInfo): Credential = {
    if (useServiceAccountForBuckets) getBucketServiceAccountCredential
    else getUserCredential(userInfo)
  }

  def getUserCredential(userInfo: UserInfo): Credential = {
    new GoogleCredential().setAccessToken(userInfo.accessToken.token).setExpiresInSeconds(userInfo.accessTokenExpiresIn)
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

  def deconstructGroupId(groupId: String): WorkspacePermissionsPair = {
    val strippedId = groupId.stripPrefix(s"${groupsPrefix}-").stripSuffix(s"@${appsDomain}").split("_", 2)
    WorkspacePermissionsPair(WorkspaceName(strippedId.head, stripCoreGroupTraits(strippedId(1))._1),
      WorkspaceAccessLevel.fromCanonicalString(stripCoreGroupTraits(strippedId(1))._2.stripPrefix("-").toUpperCase))
  }

  def stripCoreGroupTraits(groupId: String): (String, String) = groupId.splitAt(groupId.lastIndexOf("-"))

  def makeGroupEntityString(groupId: String) = s"group-$groupId"
}
