package org.broadinstitute.dsde.rawls.dataaccess

import java.io.StringReader

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Try}

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleCredential, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.admin.directory.Directory._
import com.google.api.services.admin.directory.model._
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.{StorageScopes, Storage}
import com.google.api.services.storage.model.{Bucket, BucketAccessControl}
import com.google.api.client.googleapis.json.GoogleJsonResponseException

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{WorkspaceACLUpdate,WorkspaceAccessLevel}
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

  override def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String): Unit = {
    getStorage(getBucketCredential(userInfo)).buckets.insert(projectId, new Bucket().setName(getBucketName(workspaceId))).execute
  }

  override def deleteBucket(userInfo: UserInfo, workspaceId: String): Unit = {
    import system.dispatcher
    val bucketName = getBucketName(workspaceId)
    def bucketDeleteFn(): Unit = {
      val buckets = getStorage(getBucketCredential(userInfo)).buckets

      Try(buckets.delete(bucketName).execute) match {
        //Google returns 409 Conflict if the bucket isn't empty.
        case Failure(gjre: GoogleJsonResponseException) if gjre.getDetails.getCode == 409 =>
          //Google doesn't let you delete buckets that are full.
          //You can either remove all the objects manually, or you can set up lifecycle management on the bucket.
          //This can be used to auto-delete all objects next time the Google lifecycle manager runs (~every 24h).
          //More info: http://bit.ly/1WCYhhf and http://bit.ly/1Py6b6O
          val deleteEverythingRule = new Lifecycle.Rule()
            .setAction(new Action().setType("delete"))
            .setCondition(new Condition().setAge(0))
          val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule))
          val bucket = buckets.get(bucketName).execute()
          bucket.setLifecycle(lifecycle)

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

  override def createACLGroups(userInfo: UserInfo, workspaceId: String, workspaceName: WorkspaceName): Unit = {
    val bucketName = getBucketName(workspaceId)
    val groupDirectory = getGroupDirectory
    val groups = groupDirectory.groups
    val bucketAccessControls = getStorage(getBucketCredential(userInfo)).bucketAccessControls

    // ACLs are stored internally in three Google groups.
    groupAccessLevelsAscending foreach { accessLevel =>
      // create the group
      val groupId = toGroupId(bucketName, accessLevel)
      val group = new Group().setEmail(groupId).setName(toGroupName(workspaceName,accessLevel))
      groups.insert(group).execute()

      // add group to ACL (using bucket owner's credential)
      val bucketAcl = new BucketAccessControl().setEntity(makeGroupEntityString(groupId)).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel)).setBucket(bucketName)
      bucketAccessControls.insert(bucketName, bucketAcl).execute()
    }

    // add the owner -- may take up to 1 minute after groups are created
    // (see https://developers.google.com/admin-sdk/directory/v1/guides/manage-groups)
    val targetMember = new Member().setEmail(userInfo.userEmail).setRole(groupMemberRole)
    groupDirectory.members.insert(toGroupId(bucketName, WorkspaceAccessLevel.Owner), targetMember).execute()
  }

  override def deleteACLGroups(workspaceId: String): Unit = {
    val groups = getGroupDirectory.groups
    groupAccessLevelsAscending foreach { accessLevel =>
      val groupId = toGroupId(getBucketName(workspaceId), accessLevel)
      groups.delete(groupId).execute()
    }
  }

  override def getACL(workspaceId: String): WorkspaceACL = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    // return the maximum access level for each user
    val aclMap = groupAccessLevelsAscending map { accessLevel: WorkspaceAccessLevel =>
      val membersQuery = members.list(toGroupId(bucketName, accessLevel)).execute()
      // NB: if there are no members in this group, Google returns null instead of an empty list. fix that here.
      val membersList = Option(membersQuery.getMembers).getOrElse(List.empty[Member].asJava)
      membersList.map(_.getEmail -> accessLevel).toMap[String, WorkspaceAccessLevel]
    } reduceRight(_ ++ _) // note that the reduction must go left-to-right so that higher access levels override lower ones

    WorkspaceACL(aclMap)
  }

  /**
   * Try to apply a list of ACL updates
   *
   * @return a map from userId to error message (an empty map means everything was successful)
   */
  override def updateACL(workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]): Map[String, String] = {
    val directory = getGroupDirectory

    // make map to eliminate redundant instructions for a single user (last one in the sequence for a given user wins)
    val updateMap = aclUpdates.map{ workspaceACLUpdate => workspaceACLUpdate.userId -> workspaceACLUpdate.accessLevel }.toMap
    updateMap.flatMap{ case (userId,accessLevel) => updateUserAccess(userId,accessLevel,workspaceId,directory) }
  }

  override def getOwners(workspaceId: String): Seq[String] = {
    val members = getGroupDirectory.members
    //a workspace should always have an owner, but just in case for some reason it doesn't...
    val ownersQuery = members.list(toGroupId(getBucketName(workspaceId), WorkspaceAccessLevel.Owner)).execute()

    Option(ownersQuery.getMembers).getOrElse(List.empty[Member].asJava).map(_.getEmail)
  }

  override def getMaximumAccessLevel(userId: String, workspaceId: String): WorkspaceAccessLevel = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    @tailrec
    def testLevel( levels: Seq[WorkspaceAccessLevel] ): WorkspaceAccessLevel = {
      if ( levels.isEmpty ) WorkspaceAccessLevel.NoAccess
      else if ( Try(members.get(toGroupId(bucketName,levels.head),userId).execute()).isSuccess ) levels.head
      else testLevel( levels.tail )
    }

    testLevel(groupAccessLevelsAscending.reverse)
  }

  override def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair] = {
    val directory = getGroupDirectory
    val groupsQuery = directory.groups().list().setUserKey(userId).execute()

    val workspaceGroups = Option(groupsQuery.getGroups).getOrElse(List.empty[Group].asJava)

    workspaceGroups.flatMap( group => fromGroupId(group.getId) ).toSeq
  }

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"

  // these really should all be private, but we're opening up a few of these to allow integration testing

  private def updateUserAccess(userId: String, targetAccessLevel: WorkspaceAccessLevel, workspaceId: String, directory: Directory): Option[(String,String)] = {
    val members = directory.members
    val currentAccessLevel = getMaximumAccessLevel(userId, workspaceId)
    if ( currentAccessLevel == targetAccessLevel )
      None
    else {
      val bucketName = getBucketName(workspaceId)
      val member = new Member().setEmail(userId).setRole(groupMemberRole)
      if ( currentAccessLevel >= WorkspaceAccessLevel.Read && Try(members.delete(toGroupId(bucketName,currentAccessLevel), userId).execute()).isFailure )
        Option((userId,s"Failed to change permissions for $userId."))
      else if ( targetAccessLevel >= WorkspaceAccessLevel.Read && Try(members.insert(toGroupId(bucketName,targetAccessLevel), member).execute()).isFailure )
        Option((userId,s"Failed to change permissions for $userId. They have been dropped from the workspace. Please try re-adding them."))
      else
        None
    }
  }

  def getStorage(credential: Credential) = {
    new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getGroupDirectory = {
    new Directory.Builder(httpTransport, jsonFactory, getGroupServiceAccountCredential).setApplicationName(appName).build()
  }

  private def getBucketCredential(userInfo: UserInfo): Credential = {
    if (useServiceAccountForBuckets) getBucketServiceAccountCredential
    else getUserCredential(userInfo)
  }

  private def getUserCredential(userInfo: UserInfo): Credential = {
    new GoogleCredential().setAccessToken(userInfo.accessToken.token).setExpiresInSeconds(userInfo.accessTokenExpiresIn)
  }

  private def getGroupServiceAccountCredential: Credential = {
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

  def toGroupId(bucketName: String, accessLevel: WorkspaceAccessLevel) = s"${bucketName}-${WorkspaceAccessLevel.toCanonicalString(accessLevel)}@${appsDomain}"
  def fromGroupId(groupId: String): Option[WorkspacePermissionsPair] = {
    val pattern = "rawls-([0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+)-([A-Z]+)".r
    Try{
      val pattern(workspaceId,accessLevelString) = groupId
      WorkspacePermissionsPair(workspaceId,WorkspaceAccessLevel.fromCanonicalString(accessLevelString))
    }.toOption
  }
  private def toGroupName(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) = s"rawls ${workspaceName.namespace}/${workspaceName.name} ${accessLevel}"
  def makeGroupEntityString(groupId: String) = s"group-$groupId"
}
