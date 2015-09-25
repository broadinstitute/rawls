package org.broadinstitute.dsde.rawls.dataaccess

import java.io.StringReader

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleCredential, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.admin.directory.model._
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.{StorageScopes, Storage}
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, ObjectAccessControl}
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
  deletedBucketCheckSeconds: Int) extends GoogleCloudStorageDAO with Retry {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  val groupAccessLevelsAscending = Seq(WorkspaceAccessLevel.Read, WorkspaceAccessLevel.Write, WorkspaceAccessLevel.Owner)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(clientSecretsJson))

  val system = akka.actor.ActorSystem("system")

  override def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Unit = {
    val bucketName = getBucketName(workspaceId)
    val directory = getGroupDirectory
    val groups = directory.groups
    insertGroup(groups,newGroup(bucketName,workspaceName,WorkspaceAccessLevel.Read)) {
      insertGroup(groups,newGroup(bucketName,workspaceName,WorkspaceAccessLevel.Write)) {
        insertGroup(groups,newGroup(bucketName,workspaceName,WorkspaceAccessLevel.Owner)) {
          val ownersGroupId = toGroupId(bucketName,WorkspaceAccessLevel.Owner)
          val owner = new Member().setEmail(userInfo.userEmail).setRole(groupMemberRole)
          val ownerInserter = directory.members.insert(ownersGroupId,owner)
          retry(ownerInserter.execute,when500)

          val readersEntity = makeGroupEntityString(toGroupId(bucketName,WorkspaceAccessLevel.Read))
          val writersEntity = makeGroupEntityString(toGroupId(bucketName,WorkspaceAccessLevel.Write))
          val ownersEntity = makeGroupEntityString(ownersGroupId)
          val bucket = new Bucket().setName(bucketName).setAcl(List(
              newBucketAccessControl(readersEntity,WorkspaceAccessLevel.Read),
              newBucketAccessControl(writersEntity,WorkspaceAccessLevel.Write),
              newBucketAccessControl(ownersEntity,WorkspaceAccessLevel.Owner)
            )).setDefaultObjectAcl(List(
              newObjectAccessControl(readersEntity,WorkspaceAccessLevel.Read),
              // NB: writers have read access to objects -- there is no write access, objects are immutable
              newObjectAccessControl(writersEntity,WorkspaceAccessLevel.Read/*sic!*/),
              newObjectAccessControl(ownersEntity,WorkspaceAccessLevel.Owner)
            ))
          val inserter = getStorage(getBucketCredential(userInfo)).buckets.insert(projectId,bucket)
          retry(inserter.execute,when500)
        }
      }
    }
  }

  private def newGroup(bucketName: String, workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) =
    new Group().setEmail(toGroupId(bucketName,accessLevel)).setName(toGroupName(workspaceName,accessLevel))

  private def newBucketAccessControl(entity: String, accessLevel: WorkspaceAccessLevel) =
    new BucketAccessControl().setEntity(entity).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel))

  private def newObjectAccessControl(entity: String, accessLevel: WorkspaceAccessLevel) =
    new ObjectAccessControl().setEntity(entity).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel))

  private def insertGroup(groups: Directory#Groups, group: Group)(op: => Unit) = {
    val inserter = groups.insert(group)
    tryRetry(inserter.execute,when500) match {
      case Success(_) =>
      case Failure(exc) => throw new RawlsException(s"Failed to create group ${group.getEmail}",exc)
    }
    Try(op) match {
      case Success(_) =>
      case Failure(exception) =>
        val deleter = groups.delete(group.getEmail)
        tryRetry(deleter.execute,when500) match {
          case Success(_) => throw exception
          case Failure(_) => throw new RawlsException("Additionally, failed to delete group ${group.getEmail} when cleaning up.", exception)
        }
    }
  }

  override def deleteBucket(userInfo: UserInfo, workspaceId: String): Unit = {
    import system.dispatcher
    val bucketName = getBucketName(workspaceId)
    def bucketDeleteFn(): Unit = {
      val buckets = getStorage(getBucketCredential(userInfo)).buckets
      val deleter = buckets.delete(bucketName)
      tryRetry(deleter.execute,when500) match {
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
          val fetcher = buckets.get(bucketName)
          val bucket = retry(fetcher.execute,when500)
          bucket.setLifecycle(lifecycle)

          system.scheduler.scheduleOnce(deletedBucketCheckSeconds seconds)(bucketDeleteFn)
        case _ =>
        //TODO: I am entirely unsure what other cases might want to be handled here.
        //404 means the bucket is already gone, which is fine.
        //Success is also fine, clearly.
      }
    }

    val groups = getGroupDirectory.groups
    deleteGroup(groups,toGroupId(bucketName,WorkspaceAccessLevel.Read)) {
      deleteGroup(groups,toGroupId(bucketName,WorkspaceAccessLevel.Write)) {
        deleteGroup(groups,toGroupId(bucketName,WorkspaceAccessLevel.Owner)) {
          // attempt to delete the group, scheduling future attempts if this one fails due to a 409
          Try(bucketDeleteFn()) match {
            case Success(_) =>
            case Failure(exc) => throw new RawlsException("Failed to delete bucket ${bucketName}.", exc)
          }
        }
      }
    }
  }

  private def deleteGroup( groups: Directory#Groups, groupId: String)( op: => Unit ) = {
    val deleter = groups.delete(groupId)
    tryRetry(deleter.execute,when500) match {
      case Success(_) => op
      case Failure(exception) =>
        val thisException = new RawlsException(s"Failed to delete group ${groupId}.", exception)
        Try(op) match {
          case Success(_) => throw thisException
          case Failure(thatException) => throw new RawlsException(""+thisException.getMessage+"\nand\n"+thatException.getMessage,exception)
        }
    }
  }

  override def getACL(workspaceId: String): WorkspaceACL = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    // return the maximum access level for each user
    val aclMap = groupAccessLevelsAscending map { accessLevel: WorkspaceAccessLevel =>
      val fetcher = members.list(toGroupId(bucketName, accessLevel))
      val membersQuery = retry(fetcher.execute,when500)
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
    val fetcher = members.list(toGroupId(getBucketName(workspaceId), WorkspaceAccessLevel.Owner))
    val ownersQuery = retry(fetcher.execute,when500)

    Option(ownersQuery.getMembers).getOrElse(List.empty[Member].asJava).map(_.getEmail)
  }

  override def getMaximumAccessLevel(userId: String, workspaceId: String): WorkspaceAccessLevel = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    @tailrec
    def testLevel( levels: Seq[WorkspaceAccessLevel] ): WorkspaceAccessLevel = {
      if ( levels.isEmpty ) WorkspaceAccessLevel.NoAccess
      else {
        val fetcher = members.get(toGroupId(bucketName,levels.head),userId)
        if ( tryRetry(fetcher.execute,when500).isSuccess ) levels.head
        else testLevel( levels.tail )
      }
    }

    testLevel(groupAccessLevelsAscending.reverse)
  }

  override def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair] = {
    val directory = getGroupDirectory
    val fetcher = directory.groups().list().setUserKey(userId)
    val groupsQuery = retry(fetcher.execute,when500)

    val workspaceGroups = Option(groupsQuery.getGroups).getOrElse(List.empty[Group].asJava)

    workspaceGroups.flatMap( group => fromGroupId(group.getEmail) ).toSeq
  }

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"

  // these really should all be private, but we're opening up a few of these to allow integration testing
  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case gjre: GoogleJsonResponseException => gjre.getDetails.getCode/100 == 5
      case _ => false
    }
  }

  private def updateUserAccess(userId: String, targetAccessLevel: WorkspaceAccessLevel, workspaceId: String, directory: Directory): Option[(String,String)] = {
    val members = directory.members
    val currentAccessLevel = getMaximumAccessLevel(userId, workspaceId)
    if ( currentAccessLevel == targetAccessLevel )
      None
    else {
      val bucketName = getBucketName(workspaceId)
      val member = new Member().setEmail(userId).setRole(groupMemberRole)
      val currentGroupId = toGroupId(bucketName,currentAccessLevel)
      val deleter = members.delete(currentGroupId, userId)
      val inserter = members.insert(toGroupId(bucketName,targetAccessLevel), member)
      val simpleFailure = Option((userId,s"Failed to change permissions for $userId."))
      if ( currentAccessLevel >= WorkspaceAccessLevel.Read && tryRetry(deleter.execute,when500).isFailure )
        simpleFailure
      else if ( targetAccessLevel < WorkspaceAccessLevel.Read || tryRetry(inserter.execute,when500).isSuccess )
        None
      else if ( currentAccessLevel < WorkspaceAccessLevel.Read )
        simpleFailure
      else
      {
        val restoreInserter = members.insert(currentGroupId, member)
        tryRetry(restoreInserter.execute,when500) match {
          case Success(_) => simpleFailure
          case Failure(_) => Option((userId,s"Failed to change permissions for $userId. They no longer have any access to the workspace. Please try re-adding them."))
        }
      }
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
    val pattern = s"rawls-([0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+)-([a-zA-Z]+)@${appsDomain}".r
    Try{
      val pattern(workspaceId,accessLevelString) = groupId
      WorkspacePermissionsPair(workspaceId,WorkspaceAccessLevel.fromCanonicalString(accessLevelString.toUpperCase))
    }.toOption
  }
  private def toGroupName(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) = s"rawls ${workspaceName.namespace}/${workspaceName.name} ${accessLevel}"
  def makeGroupEntityString(groupId: String) = s"group-$groupId"
}
