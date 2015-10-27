package org.broadinstitute.dsde.rawls.dataaccess

import java.io.StringReader

import akka.actor.{ActorSystem, ActorContext}
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent._
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

import spray.http.StatusCodes

// Seq[String] -> Collection<String>

class HttpGoogleServicesDAO(
  useServiceAccountForBuckets: Boolean,
  clientSecretsJson: String,
  p12File: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int)( implicit val system: ActorSystem, implicit val executionContext: ExecutionContext ) extends GoogleServicesDAO with Retry with FutureSupport {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  val groupAccessLevelsAscending = Seq(WorkspaceAccessLevel.Read, WorkspaceAccessLevel.Write, WorkspaceAccessLevel.Owner)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(clientSecretsJson))

  override def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Future[Unit] = {
    val bucketName = getBucketName(workspaceId)
    val directory = getGroupDirectory
    val groups = directory.groups

    // tries to delete all groups expected to have been created whether they actually were or not
    def rollbackGroups(t: Throwable): Throwable = {
      Future.traverse(groupAccessLevelsAscending) { accessLevel =>
        Future {
          val deleter = groups.delete(newGroup(bucketName, workspaceName, accessLevel).getEmail)
          blocking {
            deleter.execute
          }
        }
      }
      t
    }

    def insertGroups(workspaceName: WorkspaceName, bucketName: String): Future[Seq[Try[Group]]] = {
      Future.traverse(groupAccessLevelsAscending) { accessLevel =>
        toFutureTry(retry(when500) {
          () => Future {
            val inserter = groups.insert(newGroup(bucketName, workspaceName, accessLevel))
            blocking {
              inserter.execute
            }
          }
        })
      }
    }

    def insertOwnerMember: (Seq[Try[Group]]) => Future[Member] = { _ =>
      retry(when500) {
        () => Future {
          val ownersGroupId = toGroupId(bucketName, WorkspaceAccessLevel.Owner)
          val owner = new Member().setEmail(userInfo.userEmail).setRole(groupMemberRole)
          val ownerInserter = directory.members.insert(ownersGroupId, owner)
          blocking {
            ownerInserter.execute
          }
        }
      }
    }

    def insertBucket: (Member) => Future[Unit] = { _ =>
      retry(when500) {
        () => Future {
          val bucketAcls = groupAccessLevelsAscending.map(ac => newBucketAccessControl(makeGroupEntityString(toGroupId(bucketName, ac)), ac))
          val defaultObjectAcls = groupAccessLevelsAscending.map(ac => {
            // NB: writers have read access to objects -- there is no write access, objects are immutable
            newObjectAccessControl(makeGroupEntityString(toGroupId(bucketName, ac)), if (ac == WorkspaceAccessLevel.Owner) WorkspaceAccessLevel.Owner else WorkspaceAccessLevel.Read)
          })

          val bucket = new Bucket().setName(bucketName).setAcl(bucketAcls).setDefaultObjectAcl(defaultObjectAcls)
          val inserter = getStorage(getBucketCredential(userInfo)).buckets.insert(projectId, bucket)
          blocking {
            inserter.execute
          }
        }
      }
    }

    val doItAll = insertGroups(workspaceName, bucketName) flatMap assertSuccessfulTries flatMap insertOwnerMember flatMap insertBucket
    doItAll.transform(f => f, rollbackGroups)
  }

  private def newGroup(bucketName: String, workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) =
    new Group().setEmail(toGroupId(bucketName,accessLevel)).setName(toGroupName(workspaceName,accessLevel))

  private def newBucketAccessControl(entity: String, accessLevel: WorkspaceAccessLevel) =
    new BucketAccessControl().setEntity(entity).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel))

  private def newObjectAccessControl(entity: String, accessLevel: WorkspaceAccessLevel) =
    new ObjectAccessControl().setEntity(entity).setRole(WorkspaceAccessLevel.toCanonicalString(accessLevel))

  override def deleteBucket(userInfo: UserInfo, workspaceId: String): Future[Any] = {
    val bucketName = getBucketName(workspaceId)
    def bucketDeleteFn(): Future[Any] = {
      val buckets = getStorage(getBucketCredential(userInfo)).buckets
      val deleter = buckets.delete(bucketName)
      retry(when500)(() => Future {
        blocking { deleter.execute }
      }) recover {
        //Google returns 409 Conflict if the bucket isn't empty.
        case gjre: GoogleJsonResponseException if gjre.getStatusCode == 409 =>
          //Google doesn't let you delete buckets that are full.
          //You can either remove all the objects manually, or you can set up lifecycle management on the bucket.
          //This can be used to auto-delete all objects next time the Google lifecycle manager runs (~every 24h).
          //More info: http://bit.ly/1WCYhhf and http://bit.ly/1Py6b6O
          val deleteEverythingRule = new Lifecycle.Rule()
            .setAction(new Action().setType("delete"))
            .setCondition(new Condition().setAge(0))
          val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule))
          val fetcher = buckets.get(bucketName)
          val bucketFuture = retry(when500)(() => Future { blocking { fetcher.execute } })
          bucketFuture.map(bucket => bucket.setLifecycle(lifecycle))

          system.scheduler.scheduleOnce(deletedBucketCheckSeconds seconds)(bucketDeleteFn)
        case _ =>
        //TODO: I am entirely unsure what other cases might want to be handled here.
        //404 means the bucket is already gone, which is fine.
      }
    }

    val groups = getGroupDirectory.groups

    def deleteGroups(bucketName: String): Future[Seq[Unit]] = {
      Future.traverse(groupAccessLevelsAscending) { accessLevel =>
        retry[Unit](when500) {
          () => Future {
            blocking {
              groups.delete(toGroupId(bucketName, accessLevel)).execute()
            }
          }
        }
      }
    }

    deleteGroups(bucketName) flatMap { _ => bucketDeleteFn() }
  }

  override def getACL(workspaceId: String): Future[WorkspaceACL] = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    // return the maximum access level for each user
    val aclMap = Future.traverse(groupAccessLevelsAscending) { accessLevel: WorkspaceAccessLevel =>
      val fetcher = members.list(toGroupId(bucketName, accessLevel))
      val membersQuery = retry(when500)(() => Future {
        blocking {
          fetcher.execute
        }
      }) map { membersResult =>
        // NB: if there are no members in this group, Google returns null instead of an empty list. fix that here.
        Option(membersResult.getMembers).getOrElse(List.empty[Member].asJava)
      }

      membersQuery.map(membersResult => membersResult.map(_.getEmail -> accessLevel).toMap)
    }

    aclMap map { acls =>
      WorkspaceACL(acls.reduceRight(_ ++ _)) // note that the reduction must go left-to-right so that higher access levels override lower ones
    }
  }

  /**
   * Try to apply a list of ACL updates
   *
   * @return a map from userId to error message (an empty map means everything was successful)
   */
  override def updateACL(currentUserId: String, workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]): Future[Option[Seq[ErrorReport]]] = {
    val directory = getGroupDirectory

    // make map to eliminate redundant instructions for a single user (last one in the sequence for a given user wins)
    val updateMap = aclUpdates.map{ workspaceACLUpdate => workspaceACLUpdate.userId -> workspaceACLUpdate.accessLevel }.toMap
    val futureReports = Future.traverse(updateMap){ case (userId,accessLevel) =>
        updateUserAccess(currentUserId,userId,accessLevel,workspaceId,directory)
    }.map(_.collect{case Some(errorReport)=>errorReport})
    futureReports.map { reports =>
      if (reports.isEmpty) None
      else Some(reports.toSeq)
    }
  }

  override def getOwners(workspaceId: String): Future[Seq[String]] = {
    val members = getGroupDirectory.members
    //a workspace should always have an owner, but just in case for some reason it doesn't...
    val fetcher = members.list(toGroupId(getBucketName(workspaceId), WorkspaceAccessLevel.Owner))
    val ownersQuery = retry(when500) (() => Future {
      blocking { fetcher.execute }
    })

    ownersQuery map { queryResults =>
      Option(queryResults.getMembers).getOrElse(List.empty[Member].asJava).map(_.getEmail)
    }
  }

  override def getMaximumAccessLevel(userId: String, workspaceId: String): Future[WorkspaceAccessLevel] = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    Future.traverse(groupAccessLevelsAscending) { accessLevel =>
      retry(when500) { () =>
        Future {
          blocking { members.get(toGroupId(bucketName, accessLevel), userId).execute() }
          accessLevel
        }
      } recover {
        case t => WorkspaceAccessLevel.NoAccess
      }
    } map { userAccessLevels =>
      userAccessLevels.sorted.last
    }
  }

  override def getWorkspaces(userId: String): Future[Seq[WorkspacePermissionsPair]] = {
    val directory = getGroupDirectory
    val fetcher = directory.groups().list().setUserKey(userId)
    val groupsQuery = retry(when500)(() => Future {
      blocking { fetcher.execute }
    })

    groupsQuery map { groupsResult =>
      val workspaceGroups = Option(groupsResult.getGroups).getOrElse(List.empty[Group].asJava)
      workspaceGroups.flatMap( group => fromGroupId(group.getEmail) ).toSeq
    }
  }

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"

  override def isAdmin(userId: String): Future[Boolean] = {
    val query = getGroupDirectory.members.get(adminGroupName,userId)
    retry(when500)(() => Future {
      blocking { query.execute }
      true
    }) recover {
        case gjre: GoogleJsonResponseException if gjre.getStatusCode == StatusCodes.NotFound => false
    }
  }

  override def addAdmin(userId: String): Future[Unit] = {
    val member = new Member().setEmail(userId).setRole(groupMemberRole)
    val inserter = getGroupDirectory.members.insert(adminGroupName,member)
    retry(when500)(() => Future { blocking { inserter.execute } })
  }

  override def deleteAdmin(userId: String): Future[Unit] = {
    val deleter = getGroupDirectory.members.delete(adminGroupName,userId)
    retry(when500)(() => Future { blocking { deleter.execute } })
  }

  override def listAdmins(): Future[Seq[String]] = {
    val fetcher = getGroupDirectory.members.list(adminGroupName)
    retry(when500)(() => Future { blocking { fetcher.execute.getMembers } }) map { result =>
      Option(result) match {
        case None => Seq.empty
        case Some(list) => list.map(_.getEmail)
      }
    }
  }

  def createProxyGroup(userInfo: UserInfo): Future[Unit] = {
    val directory = getGroupDirectory
    val groups = directory.groups
    retry(when500) {
      () => Future {
        val inserter = groups.insert(new Group().setEmail(toProxyFromUser(userInfo)).setName(userInfo.userEmail))
        blocking {
          inserter.execute
        }
      }
    }
  }

  // these really should all be private, but we're opening up a few of these to allow integration testing
  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case gjre: GoogleJsonResponseException => gjre.getStatusCode/100 == 5
      case _ => false
    }
  }

  private def retryWhen500[T]( op: () => T ) = {
    retry(when500)(()=>Future(blocking(op())))
  }

  private def updateUserAccess(currentUserId: String, userId: String, targetAccessLevel: WorkspaceAccessLevel, workspaceId: String, directory: Directory): Future[Option[ErrorReport]] = {
    val members = directory.members
    getMaximumAccessLevel(userId, workspaceId) flatMap { currentAccessLevel =>
      if ( currentAccessLevel == targetAccessLevel )
        Future.successful(None)
      else if ( userId.toLowerCase.equals(currentUserId.toLowerCase) && currentAccessLevel != targetAccessLevel )
        Future.successful(Option(ErrorReport(s"Failed to change permissions for $userId.  You cannot change your own permissions.",Seq.empty)))
      else {
        val bucketName = getBucketName(workspaceId)
        val member = new Member().setEmail(userId).setRole(groupMemberRole)
        val currentGroupId = toGroupId(bucketName,currentAccessLevel)
        val deleter = members.delete(currentGroupId, userId)
        val targetGroupId = toGroupId(bucketName, targetAccessLevel)
        val inserter = members.insert(targetGroupId, member)

        val deleteFuture: Future[Option[Throwable]] =
          if (currentAccessLevel < WorkspaceAccessLevel.Read)
            Future.successful(None)
          else
            retryWhen500(()=>deleter.execute()).map(_=>None).recover{case throwable=>Some(throwable)}

        val insertFuture: Future[Option[Throwable]] =
          if (targetAccessLevel < WorkspaceAccessLevel.Read)
            Future.successful(None)
          else
            retryWhen500(()=>inserter.execute()).map(_=>None).recover{case throwable=>Some(throwable)}

        val badThing = s"Unable to change permissions for $userId from $currentAccessLevel to $targetAccessLevel access."
        deleteFuture zip insertFuture flatMap { _ match {
            case (None, None) => Future.successful(None) // delete and insert succeeded
            case (None, Some(throwable)) => // delete ok, but insert failed: try to restore access
              val cause = toErrorReport(throwable)
              val restoreInserter = members.insert(currentGroupId, member)
              retryWhen500(()=>restoreInserter.execute()).map{ _ =>
                Some(ErrorReport(badThing, cause))}.recover{ case _ =>
                Some(ErrorReport(s"$badThing They no longer have any access to the workspace. Please try re-adding them.", cause))
              }
            case (Some(throwable), None) => // delete failed, but insert succeeded -- user is now on two lists
              val cause = toErrorReport(throwable)
              val restoreDeleter = members.delete(targetGroupId, userId)
              retryWhen500(()=>restoreDeleter.execute()).map{ _ =>
                Some(ErrorReport(badThing, cause))}.recover { case _ =>
                Some(ErrorReport(s"Successfully granted $userId $targetAccessLevel access, but failed to remove $currentAccessLevel access. This may result in inconsistent behavior, please try removing the user (may take more than 1 try) and re-adding.", cause))
              }
            case (Some(throwable1), Some(throwable2)) => // both operations failed
              Future.successful(Some(ErrorReport(badThing,Seq(toErrorReport(throwable1),toErrorReport(throwable2)))))
          }
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

  def toProxyFromUser(userInfo: UserInfo) = s"PROXY_${userInfo.userSubjectId}@${appsDomain}"
  def toUserFromProxy(proxy: String) = getGroupDirectory.groups().get(proxy).execute().getName

  def adminGroupName = s"${groupsPrefix}-ADMIN@${appsDomain}"
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
