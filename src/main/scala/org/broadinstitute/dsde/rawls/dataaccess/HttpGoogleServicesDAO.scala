package org.broadinstitute.dsde.rawls.dataaccess

import java.io.{ByteArrayOutputStream, ByteArrayInputStream, StringReader}

import akka.actor.ActorSystem
import com.google.api.client.http.{HttpResponseException, InputStreamContent}
import org.broadinstitute.dsde.rawls.crypto.{EncryptedBytes, Aes256Cbc, SecretKey}
import org.broadinstitute.dsde.rawls.util.FutureSupport
import org.joda.time

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.concurrent.Future
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleCredential, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.admin.directory.model._
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.storage.model.Bucket.{Logging, Lifecycle}
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.{StorageScopes, Storage}
import com.google.api.services.storage.model.{StorageObject, Bucket, BucketAccessControl, ObjectAccessControl}

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._

import spray.http.StatusCodes

class HttpGoogleServicesDAO(
  useServiceAccountForBuckets: Boolean,
  clientSecretsJson: String,
  pemFile: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int,
  serviceProject: String,
  tokenEncryptionKey: String)( implicit val system: ActorSystem, implicit val executionContext: ExecutionContext ) extends GoogleServicesDAO with Retry with FutureSupport {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val clientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(clientSecretsJson))
  val tokenBucketName = "rawls-tokens-" + clientSecrets.getDetails.getClientId.stripSuffix(".apps.googleusercontent.com")
  val tokenSecretKey = SecretKey(tokenEncryptionKey)

  val serviceAccountClientId: String = clientSecrets.getDetails.get("client_email").toString

  initTokenBucket()

  private def initTokenBucket(): Unit = {
    try {
      getStorage(getBucketServiceAccountCredential).buckets().get(tokenBucketName).executeUsingHead()
    } catch {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue =>
        val logBucket = new Bucket().
          setName(tokenBucketName + "-logs")
        val logInserter = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, logBucket)
        logInserter.execute

        // add cloud-storage-analytics@google.com as a writer so it can write logs
        // do it as a separate call so bucket gets default permissions plus this one
        getStorage(getBucketServiceAccountCredential).bucketAccessControls.insert(logBucket.getName, new BucketAccessControl().setEntity("group-cloud-storage-analytics@google.com").setRole("WRITER")).execute()

        val bucketAcls = List(new BucketAccessControl().setEntity("user-" + serviceAccountClientId).setRole("OWNER"))
        val defaultObjectAcls = List(new ObjectAccessControl().setEntity("user-" + serviceAccountClientId).setRole("OWNER"))
        val bucket = new Bucket().
          setName(tokenBucketName).
          setAcl(bucketAcls).
          setDefaultObjectAcl(defaultObjectAcls).
          setLogging(new Logging().setLogBucket(logBucket.getName))
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, bucket)
        inserter.execute
    }
  }

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
          val ownersGroupId = toGroupId(bucketName, WorkspaceAccessLevels.Owner)
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
          // bucket ACLs should be:
          //   workspace owner - bucket writer
          //   workspace writer - bucket writer
          //   workspace reader - bucket reader
          //   bucket service account - bucket owner
          val workspaceAccessToBucketAcl = Map(Owner -> "WRITER", Write -> "WRITER", Read -> "READER")
          val bucketAcls =
            groupAccessLevelsAscending.map(ac => newBucketAccessControl(makeGroupEntityString(toGroupId(bucketName, ac)), workspaceAccessToBucketAcl(ac))) :+
              newBucketAccessControl("user-" + serviceAccountClientId, "OWNER")

          // default object ACLs should be:
          //   workspace owner - object reader
          //   workspace writer - object reader
          //   workspace reader - object reader
          //   bucket service account - object owner
          val defaultObjectAcls = groupAccessLevelsAscending.map(ac => newObjectAccessControl(makeGroupEntityString(toGroupId(bucketName, ac)), "READER")) :+
            newObjectAccessControl("user-" + serviceAccountClientId, "OWNER")

          val bucket = new Bucket().setName(bucketName).setAcl(bucketAcls).setDefaultObjectAcl(defaultObjectAcls)
          val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(projectId, bucket)
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
    new Group().setEmail(toGroupId(bucketName,accessLevel)).setName(UserAuth.toWorkspaceAccessGroupName(workspaceName,accessLevel))

  private def newBucketAccessControl(entity: String, accessLevel: String) =
    new BucketAccessControl().setEntity(entity).setRole(accessLevel)

  private def newObjectAccessControl(entity: String, accessLevel: String) =
    new ObjectAccessControl().setEntity(entity).setRole(accessLevel)

  override def deleteBucket(userInfo: UserInfo, workspaceId: String): Future[Any] = {
    val bucketName = getBucketName(workspaceId)
    def bucketDeleteFn(): Future[Any] = {
      val buckets = getStorage(getBucketCredential(userInfo)).buckets
      val deleter = buckets.delete(bucketName)
      retry(when500)(() => Future {
        blocking { deleter.execute }
      }) recover {
        //Google returns 409 Conflict if the bucket isn't empty.
        case t: HttpResponseException if t.getStatusCode == 409 =>
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
  override def updateACL(currentUser: UserInfo, workspaceId: String, aclUpdates: Map[Either[RawlsUser, RawlsGroup], WorkspaceAccessLevel]): Future[Option[Seq[ErrorReport]]] = {
    val directory = getGroupDirectory

    val futureReports = Future.traverse(aclUpdates){
      case (Left(ru:RawlsUser), level) => updateUserAccess(currentUser,toProxyFromUser(ru),ru.userEmail.value,level,workspaceId,directory)
      case (Right(rg:RawlsGroup), level) => updateUserAccess(currentUser,rg.groupEmail.value,rg.groupEmail.value,level,workspaceId,directory)
    }.map(_.collect{case Some(errorReport)=>errorReport})
    futureReports.map { reports =>
      if (reports.isEmpty) None
      else Some(reports.toSeq)
    }
  }

  override def getMaximumAccessLevel(email: String, workspaceId: String): Future[WorkspaceAccessLevel] = {
    val bucketName = getBucketName(workspaceId)
    val members = getGroupDirectory.members

    Future.traverse(groupAccessLevelsAscending) { accessLevel =>
      retry(when500) { () =>
        Future {
          blocking { members.get(toGroupId(bucketName, accessLevel), email).execute() }
          accessLevel
        }
      } recover {
        case t => WorkspaceAccessLevels.NoAccess
      }
    } map { userAccessLevels =>
      userAccessLevels.sorted[WorkspaceAccessLevel].last
    }
  }

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"

  override def isAdmin(userId: String): Future[Boolean] = {
    val query = getGroupDirectory.members.get(adminGroupName,userId)
    retry(when500)(() => Future {
      blocking { query.execute }
      true
    }) recover {
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => false
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

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = {
    val member = new Member().setEmail(user.userEmail.value).setRole(groupMemberRole)
    val inserter = getGroupDirectory.members.insert(toProxyFromUser(user.userSubjectId), member)
    retry(when500)(() => Future { blocking { inserter.execute } })
  }

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = {
    val deleter = getGroupDirectory.members.delete(toProxyFromUser(user.userSubjectId), user.userEmail.value)
    retry(when500)(() => Future { blocking { deleter.execute } })
  }

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = {
    val getter = getGroupDirectory.members.get(toProxyFromUser(user.userSubjectId), user.userEmail.value)
    retry(when500)(() => Future {
      blocking { Option(getter.execute) }
    } recover {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }) map { _.isDefined }
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

  def createProxyGroup(user: RawlsUser): Future[Unit] = {
    val directory = getGroupDirectory
    val groups = directory.groups
    retry(when500) {
      () => Future {
        val inserter = groups.insert(new Group().setEmail(toProxyFromUser(user.userSubjectId)).setName(user.userEmail.value))
        blocking {
          inserter.execute
        }
      }
    }
  }

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    retryWhen500(() => {
      val so = new StorageObject().setName(userInfo.userSubjectId)
      val encryptedToken = Aes256Cbc.encrypt(refreshToken.getBytes, tokenSecretKey).get
      so.setMetadata(Map("iv" -> encryptedToken.base64Iv))
      val media = new InputStreamContent("text/plain", new ByteArrayInputStream(encryptedToken.base64CipherText.getBytes))
      val inserter = getStorage(getBucketServiceAccountCredential).objects().insert(tokenBucketName, so, media)
      inserter.getMediaHttpUploader().setDirectUploadEnabled(true)
      inserter.execute()
    } )
  }

  override def getToken(userInfo: UserInfo): Future[Option[String]] = {
    retryWhen500(() => {
      val get = getStorage(getBucketServiceAccountCredential).objects().get(tokenBucketName, userInfo.userSubjectId)
      get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
      val tokenBytes = new ByteArrayOutputStream()
      try {
        get.executeMediaAndDownloadTo(tokenBytes)
        val so = get.execute()
        Option(new String(Aes256Cbc.decrypt(EncryptedBytes(tokenBytes.toString, so.getMetadata.get("iv")), tokenSecretKey).get))
      } catch {
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
      }
    } )
  }

  override def getTokenDate(userInfo: UserInfo): Future[Option[time.DateTime]] = {
    retryWhen500(() => {
      val get = getStorage(getBucketServiceAccountCredential).objects().get(tokenBucketName, userInfo.userSubjectId)
      try {
        Option(new time.DateTime(get.execute().getUpdated.getValue))
      } catch {
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
      }
    } )
  }

  override def deleteToken(userInfo: UserInfo): Future[Unit] = {
    retryWhen500(() => {
      getStorage(getBucketServiceAccountCredential).objects().delete(tokenBucketName, userInfo.userSubjectId).execute()
    } )
  }

  // these really should all be private, but we're opening up a few of these to allow integration testing
  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case _ => false
    }
  }

  private def retryWhen500[T]( op: () => T ) = {
    retry(when500)(()=>Future(blocking(op())))
  }

  //expects the email to be of a google group, i.e. that users have been converted to their proxies first.
  //userFacingEmail is the pre-proxified email for display to users in errors.
  private def updateUserAccess(currentUser: UserInfo, groupEmail: String, userFacingEmail: String, targetAccessLevel: WorkspaceAccessLevel, workspaceId: String, directory: Directory): Future[Option[ErrorReport]] = {
    val members = directory.members

    //Ask Google what it thinks this group's current access level is.
    //It might be wrong, but we still need to delete it from that group and then add it to the new level.
    getMaximumAccessLevel(groupEmail, workspaceId) flatMap { currentAccessLevel =>
      if ( currentAccessLevel == targetAccessLevel )
        Future.successful(None)
      else if ( groupEmail.toLowerCase.equals(toProxyFromUser(currentUser).toLowerCase) && currentAccessLevel != targetAccessLevel )
        Future.successful(Option(ErrorReport(s"Failed to change permissions for ${currentUser.userEmail}.  You cannot change your own permissions.",Seq.empty)))
      else {
        val bucketName = getBucketName(workspaceId)
        val member = new Member().setEmail(groupEmail).setRole(groupMemberRole)
        val currentGroupId = toGroupId(bucketName,currentAccessLevel)
        val deleter = members.delete(currentGroupId, groupEmail)
        val targetGroupId = toGroupId(bucketName, targetAccessLevel)
        val inserter = members.insert(targetGroupId, member)

        val deleteFuture: Future[Option[Throwable]] =
          if (currentAccessLevel < WorkspaceAccessLevels.Read)
            Future.successful(None)
          else
            retryWhen500(()=>deleter.execute()).map(_=>None).recover{case throwable=>Some(throwable)}

        val insertFuture: Future[Option[Throwable]] =
          if (targetAccessLevel < WorkspaceAccessLevels.Read)
            Future.successful(None)
          else
            retryWhen500(()=>inserter.execute()).map(_=>None).recover{case throwable=>Some(throwable)}

        val badThing = s"Unable to change permissions for $userFacingEmail from $currentAccessLevel to $targetAccessLevel access."
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
              val restoreDeleter = members.delete(targetGroupId, groupEmail)
              retryWhen500(()=>restoreDeleter.execute()).map{ _ =>
                Some(ErrorReport(badThing, cause))}.recover { case _ =>
                Some(ErrorReport(s"Successfully granted $userFacingEmail $targetAccessLevel access, but failed to remove $currentAccessLevel access. This may result in inconsistent behavior, please try removing the user (may take more than 1 try) and re-adding.", cause))
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
      .setServiceAccountId(serviceAccountClientId)
      .setServiceAccountScopes(directoryScopes)
      .setServiceAccountUser(clientSecrets.getDetails.get("sub_email").toString)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getBucketServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(serviceAccountClientId)
      .setServiceAccountScopes(storageScopes) // grant bucket-creation powers
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def toProxyFromUser(rawlsUser: RawlsUser) = toProxyFromUserSubjectId(rawlsUser.userSubjectId.value)
  def toProxyFromUser(userInfo: UserInfo) = toProxyFromUserSubjectId(userInfo.userSubjectId)
  def toProxyFromUser(subjectId: RawlsUserSubjectId) = toProxyFromUserSubjectId(subjectId.value)
  def toProxyFromUserSubjectId(subjectId: String) = s"PROXY_${subjectId}@${appsDomain}"
  def toUserFromProxy(proxy: String) = getGroupDirectory.groups().get(proxy).execute().getName

  def adminGroupName = s"${groupsPrefix}-ADMINS@${appsDomain}"
  def toGroupId(bucketName: String, accessLevel: WorkspaceAccessLevel) = s"${bucketName}-${accessLevel.toString}@${appsDomain}"
  def fromGroupId(groupId: String): Option[WorkspacePermissionsPair] = {
    val pattern = s"rawls-([0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+-[0-9a-f]+)-([a-zA-Z]+)@${appsDomain}".r
    Try{
      val pattern(workspaceId,accessLevelString) = groupId
      WorkspacePermissionsPair(workspaceId,WorkspaceAccessLevels.withName(accessLevelString.toUpperCase))
    }.toOption
  }
  def makeGroupEntityString(groupId: String) = s"group-$groupId"
}
