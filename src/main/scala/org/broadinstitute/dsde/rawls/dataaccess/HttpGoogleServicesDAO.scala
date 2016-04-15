package org.broadinstitute.dsde.rawls.dataaccess

import java.io.{InputStream, ByteArrayOutputStream, ByteArrayInputStream, StringReader}
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import akka.actor.{ActorRef, ActorSystem}
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.json.JsonHttpContent
import com.google.api.client.http.{EmptyContent, HttpResponseException, InputStreamContent}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.crypto.{EncryptedBytes, Aes256Cbc, SecretKey}
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.{BucketDeleted, DeleteBucket}
import org.broadinstitute.dsde.rawls.util.FutureSupport
import org.joda.time
import spray.json.JsValue

import scala.collection.JavaConversions._

import scala.concurrent.Future
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import com.google.api.client.auth.oauth2.{TokenResponse, Credential}
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
  val clientSecrets: GoogleClientSecrets,
  pemFile: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int,
  serviceProject: String,
  tokenEncryptionKey: String,
  tokenClientSecretsJson: String)( implicit val system: ActorSystem, implicit val executionContext: ExecutionContext ) extends GoogleServicesDAO(groupsPrefix) with Retry with FutureSupport with LazyLogging {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val tokenClientSecrets: GoogleClientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(tokenClientSecretsJson))
  val tokenBucketName = "tokens-" + clientSecrets.getDetails.getClientId.stripSuffix(".apps.googleusercontent.com")
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
        executeGoogleRequest(logInserter)

        // add cloud-storage-analytics@google.com as a writer so it can write logs
        // do it as a separate call so bucket gets default permissions plus this one
        executeGoogleRequest(getStorage(getBucketServiceAccountCredential).bucketAccessControls.insert(logBucket.getName, new BucketAccessControl().setEntity("group-cloud-storage-analytics@google.com").setRole("WRITER")))

        val bucketAcls = List(new BucketAccessControl().setEntity("user-" + serviceAccountClientId).setRole("OWNER"))
        val defaultObjectAcls = List(new ObjectAccessControl().setEntity("user-" + serviceAccountClientId).setRole("OWNER"))
        val bucket = new Bucket().
          setName(tokenBucketName).
          setAcl(bucketAcls).
          setDefaultObjectAcl(defaultObjectAcls).
          setLogging(new Logging().setLogBucket(logBucket.getName))
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, bucket)
        executeGoogleRequest(inserter)
    }
  }

  private def getBucketName(workspaceId: String) = s"${groupsPrefix}-${workspaceId}"

  override def setupWorkspace(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName, realm: Option[RawlsGroupRef]): Future[GoogleWorkspaceInfo] = {
    val bucketName = getBucketName(workspaceId)

    val accessGroupRefsByLevel: Map[WorkspaceAccessLevel, RawlsGroupRef] = groupAccessLevelsAscending.map { accessLevel =>
      (accessLevel, RawlsGroupRef(RawlsGroupName(workspaceAccessGroupName(workspaceId, accessLevel))))
    }.toMap

    val intersectionGroupRefsByLevel: Option[Map[WorkspaceAccessLevel, RawlsGroupRef]] = realm map { realmGroupRef =>
      groupAccessLevelsAscending.map { accessLevel =>
        (accessLevel, RawlsGroupRef(RawlsGroupName(intersectionGroupName(workspaceId, realmGroupRef, accessLevel))))
      }.toMap
    }

    def rollbackGroups(groupInsertTries: Iterable[Try[RawlsGroup]]) = {
      Future.traverse(groupInsertTries) {
        case Success(group) => deleteGoogleGroup(group)
        case _ => Future.successful(Unit)
      }
    }

    def insertGroups(groupRefsByAccess: Map[WorkspaceAccessLevel, RawlsGroupRef]): Future[Map[WorkspaceAccessLevel, Try[RawlsGroup]]] = {
      Future.traverse(groupRefsByAccess) { case (access, groupRef) => toFutureTry(createGoogleGroup(groupRef)).map(access -> _) }.map(_.toMap)
    }

    def insertOwnerMember: (Map[WorkspaceAccessLevel, RawlsGroup]) => Future[Map[WorkspaceAccessLevel, RawlsGroup]] = { groupsByAccess =>
      val ownerGroup = groupsByAccess(WorkspaceAccessLevels.Owner)
      addMemberToGoogleGroup(ownerGroup, Left(RawlsUser(userInfo))).map(_ => groupsByAccess + (WorkspaceAccessLevels.Owner -> ownerGroup.copy(users = ownerGroup.users + RawlsUser(userInfo))))
    }

    def insertBucket: (Map[WorkspaceAccessLevel, RawlsGroup], Option[Map[WorkspaceAccessLevel, RawlsGroup]]) => Future[GoogleWorkspaceInfo] = { (accessGroupsByLevel, intersectionGroupsByLevel) =>
      retryExponentially(when500orGoogleError) {
        () => Future {

          // When Intersection Groups exist, these are the groups used to determine ACLs.  Otherwise, Access Groups are used directly.

          val groupsByAccess = intersectionGroupsByLevel getOrElse accessGroupsByLevel

          // bucket ACLs should be:
          //   workspace owner - bucket writer
          //   workspace writer - bucket writer
          //   workspace reader - bucket reader
          //   bucket service account - bucket owner
          val workspaceAccessToBucketAcl: Map[WorkspaceAccessLevel, String] = Map(Owner -> "WRITER", Write -> "WRITER", Read -> "READER")
          val bucketAcls =
            groupsByAccess.map { case (access, group) => newBucketAccessControl(makeGroupEntityString(group.groupEmail.value), workspaceAccessToBucketAcl(access)) }.toSeq :+
              newBucketAccessControl("user-" + serviceAccountClientId, "OWNER")

          // default object ACLs should be:
          //   workspace owner - object reader
          //   workspace writer - object reader
          //   workspace reader - object reader
          //   bucket service account - object owner
          val defaultObjectAcls =
            groupsByAccess.map { case (access, group) => newObjectAccessControl(makeGroupEntityString(group.groupEmail.value), "READER") }.toSeq :+
              newObjectAccessControl("user-" + serviceAccountClientId, "OWNER")

          val bucket = new Bucket().setName(bucketName).setAcl(bucketAcls).setDefaultObjectAcl(defaultObjectAcls)
          val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(projectId, bucket)
          blocking {
            executeGoogleRequest(inserter)
          }

          GoogleWorkspaceInfo(bucketName, accessGroupsByLevel, intersectionGroupsByLevel)
        }
      }
    }

    // setupWorkspace main logic

    val accessGroupInserts = insertGroups(accessGroupRefsByLevel)

    val intersectionGroupInserts = intersectionGroupRefsByLevel match {
      case Some(g) => insertGroups(g) map { Option(_) }
      case None => Future.successful(None)
    }

    val bucketInsertion = for {
      accessGroupTries <- accessGroupInserts
      accessGroups <- assertSuccessfulTries(accessGroupTries) flatMap insertOwnerMember
      intersectionGroupTries <- intersectionGroupInserts
      intersectionGroups <- intersectionGroupTries match {
        case Some(t) => assertSuccessfulTries(t) flatMap insertOwnerMember map { Option(_) }
        case None => Future.successful(None)
      }
      inserted <- insertBucket(accessGroups, intersectionGroups)
    } yield inserted

    bucketInsertion recoverWith {
      case regrets =>
        val groupsToRollback = for {
          accessGroupTries <- accessGroupInserts
          intersectionGroupTries <- intersectionGroupInserts
        } yield {
          intersectionGroupTries match {
            case Some(tries) => accessGroupTries.values ++ tries.values
            case None => accessGroupTries.values
          }
        }
        groupsToRollback flatMap rollbackGroups flatMap (_ => Future.failed(regrets))
    }
  }

  def workspaceAccessGroupName(workspaceId: String, accessLevel: WorkspaceAccessLevel) = s"${workspaceId}-${accessLevel.toString}"

  def intersectionGroupName(workspaceId: String, realmGroupRef: RawlsGroupRef, accessLevel: WorkspaceAccessLevel) = {
    val realm = realmGroupRef.groupName.value
    s"I_${workspaceId}-${accessLevel.toString}"
  }

  def createCromwellAuthBucket(billingProject: RawlsBillingProjectName): Future[String] = {
    val bucketName = getCromwellAuthBucketName(billingProject)
    retry(when500) {
      () => Future {
        val bucket = new Bucket().setName(bucketName)
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(billingProject.value, bucket)
        blocking {
          executeGoogleRequest(inserter)
        }

        bucketName
      }
    } recover {
      case t: HttpResponseException if t.getStatusCode == 409 => bucketName
    }
  }

  private def newBucketAccessControl(entity: String, accessLevel: String) =
    new BucketAccessControl().setEntity(entity).setRole(accessLevel)

  private def newObjectAccessControl(entity: String, accessLevel: String) =
    new ObjectAccessControl().setEntity(entity).setRole(accessLevel)

  override def deleteBucket(bucketName: String, monitorRef: ActorRef): Future[Any] = {
    val buckets = getStorage(getBucketServiceAccountCredential).buckets
    val deleter = buckets.delete(bucketName)
    retry(when500)(() => Future {
      blocking { executeGoogleRequest(deleter) }
      monitorRef ! BucketDeleted(bucketName)
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
        val bucketFuture = retry(when500)(() => Future { blocking { executeGoogleRequest(fetcher) } })
        bucketFuture.map(bucket => bucket.setLifecycle(lifecycle))

        system.scheduler.scheduleOnce(deletedBucketCheckSeconds seconds, monitorRef, DeleteBucket(bucketName))
      // Bucket is already deleted
      case t: HttpResponseException if t.getStatusCode == 404 =>
        monitorRef ! BucketDeleted(bucketName)
      case _ =>
      //TODO: I am entirely unsure what other cases might want to be handled here.
    }
  }

  override def isAdmin(userId: String): Future[Boolean] = {
    val query = getGroupDirectory.members.get(adminGroupName,userId)
    retry(when500)(() => Future {
      blocking { executeGoogleRequest(query) }
      true
    }) recover {
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => false
    }
  }

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = {
    val member = new Member().setEmail(user.userEmail.value).setRole(groupMemberRole)
    val inserter = getGroupDirectory.members.insert(toProxyFromUser(user.userSubjectId), member)
    retry(when500)(() => Future { blocking { executeGoogleRequest(inserter) } })
  }

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = {
    val deleter = getGroupDirectory.members.delete(toProxyFromUser(user.userSubjectId), user.userEmail.value)
    retry(when500)(() => Future { blocking { executeGoogleRequest(deleter) } })
  }

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = {
    isEmailInGoogleGroup(user.userEmail.value, toProxyFromUser(user.userSubjectId))
  }

  override def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean] = {
    val getter = getGroupDirectory.members.get(groupName, email)
    retry(when500)(() => Future {
      blocking { Option(executeGoogleRequest(getter)) }
    } recover {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }) map { _.isDefined }
  }

  override def getGoogleGroup(groupName: String): Future[Option[Group]] = {
    val getter = getGroupDirectory.groups().get(groupName)
    retry(when500)(() => Future {
      blocking { Option(executeGoogleRequest(getter)) }
    } recover {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    })
  }

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = {
    val aclGetter = getStorage(getBucketServiceAccountCredential).bucketAccessControls().list(bucketName)
    retry(when500)(() => Future {
      blocking { Option(executeGoogleRequest(aclGetter).getItems.toList) }
    } recover {
      case e: HttpResponseException => None
    })
  }

  override def getBucket(bucketName: String): Future[Option[Bucket]] = {
    val getter = getStorage(getBucketServiceAccountCredential).buckets().get(bucketName)
    retry(when500)(() => Future {
      blocking { Option(executeGoogleRequest(getter)) }
    } recover {
      case e: HttpResponseException => None
    })
  }

  private def listGroupMembersInternal(groupName: String): Future[Option[Seq[String]]] = {
    val fetcher = getGroupDirectory.members.list(groupName)
    retry(when500)(() => Future {
      val result = blocking {
        executeGoogleRequest(fetcher).getMembers
      }

      Option(result) match {
        case None => Option(Seq.empty)
        case Some(list) => Option(list.map(_.getEmail))
      }
    }) recover {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  override def listGroupMembers(group: RawlsGroup): Future[Option[Set[Either[RawlsUserRef, RawlsGroupRef]]]] = {
    val proxyPattern = s"PROXY_(.+)@${appsDomain}".r
    val groupPattern = s"GROUP_(.+)@${appsDomain}".r

    listGroupMembersInternal(group.groupEmail.value) map { membersOption =>
      membersOption match {
        case None => None
        case Some(emails) => Option(emails map {
          case proxyPattern(subjectId) => Left(RawlsUserRef(RawlsUserSubjectId(subjectId)))
          case groupPattern(groupName) => Right(RawlsGroupRef(RawlsGroupName(groupName)))
          case other => throw new RawlsException(s"Group member is neither a proxy or sub group: [$other]")
        } toSet)
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
          executeGoogleRequest(inserter)
        }
      }
    }
  }

  override def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup] = {
    val newGroup = RawlsGroup(groupRef.groupName, RawlsGroupEmail(toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val directory = getGroupDirectory
    val groups = directory.groups
    val future = retryExponentially(when500orGoogleError) {
      () => Future {
        val inserter = groups.insert(new Group().setEmail(newGroup.groupEmail.value).setName(newGroup.groupName.value.take(60))) // group names have a 60 char limit
        blocking {
          executeGoogleRequest(inserter)
        }
      }
    } map { _ => newGroup}
    future recover {
      // Group already exists. Consider this success.
      case t: HttpResponseException if t.getStatusCode == StatusCodes.Conflict.intValue => newGroup
    }
  }

  override def addMemberToGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit] = {
    val memberEmail = memberToAdd match {
      case Left(member) => new Member().setEmail(toProxyFromUser(member)).setRole(groupMemberRole)
      case Right(member) => new Member().setEmail(member.groupEmail.value).setRole(groupMemberRole)
    }
    val inserter = getGroupDirectory.members.insert(group.groupEmail.value, memberEmail)
    val insertFuture: Future[Unit] = retryExponentially(when500orGoogleError)(() => Future { blocking { executeGoogleRequest(inserter) } })
    insertFuture recover {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.Conflict.intValue => Unit // it is ok of the email is already there
    }
  }

  override def removeMemberFromGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit] = {
    val memberEmail = memberToAdd match {
      case Left(member) => toProxyFromUser(member.userSubjectId)
      case Right(member) => member.groupEmail.value
    }
    val deleter = getGroupDirectory.members.delete(group.groupEmail.value, memberEmail)
    val deleteFuture: Future[Unit] = retry(when500)(() => Future { blocking { executeGoogleRequest(deleter) } })
    deleteFuture recover {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => Unit // it is ok of the email is already missing
    }
  }

  override def deleteGoogleGroup(group: RawlsGroup): Future[Unit] = {
    val directory = getGroupDirectory
    val groups = directory.groups
    val deleter = groups.delete(group.groupEmail.value)
    val deleteFuture: Future[Unit] = retry(when500)(() => Future { blocking { executeGoogleRequest(deleter) } })
    deleteFuture recover {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => Unit // if the group is already gone, don't fail
    }
  }

  //add a file to the bucket as the specified user, then remove it
  //returns an ErrorReport if something went wrong, otherwise returns None
  override def diagnosticBucketWrite(user: RawlsUser, bucketName: String): Future[Option[ErrorReport]] = {
    val uuid = UUID.randomUUID.toString
    val so = new StorageObject().setName(uuid)
    val media = new InputStreamContent("text/plain",
      new ByteArrayInputStream("This is a test file generated by FireCloud which should have been automatically deleted".getBytes))

    val result = getUserCredentials(user) map { credentialOpt =>
      credentialOpt match {
        case None => Some(ErrorReport(new RawlsException("Unable to load credentials for user")))
        case Some(credential) => {
          val inserter = getStorage(credential).objects().insert(bucketName, so, media)
          inserter.getMediaHttpUploader().setDirectUploadEnabled(true)
          val getter = getStorage(credential).objects().get(bucketName, uuid)
          val remover = getStorage(credential).objects().delete(bucketName, uuid)
          try {
            executeGoogleRequest(inserter)
            executeGoogleRequest(getter)
            executeGoogleRequest(remover)
            None
          } catch {
            case t: HttpResponseException => Some(ErrorReport(new RawlsException(t.getMessage)))
          }
        }
      }
    }
    result
  }

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    retryWhen500(() => {
      val so = new StorageObject().setName(userInfo.userSubjectId)
      val encryptedToken = Aes256Cbc.encrypt(refreshToken.getBytes, tokenSecretKey).get
      so.setMetadata(Map("iv" -> encryptedToken.base64Iv))
      val media = new InputStreamContent("text/plain", new ByteArrayInputStream(encryptedToken.base64CipherText.getBytes))
      val inserter = getStorage(getBucketServiceAccountCredential).objects().insert(tokenBucketName, so, media)
      inserter.getMediaHttpUploader().setDirectUploadEnabled(true)
      executeGoogleRequest(inserter)
    } )
  }

  override def getToken(rawlsUserRef: RawlsUserRef): Future[Option[String]] = {
    retryWhen500(() => {
      val get = getStorage(getBucketServiceAccountCredential).objects().get(tokenBucketName, rawlsUserRef.userSubjectId.value)
      get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
      val tokenBytes = new ByteArrayOutputStream()
      try {
        get.executeMediaAndDownloadTo(tokenBytes)
        val so = executeGoogleRequest(get)
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
        Option(new time.DateTime(executeGoogleRequest(get).getUpdated.getValue))
      } catch {
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
      }
    } )
  }

  override def deleteToken(userInfo: UserInfo): Future[Unit] = {
    retryWhen500(() => {
      executeGoogleRequest(getStorage(getBucketServiceAccountCredential).objects().delete(tokenBucketName, userInfo.userSubjectId))
    } )
  }

  // these really should all be private, but we're opening up a few of these to allow integration testing
  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case _ => false
    }
  }

  private def when500orGoogleError( throwable: Throwable ): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => {
        ((t.getStatusCode == 403 || t.getStatusCode == 429) && t.getDetails.getErrors.head.getDomain.equalsIgnoreCase("usageLimits")) ||
          (t.getStatusCode == 400 && t.getDetails.getErrors.head.getReason.equalsIgnoreCase("invalid")) ||
          (t.getStatusCode == 404)
      }
      case h: HttpResponseException => when500(throwable)
      case _ => false
    }
  }

  private def retryWhen500[T]( op: () => T ) = {
    retry(when500)(()=>Future(blocking(op())))
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
  def toUserFromProxy(proxy: String) = executeGoogleRequest(getGroupDirectory.groups().get(proxy)).getName
  def toGoogleGroupName(groupName: RawlsGroupName) = s"GROUP_${groupName.value}@${appsDomain}"

  def adminGroupName = s"${groupsPrefix}-ADMINS@${appsDomain}"
  def makeGroupEntityString(groupId: String) = s"group-$groupId"

  def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]] = {
    getToken(rawlsUserRef) map { refreshTokenOption =>
      refreshTokenOption.map { refreshToken =>
        new GoogleCredential.Builder().setTransport(httpTransport)
          .setJsonFactory(jsonFactory)
          .setClientSecrets(tokenClientSecrets)
          .build().setFromTokenResponse(new TokenResponse().setRefreshToken(refreshToken))
      }
    }
  }

  private def executeGoogleRequest[T](request: AbstractGoogleClientRequest[T]): T = {
    import spray.json._
    import GoogleRequestJsonSupport._

    if (logger.underlying.isDebugEnabled) {
      val payload = Option(request.getHttpContent) match {
        case Some(content: JsonHttpContent) =>
          Try {
            val outputStream = new ByteArrayOutputStream()
            content.writeTo(outputStream)
            outputStream.toString.parseJson
          }.toOption
        case _ => None
      }

      val start = System.currentTimeMillis()
      Try {
        request.executeUnparsed()
      } match {
        case Success(response) =>
          logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - start, Option(response.getStatusCode), None).toJson(GoogleRequestFormat).compactPrint)
          response.parseAs(request.getResponseClass)
        case Failure(httpRegrets: HttpResponseException) =>
          logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - start, Option(httpRegrets.getStatusCode), None).toJson(GoogleRequestFormat).compactPrint)
          throw httpRegrets
        case Failure(regrets) =>
          logger.debug(GoogleRequest(request.getRequestMethod, request.buildHttpRequestUrl().toString, payload, System.currentTimeMillis() - start, None, Option(ErrorReport(regrets))).toJson(GoogleRequestFormat).compactPrint)
          throw regrets
      }
    } else {
      request.execute()
    }
  }
}

private case class GoogleRequest(method: String, url: String, payload: Option[JsValue], time_ms: Long, statusCode: Option[Int], errorReport: Option[ErrorReport])
private object GoogleRequestJsonSupport extends JsonSupport {
  import WorkspaceJsonSupport.ErrorReportFormat
  val GoogleRequestFormat = jsonFormat6(GoogleRequest)
}
