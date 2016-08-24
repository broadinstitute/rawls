package org.broadinstitute.dsde.rawls.dataaccess

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, StringReader}
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.google.api.client.auth.oauth2.{Credential, TokenResponse}
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.http.json.JsonHttpContent
import com.google.api.client.http.{HttpResponseException, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.admin.directory.model._
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.cloudbilling.Cloudbilling
import com.google.api.services.cloudbilling.model.{BillingAccount, ProjectBillingInfo}
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.compute.model.UsageExportLocation
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.genomics.{Genomics, GenomicsScopes}
import com.google.api.services.oauth2.Oauth2.Builder
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.model.Bucket.{Lifecycle, Logging}
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, ObjectAccessControl, StorageObject}
import com.google.api.services.storage.{Storage, StorageScopes}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.crypto.{Aes256Cbc, EncryptedBytes, SecretKey}
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.{BucketDeleted, DeleteBucket}
import org.broadinstitute.dsde.rawls.util.FutureSupport
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.joda.time
import spray.client.pipelining._
import spray.http.HttpHeaders.Authorization
import spray.http.{HttpResponse, OAuth2BearerToken, StatusCode, StatusCodes}
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.{Future, _}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

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
  tokenClientSecretsJson: String,
  billingClientSecrets: GoogleClientSecrets,
  billingPemEmail: String,
  billingPemFile: String,
  val billingEmail: String)( implicit val system: ActorSystem, implicit val executionContext: ExecutionContext ) extends GoogleServicesDAO(groupsPrefix) with Retry with FutureSupport with LazyLogging {

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

  // modify these if we need more granular access in the future
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE, PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)
  val genomicsScopes = Seq(GenomicsScopes.GENOMICS) // google requires GENOMICS, not just GENOMICS_READONLY, even though we're only doing reads
  val billingScopes = Seq("https://www.googleapis.com/auth/cloud-billing")

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val tokenClientSecrets: GoogleClientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(tokenClientSecretsJson))
  val tokenBucketName = "tokens-" + clientSecrets.getDetails.getClientId.stripSuffix(".apps.googleusercontent.com")
  val tokenSecretKey = SecretKey(tokenEncryptionKey)

  val serviceAccountClientId: String = clientSecrets.getDetails.get("client_email").toString

  initTokenBucket()

  protected def initTokenBucket(): Unit = {
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
      retryWhen500orGoogleError {
        () => {

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
          executeGoogleRequest(inserter)
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
    retryWithRecoverWhen500orGoogleError(
      () => {
        val bucket = new Bucket().setName(bucketName)
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(billingProject.value, bucket)
        executeGoogleRequest(inserter)

        bucketName
      }) { case t: HttpResponseException if t.getStatusCode == 409 => bucketName }
  }

  private def newBucketAccessControl(entity: String, accessLevel: String) =
    new BucketAccessControl().setEntity(entity).setRole(accessLevel)

  private def newObjectAccessControl(entity: String, accessLevel: String) =
    new ObjectAccessControl().setEntity(entity).setRole(accessLevel)

  override def deleteBucket(bucketName: String, monitorRef: ActorRef): Future[Any] = {
    val buckets = getStorage(getBucketServiceAccountCredential).buckets
    val deleter = buckets.delete(bucketName)
    retryWithRecoverWhen500orGoogleError(() => {
      executeGoogleRequest(deleter)
      monitorRef ! BucketDeleted(bucketName)
    }) {
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
        val bucketFuture = retryWhen500orGoogleError(() => { executeGoogleRequest(fetcher) })
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
    retryWithRecoverWhen500orGoogleError(() => {
      executeGoogleRequest(query)
      true
    }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => false
    }
  }

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = {
    val member = new Member().setEmail(user.userEmail.value).setRole(groupMemberRole)
    val inserter = getGroupDirectory.members.insert(toProxyFromUser(user.userSubjectId), member)
    retryWhen500orGoogleError(() => { executeGoogleRequest(inserter) })
  }

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = {
    val deleter = getGroupDirectory.members.delete(toProxyFromUser(user.userSubjectId), user.userEmail.value)
    retryWhen500orGoogleError(() => { executeGoogleRequest(deleter) })
  }

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = {
    isEmailInGoogleGroup(user.userEmail.value, toProxyFromUser(user.userSubjectId))
  }

  override def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean] = {
    val getter = getGroupDirectory.members.get(groupName, email)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(getter)) }) {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    } map { _.isDefined }
  }

  override def getGoogleGroup(groupName: String): Future[Option[Group]] = {
    val getter = getGroupDirectory.groups().get(groupName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(getter)) }) {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = {
    val aclGetter = getStorage(getBucketServiceAccountCredential).bucketAccessControls().list(bucketName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(aclGetter).getItems.toList) }) {
      case e: HttpResponseException => None
    }
  }

  override def getBucket(bucketName: String): Future[Option[Bucket]] = {
    val getter = getStorage(getBucketServiceAccountCredential).buckets().get(bucketName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(getter)) }) {
      case e: HttpResponseException => None
    }
  }

  private def listGroupMembersInternal(groupName: String): Future[Option[Seq[String]]] = {
    val fetcher = getGroupDirectory.members.list(groupName)
    retryWithRecoverWhen500orGoogleError(() => {
      val result = blocking {
        executeGoogleRequest(fetcher).getMembers
      }

      Option(result) match {
        case None => Option(Seq.empty)
        case Some(list) => Option(list.map(_.getEmail))
      }
    }) {
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
    retryWhen500orGoogleError (() => {
      val inserter = groups.insert(new Group().setEmail(toProxyFromUser(user.userSubjectId)).setName(user.userEmail.value))
      executeGoogleRequest(inserter)
    })
  }

  def deleteProxyGroup(user: RawlsUser): Future[Unit] = {
    val directory = getGroupDirectory
    val groups = directory.groups
    retryWhen500orGoogleError (() => {
      val deleter = groups.delete(toProxyFromUser(user.userSubjectId))
      executeGoogleRequest(deleter)
    })
  }

  override def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup] = {
    val newGroup = RawlsGroup(groupRef.groupName, RawlsGroupEmail(toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val directory = getGroupDirectory
    val groups = directory.groups

    for {
      googleGroup <- retryWhen500orGoogleError (() => {
        // group names have a 60 char limit
        executeGoogleRequest(groups.insert(new Group().setEmail(newGroup.groupEmail.value).setName(newGroup.groupName.value.take(60))))
      })

      // GAWB-853 verify that the group exists by retrying to get group until success or too many tries
      _ <- retryWhen500orGoogleError (() => {
        executeGoogleRequest(groups.get(googleGroup.getEmail))
      }) recover {
        case t: Throwable =>
        // log but ignore any error in the getter, downstream code will fail or not as appropriate
        logger.debug(s"could not verify that google group $googleGroup exists", t)
      }

    } yield newGroup
  }

  override def addMemberToGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit] = {
    val memberEmail = memberToAdd match {
      case Left(member) => new Member().setEmail(toProxyFromUser(member)).setRole(groupMemberRole)
      case Right(member) => new Member().setEmail(member.groupEmail.value).setRole(groupMemberRole)
    }
    val inserter = getGroupDirectory.members.insert(group.groupEmail.value, memberEmail)
    retryWithRecoverWhen500orGoogleError[Unit](() => { executeGoogleRequest(inserter) }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.Conflict.intValue => () // it is ok of the email is already there
    }
  }

  override def removeMemberFromGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit] = {
    val memberEmail = memberToAdd match {
      case Left(member) => toProxyFromUser(member.userSubjectId)
      case Right(member) => member.groupEmail.value
    }
    val deleter = getGroupDirectory.members.delete(group.groupEmail.value, memberEmail)
    retryWithRecoverWhen500orGoogleError[Unit](() => { executeGoogleRequest(deleter) }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => () // it is ok of the email is already missing
    }
  }

  override def deleteGoogleGroup(group: RawlsGroup): Future[Unit] = {
    val directory = getGroupDirectory
    val groups = directory.groups
    val deleter = groups.delete(group.groupEmail.value)
    retryWithRecoverWhen500orGoogleError[Unit](() => { executeGoogleRequest(deleter) }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => () // if the group is already gone, don't fail
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

  def diagnosticBucketRead(userInfo: UserInfo, bucketName: String): Future[Option[ErrorReport]] = {
    Future {
      val getter = getStorage(getUserCredential(userInfo)).buckets().get(bucketName)
      try {
        blocking {
          executeGoogleRequest(getter)
        }
        None
      } catch {
        case t: HttpResponseException => Some(ErrorReport(StatusCode.int2StatusCode(t.getStatusCode), t.getMessage))
      }
    }
  }

  /**
    * NOTE: This function will returns "false" in both of the following cases:
    * if you don't have sufficient scopes
    *   - Google's JSON response body will contain "message": "Request had insufficient authentication scopes."
    * if you're not authorized to see the billing account
    *   - Google's JSON response body will contain "message" : "The caller does not have permission"
    */
  protected def credentialOwnsBillingAccount(credential: Credential, billingAccountName: String): Future[Boolean] = {
    val fetcher = getCloudBillingManager(credential).billingAccounts().get(billingAccountName)
    retryWithRecoverWhen500orGoogleError(() => {
      blocking {
        executeGoogleRequest(fetcher)
      }
      true //if the request succeeds, it has access.
    }) {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue => false
    }
  }

  protected def listBillingAccounts(credential: Credential): Future[Seq[BillingAccount]] = {
    val fetcher = getCloudBillingManager(credential).billingAccounts().list()
    retryWithRecoverWhen500orGoogleError(() => {
      val list = blocking {
        executeGoogleRequest(fetcher)
      }
      // option-wrap getBillingAccounts because it returns null for an empty list
      Option(list.getBillingAccounts).map(_.toSeq).getOrElse(Seq.empty)
    }) {
      case gjre: GoogleJsonResponseException
        if gjre.getStatusCode == StatusCodes.Forbidden.intValue &&
          gjre.getDetails.getMessage == "Request had insufficient authentication scopes." =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, BillingAccountScopes(billingScopes).toJson.toString))
    }
  }

  def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]] = {
    val cred = getUserCredential(userInfo)
    val billingSvcCred = getBillingServiceAccountCredential
    listBillingAccounts(cred) flatMap { accountList =>
      Future.sequence(accountList map { acct =>
        val acctName = acct.getName
        //NOTE: We guarantee that the firecloud billing service account always has the correct scopes.
        //So credentialOwnsBillingAccount == false definitely means no access (rather than no scopes).
        credentialOwnsBillingAccount(billingSvcCred, acctName) map { firecloudHasAccount =>
          RawlsBillingAccount(RawlsBillingAccountName(acctName), firecloudHasAccount)
        }
      })
    }
  }

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    retryWhen500orGoogleError(() => {
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
    getTokenAndDate(rawlsUserRef.userSubjectId.value) map {
      _.map { case (token, date) => token }
    }
  }

  override def getTokenDate(rawlsUserRef: RawlsUserRef): Future[Option[time.DateTime]] = {
    getTokenAndDate(rawlsUserRef.userSubjectId.value) map {
      _.map { case (token, date) =>
        // validate the token by attempting to build a UserInfo from it: Google will return an error if we can't
        UserInfo.buildFromTokens(buildCredentialFromRefreshToken(token))
        date
      }
    }
  }

  private def getTokenAndDate(userSubjectID: String): Future[Option[(String, time.DateTime)]] = {
    retryWhen500orGoogleError(() => {
      val get = getStorage(getBucketServiceAccountCredential).objects().get(tokenBucketName, userSubjectID)
      get.getMediaHttpDownloader.setDirectDownloadEnabled(true)
      try {
        val tokenBytes = new ByteArrayOutputStream()
        get.executeMediaAndDownloadTo(tokenBytes)
        val so = executeGoogleRequest(get)
        for {
          t <- Option(new String(Aes256Cbc.decrypt(EncryptedBytes(tokenBytes.toString, so.getMetadata.get("iv")), tokenSecretKey).get))
          d <- Option(new time.DateTime(so.getUpdated.getValue))
        } yield (t, d)
      } catch {
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
      }
    } )
  }

  override def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    getToken(rawlsUserRef) map {
      case Some(token) =>
        val url = s"https://accounts.google.com/o/oauth2/revoke?token=$token"
        val pipeline = sendReceive
        pipeline(Get(url))

      case None => Future.successful(Unit)
    }
  }

  override def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    retryWhen500orGoogleError(() => {
      executeGoogleRequest(getStorage(getBucketServiceAccountCredential).objects().delete(tokenBucketName, rawlsUserRef.userSubjectId.value))
    } )
  }

  override def getGenomicsOperation(jobId: String): Future[Option[JsObject]] = {

    import spray.json._

    val opId = s"operations/$jobId"

    val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, getGenomicsServiceAccountCredential).setApplicationName(appName).build()
    val operationRequest = genomicsApi.operations().get(opId)

    retryWithRecoverWhen500orGoogleError[Option[JsObject]](() => {
      // Google library returns a Map[String,AnyRef], but we don't care about understanding the response
      // So, use Google's functionality to get the json string, then parse it back into a generic json object
      Some(executeGoogleRequest(operationRequest).toPrettyString.parseJson.asJsObject)
    }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName, projectTemplate: ProjectTemplate): Future[Unit] = {
    val credential = getBillingServiceAccountCredential

    val cloudResManager = getCloudResourceManager(credential)
    val billingManager = getCloudBillingManager(credential)
    val computeManager = getComputeManager(credential)

    val projectResourceName = s"projects/${projectName.value}"
    for {
      // create the project
      project <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(cloudResManager.projects().create(new Project().setName(projectName.value).setProjectId(projectName.value)))
      }).recover {
        case t: HttpResponseException if StatusCode.int2StatusCode(t.getStatusCode) == StatusCodes.Conflict =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"A google project by the name $projectName already exists"))
      }

      // set the billing account
      billing <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(billingManager.projects().updateBillingInfo(projectResourceName, new ProjectBillingInfo().setBillingEnabled(true).setBillingAccountName(billingAccount.value)))
      })

      // get current permissions
      bindings <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(cloudResManager.projects().getIamPolicy(projectName.value, null)).getBindings
      })

      // add any missing permissions
      policy <- retryWhen500orGoogleError(() => {
        val updatedPolicy = new Policy().setBindings(updateBindings(bindings, projectTemplate))
        executeGoogleRequest(cloudResManager.projects().setIamPolicy(projectName.value, new SetIamPolicyRequest().setPolicy(updatedPolicy)))
      })

      // enable appropriate google apis
      _ <- Future.sequence(projectTemplate.services.map { service => retryExponentially(when500orGoogleError)(() => {
        enableServiceApi(projectName.value, service, credential)
      })})

      // create project usage export bucket
      bucket <- retryWhen500orGoogleError(() => {
        val bucket = new Bucket().setName(s"${projectName.value}-usage-export")
        executeGoogleRequest(getStorage(credential).buckets.insert(projectName.value, bucket))
      })

      // set usage export bucket on project, it may take up to 5 minutes for the project to be ready for this but google is working to fix that
      _ <- retryUntilSuccessOrTimeout(always)(5 seconds, 6 minutes)(() => {
        Future(blocking(executeGoogleRequest(computeManager.projects().setUsageExportBucket(projectName.value, new UsageExportLocation().setBucketName(bucket.getName).setReportNamePrefix("usage")))))
      })

    } yield {
      // nothing
    }
  }

  override def deleteProject(projectName: RawlsBillingProjectName): Future[Unit]= {
    val billingServiceAccountCredential = getBillingServiceAccountCredential
    val resMgr = getCloudResourceManager(billingServiceAccountCredential)
    val billingManager = getCloudBillingManager(billingServiceAccountCredential)
    val projectNameString = projectName.value
    for {
      _ <- retryWhen500orGoogleError(() => {
      executeGoogleRequest(billingManager.projects().updateBillingInfo(s"projects/${projectName.value}", new ProjectBillingInfo().setBillingEnabled(false)))
      })
      - <- retryWhen500orGoogleError(() => {
        executeGoogleRequest (resMgr.projects ().delete (projectNameString))
      })
    } yield {
      // nothing
    }
  }

  def getComputeManager(credential: Credential): Compute = {
    new Compute.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getCloudBillingManager(credential: Credential): Cloudbilling = {
    new Cloudbilling.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getCloudResourceManager(credential: Credential): CloudResourceManager = {
    new CloudResourceManager.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  private def enableServiceApi(projectName: String, service: String, credential: Credential): Future[HttpResponse] = {
    import spray.json._
    import GoogleRequestJsonSupport._

    // note that I could not find a google client library for this end point but I know how to http
    val url = s"https://servicemanagement.googleapis.com/v1/services/$service:enable"
    val pipeline = addHeader(Authorization(OAuth2BearerToken(credential.getAccessToken))) ~> sendReceive
    val payload = s"""{"consumerId": "project:$projectName"}"""
    val start = System.currentTimeMillis()
    pipeline(Post(url, payload)).recover {
      case t: Throwable =>
        logger.debug(GoogleRequest("POST", url, Option(payload.parseJson), System.currentTimeMillis() - start, None, Option(ErrorReport(t))).toJson(GoogleRequestFormat).compactPrint)
        throw t
    } map { response =>
      logger.debug(GoogleRequest("POST", url, Option(payload.parseJson), System.currentTimeMillis() - start, Option(response.status.intValue), None).toJson(GoogleRequestFormat).compactPrint)
      if (response.status.isFailure) {
        throw new GoogleServiceException(s"failure enabling service $service for project $projectName: status ${response.status}, response: ${response.entity.asString}")
      } else {
        response
      }
    }
  }

  private def updateBindings(bindings: Seq[Binding], template: ProjectTemplate) = {
    bindings.map { policy =>
      val newMembers = (template.policies.getOrElse(policy.getRole, Seq.empty) ++ policy.getMembers).distinct
      new Binding().setRole(policy.getRole).setMembers(newMembers)
    }
  }


  private def when500orGoogleError( throwable: Throwable ): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => {
        ((t.getStatusCode == 403 || t.getStatusCode == 429) && t.getDetails.getErrors.head.getDomain.equalsIgnoreCase("usageLimits")) ||
          (t.getStatusCode == 400 && t.getDetails.getErrors.head.getReason.equalsIgnoreCase("invalid")) ||
          (t.getStatusCode == 404)
      }
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case gse: GoogleServiceException => true
      case _ => false
    }
  }

  private def retryWhen500orGoogleError[T](op: () => T) = {
    retryExponentially(when500orGoogleError)(() => Future(blocking(op())))
  }

  private def retryWithRecoverWhen500orGoogleError[T](op: () => T)(recover: PartialFunction[Throwable, T]) = {
    retryExponentially(when500orGoogleError)(() => Future(blocking(op())).recover(recover))
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
  def getGenomicsServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(serviceAccountClientId)
      .setServiceAccountScopes(genomicsScopes)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getBillingServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountScopes(Seq(ComputeScopes.CLOUD_PLATFORM)) // need this broad scope to create/manage projects
      .setServiceAccountId(billingPemEmail)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(billingPemFile))
      .setServiceAccountUser(billingEmail)
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
        buildCredentialFromRefreshToken(refreshToken)
      }
    }
  }

  private def buildCredentialFromRefreshToken(refreshToken: String): GoogleCredential = {
    new GoogleCredential.Builder().setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setClientSecrets(tokenClientSecrets)
      .build().setFromTokenResponse(new TokenResponse().setRefreshToken(refreshToken))
  }

  def getServiceAccountRawlsUser(): Future[RawlsUser] = {
    getRawlsUserForCreds(getBucketServiceAccountCredential)
  }

  def getRawlsUserForCreds(creds: Credential): Future[RawlsUser] = {
    val oauth2 = new Builder(httpTransport, jsonFactory, null).setApplicationName(appName).build()
    Future {
      creds.refreshToken()
      val tokenInfo = executeGoogleRequest(oauth2.tokeninfo().setAccessToken(creds.getAccessToken))
      RawlsUser(RawlsUserSubjectId(tokenInfo.getUserId), RawlsUserEmail(tokenInfo.getEmail))
    }
  }

  def getServiceAccountUserInfo(): Future[UserInfo] = {
    val creds = getBucketServiceAccountCredential
    getRawlsUserForCreds(creds).map { rawlsUser =>
      UserInfo(rawlsUser.userEmail.value, OAuth2BearerToken(creds.getAccessToken), creds.getExpiresInSeconds, rawlsUser.userSubjectId.value)
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

private class GoogleServiceException(message: String) extends RawlsException(message)
