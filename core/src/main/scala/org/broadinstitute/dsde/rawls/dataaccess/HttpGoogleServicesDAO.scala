package org.broadinstitute.dsde.rawls.dataaccess

import java.io._
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.Materializer
import com.google.api.client.auth.oauth2.{Credential, TokenResponse}
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{HttpResponseException, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.admin.directory.model._
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.cloudbilling.Cloudbilling
import com.google.api.services.cloudbilling.model.{BillingAccount, ProjectBillingInfo}
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.compute.model.{ServiceAccount, UsageExportLocation}
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.genomics.model.Operation
import com.google.api.services.genomics.{Genomics, GenomicsScopes}
import com.google.api.services.oauth2.Oauth2.Builder
import com.google.api.services.plus.PlusScopes
import com.google.api.services.servicemanagement.ServiceManagement
import com.google.api.services.servicemanagement.model.EnableServiceRequest
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.model.Bucket.{Lifecycle, Logging}
import com.google.api.services.storage.model._
import com.google.api.services.storage.{Storage, StorageScopes}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.crypto.{Aes256Cbc, EncryptedBytes, SecretKey}
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.google.GoogleUtilities
import io.grpc.Status.Code
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, GoogleInstrumentedService}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard, Retry}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroup}
import org.joda.time
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Future, _}
import scala.io.Source
import scala.util.{Success, Try}

class HttpGoogleServicesDAO(
  useServiceAccountForBuckets: Boolean,
  val clientSecrets: GoogleClientSecrets,
  clientEmail: String,
  subEmail: String,
  pemFile: String,
  appsDomain: String,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int,
  serviceProject: String,
  tokenEncryptionKey: String,
  tokenClientSecretsJson: String,
  billingPemEmail: String,
  billingPemFile: String,
  val billingEmail: String,
  bucketLogsMaxAge: Int,
  maxPageSize: Int = 200,
  override val workbenchMetricBaseName: String,
  proxyNamePrefix: String)(implicit val system: ActorSystem, val materializer: Materializer, implicit val executionContext: ExecutionContext ) extends GoogleServicesDAO(groupsPrefix) with FutureSupport with GoogleUtilities {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)
  val API_SERVICE_MANAGEMENT = "ServiceManagement"
  val API_CLOUD_RESOURCE_MANAGER = "CloudResourceManager"

  // modify these if we need more granular access in the future
  val workbenchLoginScopes = Seq(PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE) ++ workbenchLoginScopes
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)
  val genomicsScopes = Seq(GenomicsScopes.GENOMICS) // google requires GENOMICS, not just GENOMICS_READONLY, even though we're only doing reads
  val billingScopes = Seq("https://www.googleapis.com/auth/cloud-billing")

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance
  val tokenClientSecrets: GoogleClientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(tokenClientSecretsJson))
  val tokenBucketName = "tokens-" + clientSecrets.getDetails.getClientId.stripSuffix(".apps.googleusercontent.com")
  val tokenSecretKey = SecretKey(tokenEncryptionKey)

  initTokenBucket()

  protected def initTokenBucket(): Unit = {
    implicit val service = GoogleInstrumentedService.Storage
    try {
      getStorage(getBucketServiceAccountCredential).buckets().get(tokenBucketName).executeUsingHead()
    } catch {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue =>
        val logBucket = new Bucket().
          setName(tokenBucketName + "-logs")
        val logInserter = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, logBucket)
        executeGoogleRequest(logInserter)
        allowGoogleCloudStorageWrite(logBucket.getName)

        val bucketAcls = List(new BucketAccessControl().setEntity("user-" + clientEmail).setRole("OWNER"))
        val defaultObjectAcls = List(new ObjectAccessControl().setEntity("user-" + clientEmail).setRole("OWNER"))
        val bucket = new Bucket().
          setName(tokenBucketName).
          setAcl(bucketAcls).
          setDefaultObjectAcl(defaultObjectAcls).
          setLogging(new Logging().setLogBucket(logBucket.getName))
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, bucket)
        executeGoogleRequest(inserter)
    }
  }

  def allowGoogleCloudStorageWrite(bucketName: String): Unit = {
    implicit val service = GoogleInstrumentedService.Storage
    // add cloud-storage-analytics@google.com as a writer so it can write logs
    // do it as a separate call so bucket gets default permissions plus this one
    val storage = getStorage(getBucketServiceAccountCredential)
    val bac = new BucketAccessControl().setEntity("group-cloud-storage-analytics@google.com").setRole("WRITER")
    executeGoogleRequest(storage.bucketAccessControls.insert(bucketName, bac))
  }

  private def getBucketName(workspaceId: String) = s"${groupsPrefix}-${workspaceId}"

  override def setupWorkspace(userInfo: UserInfo, project: RawlsBillingProject, projectOwnerGroup: RawlsGroup, workspaceId: String, workspaceName: WorkspaceName, authDomain: Set[ManagedGroupRef], authDomainProjectOwnerIntersection: Option[Set[RawlsUserRef]]): Future[GoogleWorkspaceInfo] = {

    // we do not make a special access group for project owners because the only member would be a single group
    // we will just use that group directly and avoid potential google problems with a group being in too many groups
    val accessGroupRefsByLevel: Map[WorkspaceAccessLevel, RawlsGroupRef] = (groupAccessLevelsAscending.filterNot(_ == ProjectOwner)).map { accessLevel =>
      (accessLevel, RawlsGroupRef(RawlsGroupName(workspaceAccessGroupName(workspaceId, accessLevel))))
    }.toMap

    val intersectionGroupRefsByLevel: Option[Map[WorkspaceAccessLevel, RawlsGroupRef]] = {
      if(authDomain.isEmpty) None
      else {
        Option(groupAccessLevelsAscending.map { accessLevel =>
          (accessLevel, RawlsGroupRef(RawlsGroupName(intersectionGroupName(workspaceId, accessLevel))))
        }.toMap)
      }
    }

    def rollbackGroups(groupInsertTries: Iterable[Try[RawlsGroup]]) = {
      Future.traverse(groupInsertTries) {
        case Success(group) => deleteGoogleGroup(group)
        case _ => Future.successful(())
      }
    }

    def insertGroups(groupRefsByAccess: Map[WorkspaceAccessLevel, RawlsGroupRef]): Future[Map[WorkspaceAccessLevel, Try[RawlsGroup]]] = {
      Future.traverse(groupRefsByAccess) { case (access, groupRef) => toFutureTry(createGoogleGroup(groupRef)).map(access -> _) }.map(_.toMap)
    }

    def insertOwnerMember: (Map[WorkspaceAccessLevel, RawlsGroup]) => Future[Map[WorkspaceAccessLevel, RawlsGroup]] = { groupsByAccess =>
      val ownerGroup = groupsByAccess(WorkspaceAccessLevels.Owner)
      addMemberToGoogleGroup(ownerGroup, Left(RawlsUser(userInfo))).map(_ => groupsByAccess + (WorkspaceAccessLevels.Owner -> ownerGroup.copy(users = ownerGroup.users + RawlsUser(userInfo))))
    }

    def insertAuthDomainProjectOwnerIntersection: (Map[WorkspaceAccessLevel, RawlsGroup]) => Future[Map[WorkspaceAccessLevel, RawlsGroup]] = { groupsByAccess =>
      val projectOwnerGroup = groupsByAccess(WorkspaceAccessLevels.ProjectOwner)
      val inserts = Future.traverse(authDomainProjectOwnerIntersection.getOrElse(Set.empty)) { userRef =>
        addEmailToGoogleGroup(projectOwnerGroup.groupEmail.value, toProxyFromUser(userRef.userSubjectId))
      }

      inserts.map { _ =>
        groupsByAccess.map {
          case (ProjectOwner, group) => ProjectOwner -> group.copy(users = authDomainProjectOwnerIntersection.getOrElse(Set.empty))
          case otherwise => otherwise
        }
      }
    }

    def insertBucket: (Map[WorkspaceAccessLevel, RawlsGroup], Option[Map[WorkspaceAccessLevel, RawlsGroup]]) => Future[String] = { (accessGroupsByLevel, intersectionGroupsByLevel) =>
      implicit val service = GoogleInstrumentedService.Storage
      val bucketName = getBucketName(workspaceId)
      retryWhen500orGoogleError {
        () => {

          // When Intersection Groups exist, these are the groups used to determine ACLs.  Otherwise, Access Groups are used directly.

          val groupsByAccess = intersectionGroupsByLevel getOrElse accessGroupsByLevel

          // bucket ACLs should be:
          //   project owner - bucket writer
          //   workspace owner - bucket writer
          //   workspace writer - bucket writer
          //   workspace reader - bucket reader
          //   bucket service account - bucket owner
          val workspaceAccessToBucketAcl: Map[WorkspaceAccessLevel, String] = Map(ProjectOwner -> "WRITER", Owner -> "WRITER", Write -> "WRITER", Read -> "READER")
          val bucketAcls =
            groupsByAccess.map { case (access, group) => newBucketAccessControl(makeGroupEntityString(group.groupEmail.value), workspaceAccessToBucketAcl(access)) }.toSeq :+
              newBucketAccessControl("user-" + clientEmail, "OWNER")

          // default object ACLs should be:
          //   project owner - object reader
          //   workspace owner - object reader
          //   workspace writer - object reader
          //   workspace reader - object reader
          //   bucket service account - object owner
          val defaultObjectAcls =
            groupsByAccess.map { case (access, group) => newObjectAccessControl(makeGroupEntityString(group.groupEmail.value), "READER") }.toSeq :+
              newObjectAccessControl("user-" + clientEmail, "OWNER")

          val logging = new Logging().setLogBucket(getStorageLogsBucketName(project.projectName))
          val bucket = new Bucket().
            setName(bucketName).
            setAcl(bucketAcls).
            setDefaultObjectAcl(defaultObjectAcls).
            setLogging(logging)
          val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(project.projectName.value, bucket)
          executeGoogleRequest(inserter)

          bucketName
        }
      }
    }

    def insertInitialStorageLog: (String) => Future[Unit] = { (bucketName) =>
      implicit val service = GoogleInstrumentedService.Storage
      retryWhen500orGoogleError {
        () => {
          // manually insert an initial storage log
          val stream: InputStreamContent = new InputStreamContent("text/plain", new ByteArrayInputStream(
            s""""bucket","storage_byte_hours"
                |"$bucketName","0"
                |""".stripMargin.getBytes))
          // use an object name that will always be superseded by a real storage log
          val storageObject = new StorageObject().setName(s"${bucketName}_storage_00_initial_log")
          val objectInserter = getStorage(getBucketServiceAccountCredential).objects().insert(getStorageLogsBucketName(project.projectName), storageObject, stream)
          executeGoogleRequest(objectInserter)
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
      accessGroups <- assertSuccessfulTries(accessGroupTries) flatMap insertOwnerMember map { _ + (ProjectOwner -> projectOwnerGroup) }
      intersectionGroupTries <- intersectionGroupInserts
      intersectionGroups <- intersectionGroupTries match {
        case Some(t) => assertSuccessfulTries(t) flatMap insertOwnerMember flatMap insertAuthDomainProjectOwnerIntersection map { Option(_) }
        case None => Future.successful(None)
      }
      bucketName <- insertBucket(accessGroups, intersectionGroups)
      _ <- insertInitialStorageLog(bucketName)
    } yield GoogleWorkspaceInfo(bucketName, accessGroups, intersectionGroups)

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

  def intersectionGroupName(workspaceId: String, accessLevel: WorkspaceAccessLevel) = {
    s"I_$workspaceId-${accessLevel.toString}"
  }

  def createCromwellAuthBucket(billingProject: RawlsBillingProjectName, projectNumber: Long, authBucketReaders: Set[RawlsGroupEmail]): Future[String] = {
    implicit val service = GoogleInstrumentedService.Storage
    val bucketName = getCromwellAuthBucketName(billingProject)
    retryWithRecoverWhen500orGoogleError(
      () => {
        val bucketAcls = List(
          newBucketAccessControl("project-editors-" + projectNumber, "OWNER"),
          newBucketAccessControl("project-owners-" + projectNumber, "OWNER")) ++
          authBucketReaders.map(email => newBucketAccessControl(makeGroupEntityString(email.value), "READER")).toList

        val defaultObjectAcls = List(
          newObjectAccessControl("project-editors-" + projectNumber, "OWNER"),
          newObjectAccessControl("project-owners-" + projectNumber, "OWNER")) ++
          authBucketReaders.map(email => newObjectAccessControl(makeGroupEntityString(email.value), "READER")).toList

        val bucket = new Bucket().setName(bucketName).setAcl(bucketAcls).setDefaultObjectAcl(defaultObjectAcls)
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(billingProject.value, bucket)
        executeGoogleRequest(inserter)

        bucketName
      }) { case t: HttpResponseException if t.getStatusCode == 409 => bucketName }
  }

  def grantReadAccess(billingProject: RawlsBillingProjectName,
                      bucketName: String,
                      authBucketReaders: Set[RawlsGroupEmail]): Future[String] = {
    implicit val service = GoogleInstrumentedService.Storage

    def insertNewAcls() = for {
      readerEmail <- authBucketReaders

      bucketAcls = newBucketAccessControl(makeGroupEntityString(readerEmail.value), "READER")
      defaultObjectAcls = newObjectAccessControl(makeGroupEntityString(readerEmail.value), "READER")

      inserters = List(
        getStorage(getBucketServiceAccountCredential).bucketAccessControls.insert(bucketName, bucketAcls),
        getStorage(getBucketServiceAccountCredential).defaultObjectAccessControls.insert(bucketName, defaultObjectAcls))

      inserter <- inserters
      _ <- executeGoogleRequest(inserter)
    } yield ()

    retryWithRecoverWhen500orGoogleError(
      () => { insertNewAcls(); bucketName }
    ) {
      case t: HttpResponseException if t.getStatusCode == 409 => bucketName
    }
  }

  def createStorageLogsBucket(billingProject: RawlsBillingProjectName): Future[String] = {
    implicit val service = GoogleInstrumentedService.Storage
    val bucketName = getStorageLogsBucketName(billingProject)
    logger debug s"storage log bucket: $bucketName"

    retryWithRecoverWhen500orGoogleError(() => {
      val bucket = new Bucket().setName(bucketName)
      val storageLogExpiration = new Lifecycle.Rule()
        .setAction(new Action().setType("Delete"))
        .setCondition(new Condition().setAge(bucketLogsMaxAge))
      bucket.setLifecycle(new Lifecycle().setRule(List(storageLogExpiration)))
      val inserter = getStorage(getBucketServiceAccountCredential).buckets().insert(billingProject.value, bucket)
      executeGoogleRequest(inserter)

      bucketName
    }) {
      // bucket already exists
      case t: HttpResponseException if t.getStatusCode == 409 => bucketName
    }
  }

  private def newBucketAccessControl(entity: String, accessLevel: String) =
    new BucketAccessControl().setEntity(entity).setRole(accessLevel)

  private def newObjectAccessControl(entity: String, accessLevel: String) =
    new ObjectAccessControl().setEntity(entity).setRole(accessLevel)

  override def deleteBucket(bucketName: String): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Storage
    val buckets = getStorage(getBucketServiceAccountCredential).buckets
    val deleter = buckets.delete(bucketName)
    retryWithRecoverWhen500orGoogleError(() => {
      executeGoogleRequest(deleter)
      true
    }) {
      //Google returns 409 Conflict if the bucket isn't empty.
      case t: HttpResponseException if t.getStatusCode == 409 =>
        //Google doesn't let you delete buckets that are full.
        //You can either remove all the objects manually, or you can set up lifecycle management on the bucket.
        //This can be used to auto-delete all objects next time the Google lifecycle manager runs (~every 24h).
        //More info: http://bit.ly/1WCYhhf
        val deleteEverythingRule = new Lifecycle.Rule()
          .setAction(new Action().setType("Delete"))
          .setCondition(new Condition().setAge(0))
        val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule))
        val patcher = buckets.patch(bucketName, new Bucket().setLifecycle(lifecycle))
        retryWhen500orGoogleError(() => { executeGoogleRequest(patcher) })

        false
      // Bucket is already deleted
      case t: HttpResponseException if t.getStatusCode == 404 =>
        true
    }
  }

  override def isAdmin(userEmail: String): Future[Boolean] = {
    hasGoogleRole(adminGroupName, userEmail)
  }

  override def isLibraryCurator(userEmail: String): Future[Boolean] = {
    hasGoogleRole(curatorGroupName, userEmail)
  }

  override def addLibraryCurator(userEmail: String): Future[Unit] = {
    addEmailToGoogleGroup(curatorGroupName, userEmail)
  }

  override def removeLibraryCurator(userEmail: String): Future[Unit] = {
    removeEmailFromGoogleGroup(curatorGroupName, userEmail)
  }

  override def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Groups
    val query = getGroupDirectory.members.get(roleGroupName, userEmail)
    retryWithRecoverWhen500orGoogleError(() => {
      executeGoogleRequest(query)
      true
    }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => false
    }
  }

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val member = new Member().setEmail(user.userEmail.value).setRole(groupMemberRole)
    val inserter = getGroupDirectory.members.insert(toProxyFromUser(user.userSubjectId), member)
    retryWhen500orGoogleError(() => { executeGoogleRequest(inserter) })
  }

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val deleter = getGroupDirectory.members.delete(toProxyFromUser(user.userSubjectId), user.userEmail.value)
    retryWhen500orGoogleError(() => { executeGoogleRequest(deleter) })
  }

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = {
    isEmailInGoogleGroup(user.userEmail.value, toProxyFromUser(user.userSubjectId))
  }

  override def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Groups
    val getter = getGroupDirectory.members.get(groupName, email)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(getter)) }) {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    } map { _.isDefined }
  }

  override def getGoogleGroup(groupName: String)(implicit executionContext: ExecutionContext): Future[Option[Group]] = {
    implicit val service = GoogleInstrumentedService.Groups
    val getter = getGroupDirectory.groups().get(groupName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(getter)) }) {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  override def getBucketUsage(projectName: RawlsBillingProjectName, bucketName: String, maxResults: Option[Long]): Future[BigInt] = {
    implicit val service = GoogleInstrumentedService.Storage

    def usageFromLogObject(o: StorageObject): Future[BigInt] = {
      streamObject(o.getBucket, o.getName) { inputStream =>
        val content = Source.fromInputStream(inputStream).mkString
        val byteHours = BigInt(content.split('\n')(1).split(',')(1).replace("\"", ""))
        // convert byte/hours to byte/days to better match the billing unit of GB/days
        byteHours / 24
      }
    }

    def recurse(pageToken: Option[String] = None): Future[BigInt] = {
      // Fetch objects with a prefix of "${bucketName}_storage_", (ignoring "_usage_" logs)
      val fetcher = getStorage(getBucketServiceAccountCredential).
        objects().
        list(getStorageLogsBucketName(projectName)).
        setPrefix(s"${bucketName}_storage_")
      maxResults.foreach(fetcher.setMaxResults(_))
      pageToken.foreach(fetcher.setPageToken)

      retryWhen500orGoogleError(() => {
        val result = executeGoogleRequest(fetcher)
        (Option(result.getItems), Option(result.getNextPageToken))
      }) flatMap {
        case (None, _) =>
          // No storage logs, so make sure that the bucket is actually empty
          val fetcher = getStorage(getBucketServiceAccountCredential).objects.list(bucketName).setMaxResults(1L)
          retryWhen500orGoogleError(() => {
            Option(executeGoogleRequest(fetcher).getItems)
          }) flatMap {
            case None => Future.successful(BigInt(0))
            case Some(_) => Future.failed(new GoogleStorageLogException("Not Available"))
          }
        case (_, Some(nextPageToken)) => recurse(Option(nextPageToken))
        case (Some(items), None) =>
          /* Objects are returned "in alphabetical order" (http://stackoverflow.com/a/36786877/244191). Because of the
           * timestamp, they are also in increasing chronological order. Therefore, the last one is the most recent.
           */
          usageFromLogObject(items.last)
      }
    }

    recurse()
  }

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = {
    implicit val service = GoogleInstrumentedService.Storage
    val aclGetter = getStorage(getBucketServiceAccountCredential).bucketAccessControls().list(bucketName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(aclGetter).getItems.toList) }) {
      case e: HttpResponseException => None
    }
  }

  override def getBucket(bucketName: String)(implicit executionContext: ExecutionContext): Future[Option[Bucket]] = {
    implicit val service = GoogleInstrumentedService.Storage
    val getter = getStorage(getBucketServiceAccountCredential).buckets().get(bucketName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(getter)) }) {
      case e: HttpResponseException => None
    }
  }

  private def listGroupMembersInternal(groupName: String): Future[Option[Seq[String]]] = {
    val fetcher = getGroupDirectory.members.list(groupName).setMaxResults(maxPageSize)

    listGroupMembersRecursive(fetcher) map { pagesOption =>
      pagesOption.map { pages =>
        pages.toSeq.flatMap { page =>
          Option(page.getMembers) match {
            case None => Seq.empty
            case Some(members) => members.map(_.getEmail)
          }
        }
      }
    }
  }

  /**
   * recursive because the call to list all members is paginated.
   * @param fetcher
   * @param accumulated the accumulated Members objects, 1 for each page, the head element is the last prior request
   *                    for easy retrieval. The initial state is Some(Nil). This is what is eventually returned. This
   *                    is None when the group does not exist.
   * @return None if the group does not exist or a Members object for each page.
   */
  private def listGroupMembersRecursive(fetcher: Directory#Members#List, accumulated: Option[List[Members]] = Some(Nil)): Future[Option[List[Members]]] = {
    implicit val service = GoogleInstrumentedService.Groups
    accumulated match {
      // when accumulated has a Nil list then this must be the first request
      case Some(Nil) => retryWithRecoverWhen500orGoogleError(() => {
        blocking {
          Option(executeGoogleRequest(fetcher))
        }
      }) {
        case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
      }.flatMap(firstPage => listGroupMembersRecursive(fetcher, firstPage.map(List(_))))

      // the head is the Members object of the prior request which contains next page token
      case Some(head :: xs) if head.getNextPageToken != null => retryWhen500orGoogleError(() => {
        blocking {
          executeGoogleRequest(fetcher.setPageToken(head.getNextPageToken))
        }
      }).flatMap(nextPage => listGroupMembersRecursive(fetcher, accumulated.map(pages => nextPage :: pages)))

      // when accumulated is None (group does not exist) or next page token is null
      case _ => Future.successful(accumulated)
    }
  }

  val proxyPattern = s"${proxyNamePrefix}PROXY_(.+)@${appsDomain}".toLowerCase.r
  val groupPattern = s"${proxyNamePrefix}GROUP_(.+)@${appsDomain}".toLowerCase.r

  override def listGroupMembers(group: RawlsGroup): Future[Option[Map[String, Option[Either[RawlsUserRef, RawlsGroupRef]]]]] = {
    listGroupMembersInternal(group.groupEmail.value) map { membersOption =>
      membersOption match {
        case None => None
        case Some(emails) => Option(emails map(_.toLowerCase) map {
          case email@proxyPattern(subjectId) => email -> Option(Left(RawlsUserRef(RawlsUserSubjectId(subjectId))))
          case email@groupPattern(groupName) => email -> Option(Right(RawlsGroupRef(RawlsGroupName(groupName))))
          case email => email -> None
        } toMap)
      }
    }
  }

  def createProxyGroup(user: RawlsUser): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val directory = getGroupDirectory
    val groups = directory.groups
    retryWhen500orGoogleError (() => {
      val inserter = groups.insert(new Group().setEmail(toProxyFromUser(user.userSubjectId)).setName(user.userEmail.value))
      executeGoogleRequest(inserter)
    })
  }

  def deleteProxyGroup(user: RawlsUser): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val directory = getGroupDirectory
    val groups = directory.groups
    retryWhen500orGoogleError (() => {
      val deleter = groups.delete(toProxyFromUser(user.userSubjectId))
      executeGoogleRequest(deleter)
    })
  }

  override def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup] = {
    implicit val service = GoogleInstrumentedService.Groups
    val newGroup = RawlsGroup(groupRef.groupName, RawlsGroupEmail(toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val directory = getGroupDirectory
    val groups = directory.groups

    for {
      // first verify it does not exist
      // then try to create it, sometimes we get a 503 error creating the group but it actually gets created
      // so we get a 409 on subsequent retries - because we have already check it does not exist assume that any
      // 409 means we created it
      // last verify it is there
      preexistingGroup <- retryWithRecoverWhen500orGoogleError( () => {
        Option(executeGoogleRequest(groups.get(newGroup.groupEmail.value)))
      }) {
        case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
      }

      _ <- if (preexistingGroup.isDefined) Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"google group ${newGroup.groupEmail.value} already exists"))) else Future.successful(())

      _ <- retryWithRecoverWhen500orGoogleError (() => {
        // group names have a 60 char limit
        executeGoogleRequest(groups.insert(new Group().setEmail(newGroup.groupEmail.value).setName(newGroup.groupName.value.take(60))))
        () // need this because in the recover case below we can't create a Group object to return
      }) {
        case e: HttpResponseException if e.getStatusCode == StatusCodes.Conflict.intValue =>
      }

      // GAWB-853 verify that the group exists by retrying to get group until success or too many tries
      _ <- retryWhen500orGoogleError (() => {
        executeGoogleRequest(groups.get(newGroup.groupEmail.value))
      }) recover {
        case t: Throwable =>
        // log but ignore any error in the getter, downstream code will fail or not as appropriate
        logger.debug(s"could not verify that google group ${newGroup.groupEmail.value} exists", t)
      }

    } yield newGroup
  }

  override def addMemberToGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit] = {
    val memberEmail = memberToAdd match {
      case Left(member) => toProxyFromUser(member)
      case Right(member) => member.groupEmail.value
    }
    addEmailToGoogleGroup(group.groupEmail.value, memberEmail)
  }

  override def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val inserter = getGroupDirectory.members.insert(groupEmail, new Member().setEmail(emailToAdd).setRole(groupMemberRole))
    retryWithRecoverWhen500orGoogleError[Unit](() => { executeGoogleRequest(inserter) }) {
      case t: HttpResponseException => {
        StatusCode.int2StatusCode(t.getStatusCode) match {
          case StatusCodes.Conflict => () // it is ok if the email is already there
          case StatusCodes.PreconditionFailed => {
            val msg = s"Precondition failed adding user $emailToAdd to group $groupEmail. Is the user a member of too many groups?"
            logger.error(msg)
            throw new RawlsException(msg, t)
          }
          case _ => throw t
        }
      }
    }
  }

  override def removeMemberFromGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit] = {
    val memberEmail = memberToAdd match {
      case Left(member) => toProxyFromUser(member.userSubjectId)
      case Right(member) => member.groupEmail.value
    }
    removeEmailFromGoogleGroup(group.groupEmail.value, memberEmail)
  }

  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val deleter = getGroupDirectory.members.delete(groupEmail, emailToRemove)
    retryWithRecoverWhen500orGoogleError[Unit](() => { executeGoogleRequest(deleter) }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => () // it is ok of the email is already missing
    }
  }

  override def deleteGoogleGroup(group: RawlsGroup): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
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
    implicit val service = GoogleInstrumentedService.Storage
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
    // we make requests to the target bucket as the default pet, if the user only has read access.
    // the default pet is created in a project without APIs enabled. Due to Google issue #16062674, we cannot
    // call the GCS JSON API as the default pet; we must use the XML API. In turn, this means we cannot use
    // the GCS client library (which internally calls the JSON API); we must hand-code a call to the XML API.

    // the "proper" request to make into the XML API is HEAD-bucket; see https://cloud.google.com/storage/docs/xml-api/overview.
    // however, the akka-http client is vulnerable to connection-pool exhaustion with HEAD requests; see akka/akka-http#1495.
    // therefore, we make a request to GET /?storageClass. Since all we care about is the 200 vs. 40x status code
    // in the response, this is an equivalent request.
    val bucketUrl = s"https://$bucketName.storage.googleapis.com/?storageClass"
    val bucketRequest = httpClientUtils.addHeader(RequestBuilding.Get(bucketUrl), Authorization(userInfo.accessToken))

    httpClientUtils.executeRequest(http, bucketRequest) map { httpResponse =>
      logger.info(s"diagnosticBucketRead to $bucketName returned ${httpResponse.status.intValue}")
      httpResponse.status match {
        case StatusCodes.OK => None
        case x => Some(ErrorReport(x, x.defaultMessage()))
      }
    } recover {
      case t:Throwable =>
        logger.warn(s"diagnosticBucketRead to $bucketName encountered unexpected error: ${t.getMessage}")
        Some(ErrorReport(t))
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
    implicit val service = GoogleInstrumentedService.Billing
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

  protected def listBillingAccounts(credential: Credential)(implicit executionContext: ExecutionContext): Future[Seq[BillingAccount]] = {
    implicit val service = GoogleInstrumentedService.Billing
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
        // This error message is purely informational. A client can determine which scopes it has
        // been granted, so an insufficiently-scoped request would generally point to a programming
        // error.
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, BillingAccountScopes(billingScopes).toJson.toString))
    }
  }

  override def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]] = {
    val cred = getUserCredential(userInfo)
    val billingSvcCred = getBillingServiceAccountCredential
    listBillingAccounts(cred) flatMap { accountList =>
      Future.sequence(accountList map { acct =>
        val acctName = acct.getName
        //NOTE: We guarantee that the firecloud billing service account always has the correct scopes.
        //So credentialOwnsBillingAccount == false definitely means no access (rather than no scopes).
        credentialOwnsBillingAccount(billingSvcCred, acctName) map { firecloudHasAccount =>
          RawlsBillingAccount(RawlsBillingAccountName(acctName), firecloudHasAccount, acct.getDisplayName)
        }
      })
    }
  }

  override def listBillingAccountsUsingServiceCredential(implicit executionContext: ExecutionContext): Future[Seq[RawlsBillingAccount]] = {
    val billingSvcCred = getBillingServiceAccountCredential
    listBillingAccounts(billingSvcCred) map { accountList =>
      accountList.map(acct => RawlsBillingAccount(RawlsBillingAccountName(acct.getName), true, acct.getDisplayName))
    }
  }

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Storage
    retryWhen500orGoogleError(() => {
      val so = new StorageObject().setName(userInfo.userSubjectId.value)
      val encryptedToken = Aes256Cbc.encrypt(refreshToken.getBytes, tokenSecretKey).get
      so.setMetadata(Map("iv" -> encryptedToken.base64Iv))
      val media = new InputStreamContent("text/plain", new ByteArrayInputStream(encryptedToken.base64CipherText.getBytes))
      val inserter = getStorage(getBucketServiceAccountCredential).objects().insert(tokenBucketName, so, media)
      inserter.getMediaHttpUploader().setDirectUploadEnabled(true)
      executeGoogleRequest(inserter)
    })
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
    implicit val service = GoogleInstrumentedService.Storage
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
        Http(system).singleRequest(RequestBuilding.Get(url))

      case None => Future.successful(())
    }
  }

  override def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Storage
    retryWhen500orGoogleError(() => {
      executeGoogleRequest(getStorage(getBucketServiceAccountCredential).objects().delete(tokenBucketName, rawlsUserRef.userSubjectId.value))
    } )
  }

  override def getGenomicsOperation(jobId: String): Future[Option[JsObject]] = {
    implicit val service = GoogleInstrumentedService.Genomics
    val opId = s"operations/$jobId"
    val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, getGenomicsServiceAccountCredential).setApplicationName(appName).build()
    val operationRequest = genomicsApi.operations().get(opId)

    retryWithRecoverWhen500orGoogleError(() => {
      // Google library returns a Map[String,AnyRef], but we don't care about understanding the response
      // So, use Google's functionality to get the json string, then parse it back into a generic json object
      Option(executeGoogleRequest(operationRequest).toPrettyString.parseJson.asJsObject)
    }) {
      // Recover from Google 404 errors because it's an expected return status.
      // Here we use `None` to represent a 404 from Google.
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  override def listGenomicsOperations(implicit executionContext: ExecutionContext): Future[Seq[Operation]] = {
    implicit val service = GoogleInstrumentedService.Genomics
    val opId = "operations"
    val filter = s"projectId = $serviceProject"
    val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, getGenomicsServiceAccountCredential).setApplicationName(appName).build()
    val operationRequest = genomicsApi.operations().list(opId).setFilter(filter)
    retryWhen500orGoogleError(() => {
      val list = executeGoogleRequest(operationRequest)
      list.getOperations
    })
  }

  override def getGoogleProject(projectName: RawlsBillingProjectName): Future[Project] = {
    implicit val service = GoogleInstrumentedService.Billing
    val credential = getBillingServiceAccountCredential

    val cloudResManager = getCloudResourceManager(credential)

    retryWhen500orGoogleError(() => {
      executeGoogleRequest(cloudResManager.projects().get(projectName.value))
    })
  }

  override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount): Future[RawlsBillingProjectOperationRecord] = {
    implicit val service = GoogleInstrumentedService.Billing
    val credential = getBillingServiceAccountCredential

    val cloudResManager = getCloudResourceManager(credential)

    retryWhen500orGoogleError(() => {
      executeGoogleRequest(cloudResManager.projects().create(new Project().setName(projectName.value).setProjectId(projectName.value).setLabels(Map("billingaccount" -> labelSafeString(billingAccount.displayName)))))
    }).recover {
      case t: HttpResponseException if StatusCode.int2StatusCode(t.getStatusCode) == StatusCodes.Conflict =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"A google project by the name $projectName already exists"))
    } map ( googleOperation => {
      if (toScalaBool(googleOperation.getDone) && Option(googleOperation.getError).exists(_.getCode.intValue() == Code.ALREADY_EXISTS.value())) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Conflict, s"A google project by the name $projectName already exists"))
      }
      RawlsBillingProjectOperationRecord(projectName.value, CREATE_PROJECT_OPERATION, googleOperation.getName, toScalaBool(googleOperation.getDone), Option(googleOperation.getError).map(error => toErrorMessage(error.getMessage, error.getCode)), API_CLOUD_RESOURCE_MANAGER)
    })
  }

  override def pollOperation(rawlsBillingProjectOperation: RawlsBillingProjectOperationRecord): Future[RawlsBillingProjectOperationRecord] = {
    implicit val service = GoogleInstrumentedService.Billing
    val credential = getBillingServiceAccountCredential

    // this code is a colossal DRY violation but because the operations collection is different
    // for cloudResManager and servicesManager and they return different but identical Status objects
    // there is not much else to be done... too bad scala does not have duck typing.
    rawlsBillingProjectOperation.api match {
      case API_CLOUD_RESOURCE_MANAGER =>
        val cloudResManager = getCloudResourceManager(credential)

        retryWhen500orGoogleError(() => {
          executeGoogleRequest(cloudResManager.operations().get(rawlsBillingProjectOperation.operationId))
        }).map { op =>
          rawlsBillingProjectOperation.copy(done = toScalaBool(op.getDone), errorMessage = Option(op.getError).map(error => toErrorMessage(error.getMessage, error.getCode)))
        }

      case API_SERVICE_MANAGEMENT =>
        val servicesManager = getServicesManager(credential)

        retryWhen500orGoogleError(() => {
          executeGoogleRequest(servicesManager.operations().get(rawlsBillingProjectOperation.operationId))
        }).map { op =>
          rawlsBillingProjectOperation.copy(done = toScalaBool(op.getDone), errorMessage = Option(op.getError).map(error => toErrorMessage(error.getMessage, error.getCode)))
        }
    }

  }

  /**
   * converts a possibly null java boolean to a scala boolean, null is treated as false
   */
  private def toScalaBool(b: java.lang.Boolean) = Option(b).contains(java.lang.Boolean.TRUE)

  private def toErrorMessage(message: String, code: Int): String = {
    s"${Option(message).getOrElse("")} - code ${code}"
  }

  private def removePolicyBindings(projectName: RawlsBillingProjectName, policiesToRemove: Map[String, Seq[String]]): Future[Unit] = {
    val cloudResManager = getCloudResourceManager(getBillingServiceAccountCredential)
    implicit val service = GoogleInstrumentedService.CloudResourceManager

    for {
      existingPolicy <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(cloudResManager.projects().getIamPolicy(projectName.value, null))
      })

      _ <- retryWhen500orGoogleError(() => {
        val existingPolicies: Map[String, Seq[String]] = existingPolicy.getBindings.map { policy => policy.getRole -> policy.getMembers.toSeq }.toMap

        // this may result in keys with empty-seq values.  That's ok, we will ignore those later
        val updatedKeysWithRemovedPolicies: Map[String, Seq[String]] = policiesToRemove.keys.map { k =>
          val existingForKey = existingPolicies(k)
          val updatedForKey = existingForKey.toSet diff policiesToRemove(k).toSet
          k -> updatedForKey.toSeq
        }.toMap

        // Use standard Map ++ instead of semigroup because we want to replace the original values
        val newPolicies = existingPolicies ++ updatedKeysWithRemovedPolicies

        val updatedBindings = newPolicies.collect { case (role, members) if members.nonEmpty =>
          new Binding().setRole(role).setMembers(members.distinct)
        }.toSeq

        // when setting IAM policies, always reuse the existing policy so the etag is preserved.
        val policyRequest = new SetIamPolicyRequest().setPolicy(existingPolicy.setBindings(updatedBindings))
        executeGoogleRequest(cloudResManager.projects().setIamPolicy(projectName.value, policyRequest))
      })
    } yield ()
  }

  override def addPolicyBindings(projectName: RawlsBillingProjectName, policiesToAdd: Map[String, List[String]]): Future[Unit] = {
    val cloudResManager = getCloudResourceManager(getBillingServiceAccountCredential)
    implicit val service = GoogleInstrumentedService.CloudResourceManager

    for {
      existingPolicy <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(cloudResManager.projects().getIamPolicy(projectName.value, null))
      })

      _ <- retryWhen500orGoogleError(() => {
        val existingPolicies: Map[String, List[String]] = existingPolicy.getBindings.map { policy => policy.getRole -> policy.getMembers.toList }.toMap

        // |+| is a semigroup: it combines a map's keys by combining their values' members instead of replacing them
        import cats.implicits._
        val newPolicies = existingPolicies |+| policiesToAdd

        val updatedBindings = newPolicies.collect { case (role, members) if members.nonEmpty =>
          new Binding().setRole(role).setMembers(members.distinct)
        }.toSeq

        // when setting IAM policies, always reuse the existing policy so the etag is preserved.
        val policyRequest = new SetIamPolicyRequest().setPolicy(existingPolicy.setBindings(updatedBindings))
        executeGoogleRequest(cloudResManager.projects().setIamPolicy(projectName.value, policyRequest))
      })
    } yield ()
  }

  override def beginProjectSetup(project: RawlsBillingProject, projectTemplate: ProjectTemplate): Future[Try[Seq[RawlsBillingProjectOperationRecord]]] = {
    implicit val instrumentedService = GoogleInstrumentedService.Billing
    val projectName = project.projectName
    val credential = getBillingServiceAccountCredential

    val billingManager = getCloudBillingManager(credential)
    val serviceManager = getServicesManager(credential)

    val projectResourceName = s"projects/${projectName.value}"

    // all of these things should be idempotent
    toFutureTry(for {
      // set the billing account
      billing <- retryWhen500orGoogleError(() => {
        val billingAccount = project.billingAccount.getOrElse(throw new RawlsException(s"billing account undefined for project ${project.projectName.value}")).value
        executeGoogleRequest(billingManager.projects().updateBillingInfo(projectResourceName, new ProjectBillingInfo().setBillingEnabled(true).setBillingAccountName(billingAccount)))
      })

      // add new policies to the project
      _ <- addPolicyBindings(projectName, projectTemplate.policies.mapValues(_.toList))

      // enable appropriate google apis
      operations <- Future.sequence(projectTemplate.services.map { service => retryWhen500orGoogleError(() => {
        executeGoogleRequest(serviceManager.services().enable(service, new EnableServiceRequest().setConsumerId(s"project:${projectName.value}")))
      }) map { googleOperation =>
        RawlsBillingProjectOperationRecord(projectName.value, service, googleOperation.getName, toScalaBool(googleOperation.getDone), Option(googleOperation.getError).map(error => toErrorMessage(error.getMessage, error.getCode)), API_SERVICE_MANAGEMENT)
      }})

    } yield {
      operations
    })
  }

  override def addRoleToGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Unit] = {
    addPolicyBindings(projectName, Map(s"roles/$role" -> List(s"group:${groupEmail.value}")))
  }

  override def removeRoleFromGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Unit] = {
    removePolicyBindings(projectName, Map(s"roles/$role" -> Seq(s"group:${groupEmail.value}")))
  }

  override def completeProjectSetup(project: RawlsBillingProject, authBucketReaders: Set[RawlsGroupEmail]): Future[Try[Unit]] = {
    implicit val service = GoogleInstrumentedService.Billing
    val projectName = project.projectName
    val credential = getBillingServiceAccountCredential

    val computeManager = getComputeManager(credential)

    // all of these things should be idempotent
    toFutureTry(for {
      // create project usage export bucket
      exportBucket <- retryWithRecoverWhen500orGoogleError(() => {
        val bucket = new Bucket().setName(projectUsageExportBucketName(projectName))
        executeGoogleRequest(getStorage(credential).buckets.insert(projectName.value, bucket))
      }) { case t: HttpResponseException if t.getStatusCode == 409 => new Bucket().setName(projectUsageExportBucketName(projectName)) }

      // create bucket for workspace bucket storage/usage logs
      storageLogsBucket <- createStorageLogsBucket(projectName)
      _ <- retryWhen500orGoogleError(() => { allowGoogleCloudStorageWrite(storageLogsBucket) })

      googleProject <- getGoogleProject(projectName)

      cromwellAuthBucket <- createCromwellAuthBucket(projectName, googleProject.getProjectNumber, authBucketReaders)

      _ <- retryWhen500orGoogleError(() => {
        val usageLoc = new UsageExportLocation().setBucketName(projectUsageExportBucketName(projectName)).setReportNamePrefix("usage")
        executeGoogleRequest(computeManager.projects().setUsageExportBucket(projectName.value, usageLoc))
      })
    } yield {
      // nothing
    })
  }

  def labelSafeString(s: String): String = {
    // The google ui says the only valid values for labels are lower case letters and numbers and must start with
    // a letter. Dashes also appear to be acceptable though the ui does not say so.
    // The fc in front ensures it starts with a lower case letter
    "fc-" + s.toLowerCase.replaceAll("[^a-z0-9\\-]", "-")
  }

  override def deleteProject(projectName: RawlsBillingProjectName): Future[Unit]= {
    implicit val service = GoogleInstrumentedService.Billing
    val billingServiceAccountCredential = getBillingServiceAccountCredential
    val resMgr = getCloudResourceManager(billingServiceAccountCredential)
    val billingManager = getCloudBillingManager(billingServiceAccountCredential)
    val projectNameString = projectName.value
    for {
      _ <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(billingManager.projects().updateBillingInfo(s"projects/${projectName.value}", new ProjectBillingInfo().setBillingEnabled(false)))
      })
      _ <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(resMgr.projects().delete(projectNameString))
      })
    } yield {
      // nothing
    }
  }

  def projectUsageExportBucketName(projectName: RawlsBillingProjectName) = s"${projectName.value}-usage-export"

  def getComputeManager(credential: Credential): Compute = {
    new Compute.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getCloudBillingManager(credential: Credential): Cloudbilling = {
    new Cloudbilling.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getServicesManager(credential: Credential): ServiceManagement = {
    new ServiceManagement.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getCloudResourceManager(credential: Credential): CloudResourceManager = {
    new CloudResourceManager.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
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
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(directoryScopes)
      .setServiceAccountUser(subEmail)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getBucketServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(storageScopes) // grant bucket-creation powers
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getGenomicsServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
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

  def toProxyFromUser(rawlsUser: RawlsUser): String = toProxyFromUser(rawlsUser.userSubjectId)
  def toProxyFromUser(userInfo: UserInfo): String = toProxyFromUser(userInfo.userSubjectId)
  def toProxyFromUser(subjectId: RawlsUserSubjectId): String = s"${proxyNamePrefix}PROXY_${subjectId.value}@${appsDomain}"
  def toUserFromProxy(proxy: String): String = {
    implicit val service = GoogleInstrumentedService.Groups
    executeGoogleRequest(getGroupDirectory.groups().get(proxy)).getName
  }
  def toGoogleGroupName(groupName: RawlsGroupName) = s"${proxyNamePrefix}GROUP_${groupName.value}@${appsDomain}"

  def adminGroupName = s"${groupsPrefix}-ADMINS@${appsDomain}"
  def curatorGroupName = s"${groupsPrefix}-CURATORS@${appsDomain}"
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

  def getAccessTokenUsingJson(saKey: String) : Future[String] = Future {
    val keyStream = new ByteArrayInputStream(saKey.getBytes)
    val credential = ServiceAccountCredentials.fromStream(keyStream).createScoped(storageScopes)
    credential.refreshAccessToken.getTokenValue
  }

  def getServiceAccountRawlsUser(): Future[RawlsUser] = {
    getRawlsUserForCreds(getBucketServiceAccountCredential)
  }

  def getRawlsUserForCreds(creds: Credential): Future[RawlsUser] = {
    implicit val service = GoogleInstrumentedService.Groups
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
      UserInfo(rawlsUser.userEmail, OAuth2BearerToken(creds.getAccessToken), creds.getExpiresInSeconds, rawlsUser.userSubjectId)
    }
  }

  private def streamObject[A](bucketName: String, objectName: String)(f: (InputStream) => A): Future[A] = {
    implicit val service = GoogleInstrumentedService.Storage
    val getter = getStorage(getBucketServiceAccountCredential).objects().get(bucketName, objectName).setAlt("media")
    retryWhen500orGoogleError(() => { executeGoogleFetch(getter) { is => f(is) } })
  }

}

class GoogleStorageLogException(message: String) extends RawlsException(message)
