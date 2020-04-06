package org.broadinstitute.dsde.rawls.dataaccess

import java.io._
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import cats.effect.{ContextShift, IO, Timer}
import cats.data.NonEmptyList
import cats.syntax.functor._
import cats.instances.future._
import com.google.api.client.auth.oauth2.{Credential, TokenResponse}
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{HttpResponseException, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.admin.directory.model._
import com.google.api.services.admin.directory.{Directory, DirectoryScopes}
import com.google.api.services.cloudbilling.Cloudbilling
import com.google.api.services.cloudbilling.model.{BillingAccount, ProjectBillingInfo, TestIamPermissionsRequest}
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.deploymentmanager.model.{ConfigFile, Deployment, TargetConfiguration}
import com.google.api.services.deploymentmanager.DeploymentManagerV2Beta
import com.google.api.services.oauth2.Oauth2.Builder
import com.google.api.services.plus.PlusScopes
import com.google.api.services.servicemanagement.ServiceManagement
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.model.Bucket.{Lifecycle, Logging}
import com.google.api.services.storage.model._
import com.google.api.services.storage.{Storage, StorageScopes}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.Identity
import com.google.cloud.storage.BucketInfo.LifecycleRule
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleAction
import fs2.Stream
import org.broadinstitute.dsde.rawls.crypto.{Aes256Cbc, EncryptedBytes, SecretKey}
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, GoogleUtilities}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, GoogleResourceTypes}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.joda.time
import spray.json._
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import com.google.api.services.genomics.v2alpha1.{Genomics, GenomicsScopes}
import com.google.api.services.iam.v1.Iam
import com.google.api.services.iamcredentials.v1.IAMCredentials
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenRequest
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.dataaccess.CloudResourceManagerV2Model.{Folder, FolderSearchResponse}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates

import scala.collection.JavaConverters._
import scala.concurrent.{Future, _}
import scala.io.Source
import scala.util.matching.Regex

import io.opencensus.scala.Tracing._

case class Resources (
                       name: String,
                       `type`: String,
                       properties: Map[String, JsValue]
                     )
case class ConfigContents (
                            resources: Seq[Resources]
                          )

//we're not using camelcase here because these become GCS labels, which all have to be lowercase.
case class TemplateLocation(
                             template_org: String,
                             template_repo: String,
                             template_branch: String,
                             template_path: String
                           )

object DeploymentManagerJsonSupport {
  import spray.json.DefaultJsonProtocol._
  implicit val resourceJsonFormat = jsonFormat3(Resources)
  implicit val configContentsJsonFormat = jsonFormat1(ConfigContents)
  implicit val templateLocationJsonFormat = jsonFormat4(TemplateLocation)
}

class HttpGoogleServicesDAO(
  useServiceAccountForBuckets: Boolean,
  val clientSecrets: GoogleClientSecrets,
  clientEmail: String,
  subEmail: String,
  pemFile: String,
  appsDomain: String,
  orgID: Long,
  groupsPrefix: String,
  appName: String,
  deletedBucketCheckSeconds: Int,
  serviceProject: String,
  tokenEncryptionKey: String,
  tokenClientSecretsJson: String,
  billingPemEmail: String,
  billingPemFile: String,
  val billingEmail: String,
  val billingGroupEmail: String,
  billingGroupEmailAliases: List[String],
  billingProbeEmail: String,
  bucketLogsMaxAge: Int,
  maxPageSize: Int = 200,
  hammCromwellMetadata: HammCromwellMetadata,
  googleStorageService: GoogleStorageService[IO],
  googleServiceHttp: GoogleServiceHttp[IO],
  topicAdmin: GoogleTopicAdmin[IO],
  override val workbenchMetricBaseName: String,
  proxyNamePrefix: String,
  deploymentMgrProject: String,
  cleanupDeploymentAfterCreating: Boolean,
  terraBucketReaderRole: String,
  terraBucketWriterRole: String,
  override val accessContextManagerDAO: AccessContextManagerDAO)(implicit val system: ActorSystem, val materializer: Materializer, implicit val executionContext: ExecutionContext, implicit val cs: ContextShift[IO], implicit val timer: Timer[IO]) extends GoogleServicesDAO(groupsPrefix) with FutureSupport with GoogleUtilities {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()
  implicit val log4CatsLogger: _root_.io.chrisdavenport.log4cats.Logger[IO] = Slf4jLogger.getLogger[IO]

  val groupMemberRole = "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)

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

  //we only have to do this once, because there's only one DM project
  lazy val getDeploymentManagerSAEmail: Future[String] = {
    getGoogleProject(RawlsBillingProjectName(deploymentMgrProject))
      .map( p => s"${p.getProjectNumber}@cloudservices.gserviceaccount.com")
  }

  initBuckets()

  protected def initBuckets(): Unit = {
    implicit val service = GoogleInstrumentedService.Storage
    val bucketAcls = List(new BucketAccessControl().setEntity("user-" + clientEmail).setRole("OWNER"))
    val defaultObjectAcls = List(new ObjectAccessControl().setEntity("user-" + clientEmail).setRole("OWNER"))

    try {
      getStorage(getBucketServiceAccountCredential).buckets().get(tokenBucketName).executeUsingHead()
    } catch {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue =>
        val logBucket = new Bucket().
          setName(tokenBucketName + "-logs")
        val logInserter = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, logBucket)
        executeGoogleRequest(logInserter)
        allowGoogleCloudStorageWrite(logBucket.getName)

        val tokenBucket = new Bucket().
          setName(tokenBucketName).
          setAcl(bucketAcls.asJava).
          setDefaultObjectAcl(defaultObjectAcls.asJava).
          setLogging(new Logging().setLogBucket(logBucket.getName))
        val insertTokenBucket = getStorage(getBucketServiceAccountCredential).buckets.insert(serviceProject, tokenBucket)
        executeGoogleRequest(insertTokenBucket)
    }

    val lifecyleRule = new LifecycleRule(
      LifecycleAction.newDeleteAction(), LifecycleRule.LifecycleCondition.newBuilder().setAge(30).build())

    val result = for{
      traceId <- Stream.eval(IO(TraceId(UUID.randomUUID())))
      _ <- googleStorageService.insertBucket(GoogleProject(serviceProject), hammCromwellMetadata.bucketName, None, Map.empty, Some(traceId))
      _ <- googleStorageService.setIamPolicy(hammCromwellMetadata.bucketName, Map(StorageRole.StorageAdmin -> NonEmptyList.of(Identity.serviceAccount(clientEmail))), Some(traceId))
      _ <- googleStorageService.setBucketLifecycle(hammCromwellMetadata.bucketName, List(lifecyleRule), Some(traceId))
      projectServiceAccount <- Stream.eval(googleServiceHttp.getProjectServiceAccount(GoogleProject(serviceProject), Some(traceId)))
      _ <- topicAdmin.createWithPublisherMembers(hammCromwellMetadata.topicName, List(projectServiceAccount), Some(traceId))
      _ <- Stream.eval(googleServiceHttp.createNotification(hammCromwellMetadata.topicName, hammCromwellMetadata.bucketName, Filters(List(NotificationEventTypes.ObjectFinalize), None), Some(traceId)))
    } yield ()
    // unsafeRunSync is not desired, but return type for initBuckets dictates execution has to happen immediately when the method is called.
    // Changing initBuckets's signature requires larger effort which doesn't seem to worth it
    result.compile.drain.unsafeRunSync()
  }

  def allowGoogleCloudStorageWrite(bucketName: String): Unit = {
    implicit val service = GoogleInstrumentedService.Storage
    // add cloud-storage-analytics@google.com as a writer so it can write logs
    // do it as a separate call so bucket gets default permissions plus this one
    val storage = getStorage(getBucketServiceAccountCredential)
    val bac = new BucketAccessControl().setEntity("group-cloud-storage-analytics@google.com").setRole("WRITER")
    executeGoogleRequest(storage.bucketAccessControls.insert(bucketName, bac))
  }

  override def setupWorkspace(userInfo: UserInfo, projectName: RawlsBillingProjectName, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail], bucketName: String, labels: Map[String, String], parentSpan: Span = null): Future[GoogleWorkspaceInfo] = {
    def updateBucketIam(policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail]): Stream[IO, Unit] = {
      //default object ACLs are no longer used. bucket only policy is enabled on buckets to ensure that objects
      //do not have separate permissions that deviate from the bucket-level permissions.
      //
      // project owner - organizations/$ORG_ID/roles/terraBucketWriter
      // workspace owner - organizations/$ORG_ID/roles/terraBucketWriter
      // workspace writer - organizations/$ORG_ID/roles/terraBucketWriter
      // workspace reader - organizations/$ORG_ID/roles/terraBucketReader
      // bucket service account - organizations/$ORG_ID/roles/terraBucketWriter + roles/storage.admin

      val customTerraBucketReaderRole = StorageRole.CustomStorageRole(terraBucketReaderRole)
      val customTerraBucketWriterRole = StorageRole.CustomStorageRole(terraBucketWriterRole)

      val workspaceAccessToStorageRole: Map[WorkspaceAccessLevel, StorageRole] = Map(ProjectOwner -> customTerraBucketWriterRole, Owner -> customTerraBucketWriterRole, Write -> customTerraBucketWriterRole, Read -> customTerraBucketReaderRole)
      val bucketRoles =
        policyGroupsByAccessLevel.map { case (access, policyEmail) => Identity.group(policyEmail.value) -> workspaceAccessToStorageRole(access) } +
          (Identity.serviceAccount(clientEmail) -> StorageRole.StorageAdmin)

      val roleToIdentities = bucketRoles.groupBy(_._2).mapValues(_.keys).collect { case (role, identities) if identities.nonEmpty => role -> NonEmptyList.fromListUnsafe(identities.toList) }

      for {
        // it takes some time for a newly created google group to percolate through the system, if it doesn't fully
        // exist yet the set iam call will return a 400 error, we need to explicitly retry that in addition to the usual
        _ <- googleStorageService.setIamPolicy(GcsBucketName(bucketName), roleToIdentities,
          retryConfig = RetryPredicates.retryConfigWithPredicates(RetryPredicates.standardRetryPredicate, RetryPredicates.whenStatusCode(400)))
      } yield ()
    }

    def insertInitialStorageLog(bucketName: String): Future[Unit] = {
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
          val objectInserter = getStorage(getBucketServiceAccountCredential).objects().insert(getStorageLogsBucketName(projectName), storageObject, stream)
          executeGoogleRequest(objectInserter)
        }
      }
    }

    // setupWorkspace main logic
    val traceId = TraceId(UUID.randomUUID())

    for {
      _ <- traceWithParent("insertBucket", parentSpan)(_ => googleStorageService.insertBucket(GoogleProject(projectName.value), GcsBucketName(bucketName), None, labels, Option(traceId), true, Option(GcsBucketName(getStorageLogsBucketName(projectName)))).compile.drain.unsafeToFuture()) //ACL = None because bucket IAM will be set separately in updateBucketIam
      updateBucketIamFuture = traceWithParent("updateBucketIam", parentSpan)(_ => updateBucketIam(policyGroupsByAccessLevel).compile.drain.unsafeToFuture())
      insertInitialStorageLogFuture = traceWithParent("insertInitialStorageLog", parentSpan)(_ => insertInitialStorageLog(bucketName))
      _ <- updateBucketIamFuture
      _ <- insertInitialStorageLogFuture
    } yield GoogleWorkspaceInfo(bucketName, policyGroupsByAccessLevel)
  }

  def createCromwellAuthBucket(billingProject: RawlsBillingProjectName, projectNumber: Long, authBucketReaders: Set[WorkbenchEmail]): Future[String] = {
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

        val bucket = new Bucket().setName(bucketName).setAcl(bucketAcls.asJava).setDefaultObjectAcl(defaultObjectAcls.asJava)
        val inserter = getStorage(getBucketServiceAccountCredential).buckets.insert(billingProject.value, bucket)
        executeGoogleRequest(inserter)

        bucketName
      }) { case t: HttpResponseException if t.getStatusCode == 409 => bucketName }
  }

  def grantReadAccess(billingProject: RawlsBillingProjectName,
                      bucketName: String,
                      authBucketReaders: Set[WorkbenchEmail]): Future[String] = {
    implicit val service = GoogleInstrumentedService.Storage

    def insertNewAcls() = for {
      readerEmail <- authBucketReaders

      bucketAcls = newBucketAccessControl(makeGroupEntityString(readerEmail.value), "READER")
      defaultObjectAcls = newObjectAccessControl(makeGroupEntityString(readerEmail.value), "READER")

      inserters = List(
        getStorage(getBucketServiceAccountCredential).bucketAccessControls.insert(bucketName, bucketAcls),
        getStorage(getBucketServiceAccountCredential).defaultObjectAccessControls.insert(bucketName, defaultObjectAcls))

       _ <- inserters.map(inserter => executeGoogleRequest(inserter))
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
      bucket.setLifecycle(new Lifecycle().setRule(List(storageLogExpiration).asJava))
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
    //Google doesn't let you delete buckets that are full.
    //You can either remove all the objects manually, or you can set up lifecycle management on the bucket.
    //This can be used to auto-delete all objects next time the Google lifecycle manager runs (~every 24h).
    //We set the lifecycle before we even try to delete the bucket so that if Google 500s trying to delete a bucket
    //with too many files, we've still made progress by setting the lifecycle to delete the objects to help us clean up
    //the bucket on another try. See CA-632.
    //More info: http://bit.ly/1WCYhhf
    val deleteEverythingRule = new Lifecycle.Rule()
      .setAction(new Action().setType("Delete"))
      .setCondition(new Condition().setAge(0))
    val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule).asJava)
    val patcher = buckets.patch(bucketName, new Bucket().setLifecycle(lifecycle))
    retryWhen500orGoogleError(() => { executeGoogleRequest(patcher) })

    // Now attempt to delete the bucket. If there were still objects in the bucket, we expect this to fail as the
    // lifecycle manager has probably not run yet.
    val deleter = buckets.delete(bucketName)
    retryWithRecoverWhen500orGoogleError(() => {
      executeGoogleRequest(deleter)
      true
    }) {
      //Google returns 409 Conflict if the bucket isn't empty.
      case t: HttpResponseException if t.getStatusCode == 409 =>
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
          usageFromLogObject(items.asScala.last)
      }
    }

    recurse()
  }

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = {
    implicit val service = GoogleInstrumentedService.Storage
    val aclGetter = getStorage(getBucketServiceAccountCredential).bucketAccessControls().list(bucketName)
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(aclGetter).getItems.asScala.toList) }) {
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

  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val deleter = getGroupDirectory.members.delete(groupEmail, emailToRemove)
    retryWithRecoverWhen500orGoogleError[Unit](() => { executeGoogleRequest(deleter) }) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => () // it is ok of the email is already missing
    }
  }

  override def copyFile(sourceBucket: String, sourceObject: String, destinationBucket: String, destinationObject: String): Future[Option[StorageObject]] = {
    implicit val service = GoogleInstrumentedService.Storage

    val copier = getStorage(getBucketServiceAccountCredential).objects.copy(sourceBucket, sourceObject, destinationBucket, destinationObject, new StorageObject())
    retryWithRecoverWhen500orGoogleError(() => { Option(executeGoogleRequest(copier)) }) {
      case e: HttpResponseException => {
        logger.warn(s"encountered error [${e.getStatusMessage}] with status code [${e.getStatusCode}] when copying [$sourceBucket/$sourceObject] to [$destinationBucket]")
        None
      }
    }
  }

  override def storeCromwellMetadata(objectName: GcsBlobName, body: fs2.Stream[fs2.Pure, Byte]): Stream[IO, Unit] = {
    val gzipped = (body through fs2.compress.gzip[fs2.Pure](2048)).compile.toList.toArray //after fs2 1.0.4, we can `to[Array]` directly
    googleStorageService.storeObject(hammCromwellMetadata.bucketName, objectName, gzipped, "text/plain")
  }

  override def listObjectsWithPrefix(bucketName: String, objectNamePrefix: String): Future[List[StorageObject]] = {
    implicit val service = GoogleInstrumentedService.Storage
    val getter = getStorage(getBucketServiceAccountCredential).objects().list(bucketName).setPrefix(objectNamePrefix).setMaxResults(maxPageSize.toLong)

    listObjectsRecursive(getter) map { pagesOption =>
      pagesOption.map { pages =>
        pages.flatMap { page =>
          Option(page.getItems) match {
            case None => List.empty
            case Some(objects) => objects.asScala.toList
          }
        }
      }.getOrElse(List.empty)
    }
  }

  private def listObjectsRecursive(fetcher: Storage#Objects#List, accumulated: Option[List[Objects]] = Some(Nil)): Future[Option[List[Objects]]] = {
    implicit val service = GoogleInstrumentedService.Storage

    accumulated match {
      // when accumulated has a Nil list then this must be the first request
      case Some(Nil) => retryWithRecoverWhen500orGoogleError(() => {
        Option(executeGoogleRequest(fetcher))
      }) {
        case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
      }.flatMap(firstPage => listObjectsRecursive(fetcher, firstPage.map(List(_))))

      // the head is the Objects object of the prior request which contains next page token
      case Some(head :: _) if head.getNextPageToken != null => retryWhen500orGoogleError(() => {
        executeGoogleRequest(fetcher.setPageToken(head.getNextPageToken))
      }).flatMap(nextPage => listObjectsRecursive(fetcher, accumulated.map(pages => nextPage :: pages)))

      // when accumulated is None (bucket does not exist) or next page token is null
      case _ => Future.successful(accumulated)
    }
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
    // we could switch back to HEAD requests by adding a call to httpResponse.discardEntityBytes().
    val bucketUrl = s"https://$bucketName.storage.googleapis.com/?storageClass"
    val bucketRequest = httpClientUtils.addHeader(RequestBuilding.Get(bucketUrl), Authorization(userInfo.accessToken))

    httpClientUtils.executeRequest(http, bucketRequest) map { httpResponse =>
      logger.info(s"diagnosticBucketRead to $bucketName returned ${httpResponse.status.intValue} " +
        s"as user ${userInfo.userEmail.value}, subjectid ${userInfo.userSubjectId.value}, with token hash ${userInfo.accessToken.token.hashCode} " +
        s"and response entity ${Unmarshal(httpResponse.entity).to[String]}")
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

  protected def testDMBillingAccountAccess(billingAccountId: String): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.IamCredentials

    /* Because we can't assume the identity of the Google SA that actually does the work in DM (it's in their project and we can't access it),
       we've added billingprobe@terra-deployments-{env} to the Google Group we ask our users to add to their billing accounts.
       In order to test that users have set up their billing accounts correctly, Rawls creates a token as the billingprobe@ SA and then
       calls testIamPermissions as it to see if it has the scopes required to associate projects with billing accounts.
       If it does, the group was correctly added, and the Google DM SA will be fine.
       (This function would incorrectly return true if users were to add billingprobe@ to their project and not the group or DM SA, but we
       don't publicise the existence of that account and it's not the thing we ask them to do, so the probability is near-zero.)

       In order for all of this to work, Rawls needs iam.serviceAccountTokenCreator on either the terra-deployments-{env} project
       or the billingprobe@ SA itself. We could also generate keys for the billingprobe@ SA and put them in Vault, but doing that and then
       intermittently refreshing them is a chore.
     */
    //First, get an access token to act as the Google APIs Service Agent that Deployment Manager runs as.
    val tokenRequestBody = new GenerateAccessTokenRequest().setScope(List(ComputeScopes.CLOUD_PLATFORM).asJava)
    val saResourceName = s"projects/-/serviceAccounts/$billingProbeEmail" //the dash is required; a project name will not work. https://bit.ly/2EXrXnj
    val accessTokenRequest = getIAMCredentials(getDeploymentManagerAccountCredential).projects().serviceAccounts().generateAccessToken(saResourceName, tokenRequestBody)

    val BILLING_ACCOUNT_PERMISSION = "billing.resourceAssociations.create"

    for {
      tokenResponse <- retryWhen500orGoogleError(() => {
                        blocking {
                          executeGoogleRequest (accessTokenRequest)
                        }})

      //Now we've got an access token, test IAM permissions to see if the SA has permission to create projects.
      probeSACredential = buildCredentialFromAccessToken(tokenResponse.getAccessToken, billingProbeEmail)
      testPermissionsBody = new TestIamPermissionsRequest().setPermissions(List(BILLING_ACCOUNT_PERMISSION).asJava)
      testPermissionsRequest = getCloudBillingManager(probeSACredential).billingAccounts().testIamPermissions(billingAccountId, testPermissionsBody)

      permissionResponse <- retryWhen500orGoogleError(() => {
                              blocking {
                                executeGoogleRequest(testPermissionsRequest)
                              }})
    } yield {
      Option(permissionResponse.getPermissions).exists(_.asScala.contains(BILLING_ACCOUNT_PERMISSION))
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
      Option(list.getBillingAccounts.asScala).map(_.toSeq).getOrElse(Seq.empty)
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
    import cats.implicits._

    val cred = getUserCredential(userInfo)
    listBillingAccounts(cred) flatMap { accountList =>
      //some users have TONS of billing accounts, enough to hit quota limits.
      //break the list of billing accounts up into chunks.
      //each chunk executes all its requests in parallel. the chunks themselves are processed serially.
      //this limits the amount of parallelism (to 10 inflight requests at a time), which should should slow
      //our rate of making requests. if this fails to be enough, we may need to upgrade this to an explicit throttle.
      val accountChunks: List[Seq[BillingAccount]] = accountList.grouped(10).toList

      //Iterate over each chunk.
      val allProcessedChunks: IO[List[Seq[RawlsBillingAccount]]] = accountChunks traverse { chunk =>

        //Run all tests in the chunk in parallel.
        IO.fromFuture(IO(Future.traverse(chunk){ acct =>
          val acctName = acct.getName
          testDMBillingAccountAccess(acctName) map { firecloudHasAccount =>
            RawlsBillingAccount(RawlsBillingAccountName(acctName), firecloudHasAccount, acct.getDisplayName)
          }
        }))
      }
      allProcessedChunks.map(_.flatten).unsafeToFuture()
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
      so.setMetadata(Map("iv" -> encryptedToken.base64Iv).asJava)
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

  override def getGenomicsOperation(opId: String): Future[Option[JsObject]] = {
    implicit val service = GoogleInstrumentedService.Genomics
    val genomicsServiceAccountCredential = getGenomicsServiceAccountCredential
    genomicsServiceAccountCredential.refreshToken()

    if (opId.startsWith("operations")) {
      // PAPIv1 ids start with "operations". We have to use a direct http call instead of a client library because
      // the client lib does not support PAPIv1 and PAPIv2 concurrently.
      new GenomicsV1DAO().getOperation(opId, OAuth2BearerToken(genomicsServiceAccountCredential.getAccessToken))
    } else {
      val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, genomicsServiceAccountCredential).setApplicationName(appName).build()
      val operationRequest = genomicsApi.projects().operations().get(opId)

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
  }

  override def checkGenomicsOperationsHealth(implicit executionContext: ExecutionContext): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Genomics
    val opId = s"projects/$serviceProject/operations"
    val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, getGenomicsServiceAccountCredential).setApplicationName(appName).build()
    val operationRequest = genomicsApi.projects().operations().list(opId).setPageSize(1)
    retryWhen500orGoogleError(() => {
      executeGoogleRequest(operationRequest)
      true
    })
  }

  override def getGoogleProject(projectName: RawlsBillingProjectName): Future[Project] = {
    implicit val service = GoogleInstrumentedService.Billing
    val credential = getDeploymentManagerAccountCredential

    val cloudResManager = getCloudResourceManager(credential)

    retryWhen500orGoogleError(() => {
      executeGoogleRequest(cloudResManager.projects().get(projectName.value))
    })
  }

  def getDMConfigYamlString(projectName: RawlsBillingProjectName, dmTemplatePath: String, properties: Map[String, JsValue]): String = {
    import DeploymentManagerJsonSupport._
    import cats.syntax.either._
    import io.circe.yaml.syntax._

    val configContents = ConfigContents(Seq(Resources(projectName.value, dmTemplatePath, properties)))
    val jsonVersion = io.circe.jawn.parse(configContents.toJson.toString).valueOr(throw _)
    jsonVersion.asYaml.spaces2
  }

  /*
   * Set the deployment policy to "abandon" -- i.e. allows the created project to persist even if the deployment is deleted --
   * and then delete the deployment. There's a limit of 1000 deployments so this is important to do.
   */
  override def cleanupDMProject(projectName: RawlsBillingProjectName): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.DeploymentManager
    val credential = getDeploymentManagerAccountCredential
    val deploymentManager = getDeploymentManager(credential)

    if( cleanupDeploymentAfterCreating ) {
      executeGoogleRequestWithRetry(
        deploymentManager.deployments().delete(deploymentMgrProject, projectToDM(projectName)).setDeletePolicy("ABANDON")).void
    } else {
      Future.successful(())
    }
  }

  def projectToDM(projectName: RawlsBillingProjectName) = s"dm-${projectName.value}"


  def parseTemplateLocation(path: String): Option[TemplateLocation] = {
    val rx: Regex = "https://raw.githubusercontent.com/(.*)/(.*)/(.*)/(.*)".r
    rx.findAllMatchIn(path).toList.headOption map { groups =>
      TemplateLocation(
        labelSafeString(groups.subgroups(0), ""),
        labelSafeString(groups.subgroups(1), ""),
        labelSafeString(groups.subgroups(2), ""),
        labelSafeString(groups.subgroups(3), ""))
    }
  }

  override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount, dmTemplatePath: String, highSecurityNetwork: Boolean, enableFlowLogs: Boolean, privateIpGoogleAccess: Boolean, requesterPaysRole: String, ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, projectTemplate: ProjectTemplate, parentFolderId: Option[String]): Future[RawlsBillingProjectOperationRecord] = {
    implicit val service = GoogleInstrumentedService.DeploymentManager
    val credential = getDeploymentManagerAccountCredential
    val deploymentManager = getDeploymentManager(credential)

    import spray.json._
    import spray.json.DefaultJsonProtocol._
    import DeploymentManagerJsonSupport._

    val templateLabels = parseTemplateLocation(dmTemplatePath).map(_.toJson).getOrElse(Map("template_path" -> labelSafeString(dmTemplatePath)).toJson)

    val properties = Map (
      "billingAccountId" -> billingAccount.accountName.value.toJson,
      "billingAccountFriendlyName" -> billingAccount.displayName.toJson,
      "projectId" -> projectName.value.toJson,
      "parentOrganization" -> orgID.toJson,
      "fcBillingGroup" -> billingGroupEmail.toJson,
      "projectOwnersGroup" -> ownerGroupEmail.value.toJson,
      "projectViewersGroup" -> computeUserGroupEmail.value.toJson,
      "requesterPaysRole" -> requesterPaysRole.toJson,
      "highSecurityNetwork" -> highSecurityNetwork.toJson,
      "enableFlowLogs" -> enableFlowLogs.toJson,
      "privateIpGoogleAccess" -> privateIpGoogleAccess.toJson,
      "fcProjectOwners" -> projectTemplate.owners.toJson,
      "fcProjectEditors" -> projectTemplate.editors.toJson,
      "labels" -> templateLabels
    ) ++ parentFolderId.map("parentFolder" -> folderNumberOnly(_).toJson).toMap

    //a list of one resource: type=composite-type, name=whocares, properties=pokein
    val yamlConfig = new ConfigFile().setContent(getDMConfigYamlString(projectName, dmTemplatePath, properties))
    val deploymentConfig = new TargetConfiguration().setConfig(yamlConfig)

    retryWhen500orGoogleError(() => {
      executeGoogleRequest {
        deploymentManager.deployments().insert(deploymentMgrProject, new Deployment().setName(projectToDM(projectName)).setTarget(deploymentConfig))
      }
    }) map { googleOperation =>
      val errorStr = Option(googleOperation.getError).map(errors => errors.getErrors.asScala.map(e => toErrorMessage(e.getMessage, e.getCode)).mkString("\n"))
      RawlsBillingProjectOperationRecord(projectName.value, GoogleOperationNames.DeploymentManagerCreateProject, googleOperation.getName, false, errorStr, GoogleApiTypes.DeploymentManagerApi)
    }
  }

  /**
    * Google is not consistent when dealing with folder ids. Some apis do not want the folder id to start with
    * "folders/" but other apis return that or expect that. This function strips the prefix if it exists.
    *
    * @param folderId
    * @return
    */
  private def folderNumberOnly(folderId: String) = folderId.stripPrefix("folders/")

  override def pollOperation(operationId: OperationId): Future[OperationStatus] = {
    val dmCredential = getDeploymentManagerAccountCredential

    // this code is a colossal DRY violation but because the operations collection is different
    // for cloudResManager and servicesManager and they return different but identical Status objects
    // there is not much else to be done... too bad scala does not have duck typing.
    operationId.apiType match {
      case GoogleApiTypes.DeploymentManagerApi =>
        val deploymentManager = getDeploymentManager(dmCredential)
        implicit val service = GoogleInstrumentedService.DeploymentManager

        retryWhen500orGoogleError(() => {
          executeGoogleRequest(deploymentManager.operations().get(deploymentMgrProject, operationId.operationId))
        }).map { op =>
          val errorStr = Option(op.getError).map(errors => errors.getErrors.asScala.map(e => toErrorMessage(e.getMessage, e.getCode)).mkString("\n"))
          OperationStatus(op.getStatus == "DONE", errorStr)
        }

      case GoogleApiTypes.AccessContextManagerApi =>
        accessContextManagerDAO.pollOperation(operationId.operationId).map { op =>
          OperationStatus(toScalaBool(op.getDone), Option(op.getError).map(error => toErrorMessage(error.getMessage, error.getCode)))
        }
    }
  }

  /**
   * converts a possibly null java boolean to a scala boolean, null is treated as false
   */
  private def toScalaBool(b: java.lang.Boolean) = Option(b).contains(java.lang.Boolean.TRUE)

  private def toErrorMessage(message: String, code: String): String = {
    s"${Option(message).getOrElse("")} - code ${code}"
  }

  private def toErrorMessage(message: String, code: Int): String = {
    s"${Option(message).getOrElse("")} - code ${code}"
  }

  /**
    * Updates policy bindings on a google project.
    * 1) get existing policies
    * 2) call updatePolicies
    * 3) if updated policies are the same as existing policies return false, don't call google
    * 4) if updated policies are different than existing policies update google and return true
    *
    * @param projectName google project name
    * @param updatePolicies function (existingPolicies => updatedPolicies). May return policies with no members
    *                       which will be handled appropriately when sent to google.
    * @return true if google was called to update policies, false otherwise
    */
  override protected def updatePolicyBindings(projectName: RawlsBillingProjectName)(updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]): Future[Boolean] = {
    val cloudResManager = getCloudResourceManager(getBillingServiceAccountCredential)
    implicit val service = GoogleInstrumentedService.CloudResourceManager

    for {
      updated <- retryWhen500orGoogleError(() => {
        // it is important that we call getIamPolicy within the same retry block as we call setIamPolicy
        // getIamPolicy gets the etag that is used in setIamPolicy, the etag is used to detect concurrent
        // modifications and if that happens we need to be sure to get a new etag before retrying setIamPolicy
        val existingPolicy = executeGoogleRequest(cloudResManager.projects().getIamPolicy(projectName.value, null))
        val existingPolicies: Map[String, Set[String]] = existingPolicy.getBindings.asScala.map { policy => policy.getRole -> policy.getMembers.asScala.toSet }.toMap

        val updatedPolicies = updatePolicies(existingPolicies)

        if (updatedPolicies.equals(existingPolicies)) {
          false
        } else {
          val updatedBindings = updatedPolicies.collect { case (role, members) if members.nonEmpty => // exclude policies with empty members
            new Binding().setRole(role).setMembers(members.toList.asJava)
          }.toSeq

          // when setting IAM policies, always reuse the existing policy so the etag is preserved.
          val policyRequest = new SetIamPolicyRequest().setPolicy(existingPolicy.setBindings(updatedBindings.asJava))
          executeGoogleRequest(cloudResManager.projects().setIamPolicy(projectName.value, policyRequest))
          true
        }
      })

    } yield updated
  }

  override def addRoleToGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Boolean] = {
    addPolicyBindings(projectName, Map(s"roles/$role" -> Set(s"group:${groupEmail.value}")))
  }

  override def removeRoleFromGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Boolean] = {
    removePolicyBindings(projectName, Map(s"roles/$role" -> Set(s"group:${groupEmail.value}")))
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
      _ <- retryWithRecoverWhen500orGoogleError(() => {
        executeGoogleRequest(resMgr.projects().delete(projectNameString))
      }) {
        case e: GoogleJsonResponseException if e.getDetails.getCode == 403 && "Cannot delete an inactive project.".equals(e.getDetails.getMessage) => new Empty()
          // stop trying to delete an already deleted project
      }
    } yield {
      // nothing
    }
  }

  def projectUsageExportBucketName(projectName: RawlsBillingProjectName) = s"${projectName.value}-usage-export"

  override def getBucketDetails(bucketName: String, project: RawlsBillingProjectName): Future[WorkspaceBucketOptions] = {
    implicit val service = GoogleInstrumentedService.Storage
    val cloudStorage = getStorage(getBucketServiceAccountCredential)
    for {
      bucketDetails <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(cloudStorage.buckets().get(bucketName).setUserProject(project.value))
      })
    } yield {
      val requesterPays = for {
        billing <- Option(bucketDetails.getBilling)
        rp <- Option(billing.getRequesterPays)
      } yield rp.booleanValue()

      WorkspaceBucketOptions(
        requesterPays = requesterPays.getOrElse(false)
      )
    }
  }

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

  def getIAM(credential: Credential): Iam = {
    new Iam.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getIAMCredentials(credential: Credential): IAMCredentials = {
    new IAMCredentials.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getDeploymentManager(credential: Credential): DeploymentManagerV2Beta = {
    new DeploymentManagerV2Beta.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getStorage(credential: Credential) = {
    new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def getGroupDirectory = {
    new Directory.Builder(httpTransport, jsonFactory, getGroupServiceAccountCredential).setApplicationName(appName).build()
  }

  private def getUserCredential(userInfo: UserInfo): Credential = {
    new GoogleCredential().setAccessToken(userInfo.accessToken.token).setExpiresInSeconds(userInfo.accessTokenExpiresIn)
  }

  private def getGroupServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(directoryScopes.asJava)
      .setServiceAccountUser(subEmail)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getBucketServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(storageScopes.asJava) // grant bucket-creation powers
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getGenomicsServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(genomicsScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getDeploymentManagerAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(Seq(ComputeScopes.CLOUD_PLATFORM).asJavaCollection)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getBillingServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountScopes(Seq(ComputeScopes.CLOUD_PLATFORM).asJava) // need this broad scope to create/manage projects
      .setServiceAccountId(billingPemEmail)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(billingPemFile))
      .setServiceAccountUser(billingEmail)
      .build()
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

  private def buildCredentialFromAccessToken(accessToken: String, credentialEmail: String): GoogleCredential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .build().setAccessToken(accessToken)
  }

  def getAccessTokenUsingJson(saKey: String) : Future[String] = {
    implicit val service = GoogleInstrumentedService.OAuth
    retryWhen500orGoogleError(() => {
      val keyStream = new ByteArrayInputStream(saKey.getBytes)
      val credential = ServiceAccountCredentials.fromStream(keyStream).createScoped(storageScopes.asJava)
      credential.refreshAccessToken.getTokenValue
    })
  }

  def getUserInfoUsingJson(saKey: String): Future[UserInfo] = {
    implicit val service = GoogleInstrumentedService.OAuth
    retryWhen500orGoogleError(() => {
      val keyStream = new ByteArrayInputStream(saKey.getBytes)
      val credential = ServiceAccountCredentials.fromStream(keyStream).createScoped(storageScopes.asJava)
      UserInfo.buildFromTokens(credential)
    })
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

  override def addProjectToFolder(projectName: RawlsBillingProjectName, folderId: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.CloudResourceManager
    val cloudResourceManager = getCloudResourceManager(getBillingServiceAccountCredential)

    retryWhen500orGoogleError( () => {
      val existingProject = executeGoogleRequest(cloudResourceManager.projects().get(projectName.value))

      val folderResourceId = new ResourceId().setType(GoogleResourceTypes.Folder.value).setId(folderNumberOnly(folderId))
      executeGoogleRequest(cloudResourceManager.projects().update(projectName.value, existingProject.setParent(folderResourceId)))
    })
  }

  override def getFolderId(folderName: String): Future[Option[String]] = {
    val credential = getBillingServiceAccountCredential
    credential.refreshToken()

    retryExponentially(when500orGoogleError)( () => {
      new CloudResourceManagerV2DAO().getFolderId(folderName, OAuth2BearerToken(credential.getAccessToken))
    })
  }
}

class GoogleStorageLogException(message: String) extends RawlsException(message)

class GenomicsV1DAO(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DsdeHttpDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getOperation(opId: String, accessToken: OAuth2BearerToken): Future[Option[JsObject]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import DefaultJsonProtocol._
    executeRequestWithToken[Option[JsObject]](accessToken)(RequestBuilding.Get(s"https://genomics.googleapis.com/v1alpha2/$opId"))
  }
}

/**
  * The cloud resource manager api is maddening. V2 is not a newer, better version of V1 but a completely different api.
  * V1 manages projects and organizations, V2 manages folders. But because they appear to be different versions of the
  * same thing and use the same class names we can't have both client libraries. So this v2 dao calls the folder
  * apis we need via direct http call.
  * @param system
  * @param materializer
  * @param executionContext
  */
class CloudResourceManagerV2DAO(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DsdeHttpDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getFolderId(folderName: String, accessToken: OAuth2BearerToken): Future[Option[String]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import DefaultJsonProtocol._

    implicit val FolderFormat = jsonFormat1(Folder)
    implicit val FolderSearchResponseFormat = jsonFormat1(FolderSearchResponse)

    executeRequestWithToken[FolderSearchResponse](accessToken)(RequestBuilding.Post(s"https://cloudresourcemanager.googleapis.com/v2/folders:search", Map("query" -> s"displayName=$folderName"))).map { response =>
      response.folders.flatMap { folders =>
        if (folders.size > 1) {
          throw new RawlsException(s"google folder search returned more than one folder with display name $folderName: $folders")
        } else {
          folders.headOption.map(_.name)
        }
      }
    }
  }
}

object CloudResourceManagerV2Model {
  case class Folder(name: String)
  case class FolderSearchResponse(folders: Option[Seq[Folder]])
}
