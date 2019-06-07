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
import com.google.api.services.cloudbilling.model.{BillingAccount, ProjectBillingInfo}
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.compute.model.UsageExportLocation
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.deploymentmanager.model.{ConfigFile, Deployment, ImportFile, TargetConfiguration}
import com.google.api.services.deploymentmanager.DeploymentManagerV2Beta
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
import com.google.cloud.Identity
import com.google.cloud.storage.BucketInfo.LifecycleRule
import com.google.cloud.storage.BucketInfo.LifecycleRule.LifecycleAction
import fs2.Stream
import io.grpc.Status.Code
import org.broadinstitute.dsde.rawls.crypto.{Aes256Cbc, EncryptedBytes, SecretKey}
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, GoogleUtilities, HttpGoogleAccessContextManagerDAO}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.joda.time
import spray.json._
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import com.google.api.services.accesscontextmanager.v1beta.{AccessContextManager, AccessContextManagerScopes}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, _}
import scala.io.Source
import scala.util.matching.Regex

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
  bucketLogsMaxAge: Int,
  maxPageSize: Int = 200,
  hammCromwellMetadata: HammCromwellMetadata,
  googleStorageService: GoogleStorageService[IO],
  googleServiceHttp: GoogleServiceHttp[IO],
  topicAdmin: GoogleTopicAdmin[IO],
  override val workbenchMetricBaseName: String,
  proxyNamePrefix: String,
  deploymentMgrProject: String,
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
      _ <- googleStorageService.createBucket(GoogleProject(serviceProject), hammCromwellMetadata.bucketName, None, Some(traceId))
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

  override def setupWorkspace(userInfo: UserInfo, project: RawlsBillingProject, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail], bucketName: String, labels: Map[String, String]): Future[GoogleWorkspaceInfo] = {

    def insertBucket: Map[WorkspaceAccessLevel, WorkbenchEmail] => Future[String] = { policyGroupsByAccessLevel =>
      implicit val service = GoogleInstrumentedService.Storage
      retryWhen500orGoogleError {
        () => {
          // bucket ACLs should be:
          //   project owner - bucket writer
          //   workspace owner - bucket writer
          //   workspace writer - bucket writer
          //   workspace reader - bucket reader
          //   bucket service account - bucket owner
          val workspaceAccessToBucketAcl: Map[WorkspaceAccessLevel, String] = Map(ProjectOwner -> "WRITER", Owner -> "WRITER", Write -> "WRITER", Read -> "READER")
          val bucketAcls =
            policyGroupsByAccessLevel.map { case (access, policyEmail) => newBucketAccessControl(makeGroupEntityString(policyEmail.value), workspaceAccessToBucketAcl(access)) }.toSeq :+
              newBucketAccessControl("user-" + clientEmail, "OWNER")

          // default object ACLs should be:
          //   project owner - object reader
          //   workspace owner - object reader
          //   workspace writer - object reader
          //   workspace reader - object reader
          //   bucket service account - object owner
          val defaultObjectAcls =
          policyGroupsByAccessLevel.map { case (_, policyEmail) => newObjectAccessControl(makeGroupEntityString(policyEmail.value), "READER") }.toSeq :+
              newObjectAccessControl("user-" + clientEmail, "OWNER")

          val logging = new Logging().setLogBucket(getStorageLogsBucketName(project.projectName))

          val bucket = new Bucket().
            setName(bucketName).
            setAcl(bucketAcls.asJava).
            setDefaultObjectAcl(defaultObjectAcls.asJava).
            setLogging(logging).
            setLabels(labels.asJava)
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

    val bucketInsertion = for {
      bucketName <- insertBucket(policyGroupsByAccessLevel)
      _ <- insertInitialStorageLog(bucketName)
    } yield GoogleWorkspaceInfo(bucketName, policyGroupsByAccessLevel)

    bucketInsertion
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
        val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule).asJava)
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

  override def storeCromwellMetadata(objectName: GcsBlobName, body: fs2.Stream[fs2.Pure, Byte]): IO[Unit] = {
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

  protected def billingAccountContainsFCBillingGroup(credential: Credential, billingAccountId: String): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Billing
    val fetcher = getCloudBillingManager(credential).billingAccounts().getIamPolicy(billingAccountId)
    retryWhen500orGoogleError(() => {
      blocking {
        executeGoogleRequest(fetcher)
      }
    }) map { iamPolicy =>
      val billingUsersOpt = iamPolicy.getBindings.asScala.find(_.getRole == "roles/billing.user")
      billingUsersOpt.exists(_.getMembers.asScala.contains(s"group:$billingGroupEmail"))
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
    val cred = getUserCredential(userInfo)
    listBillingAccounts(cred) flatMap { accountList =>
      Future.sequence(accountList map { acct =>
        val acctName = acct.getName
        billingAccountContainsFCBillingGroup(cred, acctName) map { firecloudHasAccount =>
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
      list.getOperations.asScala
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

  def getDMConfigString(projectName: RawlsBillingProjectName, dmTemplatePath: String, properties: Map[String, JsValue]): String = {
    import DeploymentManagerJsonSupport._
    import cats.syntax.either._
    import io.circe.yaml._
    import io.circe.yaml.syntax._

    val cc = ConfigContents(Seq(Resources(projectName.value, dmTemplatePath, properties)))
    val jsonVersion = io.circe.jawn.parse(cc.toJson.toString).valueOr(throw _)
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

    executeGoogleRqWithRetry(
      deploymentManager.deployments().delete(deploymentMgrProject, projectToDM(projectName)).setDeletePolicy("ABANDON")).void
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

  override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount, dmTemplatePath: String, requesterPaysRole: String, ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, projectTemplate: ProjectTemplate): Future[RawlsBillingProjectOperationRecord] = {
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
      "highSecurityNetwork" -> false.toJson,
      "fcProjectOwners" -> projectTemplate.policies("roles/owner").toJson,
      "fcProjectEditors" -> projectTemplate.policies("roles/editor").toJson,
      "labels" -> templateLabels
    )

    //config is a list of one resource: type=composite-type, name=whocares, properties=pokein
    val confy = new ConfigFile().setContent(getDMConfigString(projectName, dmTemplatePath, properties))
    val dconf = new TargetConfiguration().setConfig(confy)

    retryWhen500orGoogleError(() => {
      executeGoogleRequest {
        deploymentManager.deployments().insert(deploymentMgrProject, new Deployment().setName(projectToDM(projectName)).setTarget(dconf))
      }
    }) map { googleOperation =>
      val errorStr = Option(googleOperation.getError).map(errors => errors.getErrors.asScala.map(e => toErrorMessage(e.getMessage, e.getCode)).mkString("\n"))
      RawlsBillingProjectOperationRecord(projectName.value, GoogleOperationNames.DeploymentManagerCreateProject, googleOperation.getName, false, errorStr, GoogleApiTypes.DeploymentManagerApi)
    }
  }

  override def pollOperation(operationId: OperationId): Future[OperationStatus] = {
    val credential = getBillingServiceAccountCredential
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

  private def removePolicyBindings(projectName: RawlsBillingProjectName, policiesToRemove: Map[String, Seq[String]]): Future[Unit] = {
    val cloudResManager = getCloudResourceManager(getBillingServiceAccountCredential)
    implicit val service = GoogleInstrumentedService.CloudResourceManager

    for {
      existingPolicy <- retryWhen500orGoogleError(() => {
        executeGoogleRequest(cloudResManager.projects().getIamPolicy(projectName.value, null))
      })

      _ <- retryWhen500orGoogleError(() => {
        val existingPolicies: Map[String, Seq[String]] = existingPolicy.getBindings.asScala.map { policy => policy.getRole -> policy.getMembers.asScala.toSeq }.toMap

        // this may result in keys with empty-seq values.  That's ok, we will ignore those later
        val updatedKeysWithRemovedPolicies: Map[String, Seq[String]] = policiesToRemove.keys.map { k =>
          val existingForKey = existingPolicies(k)
          val updatedForKey = existingForKey.toSet diff policiesToRemove(k).toSet
          k -> updatedForKey.toSeq
        }.toMap

        // Use standard Map ++ instead of semigroup because we want to replace the original values
        val newPolicies = existingPolicies ++ updatedKeysWithRemovedPolicies

        val updatedBindings = newPolicies.collect { case (role, members) if members.nonEmpty =>
          new Binding().setRole(role).setMembers(members.distinct.asJava)
        }.toSeq

        // when setting IAM policies, always reuse the existing policy so the etag is preserved.
        val policyRequest = new SetIamPolicyRequest().setPolicy(existingPolicy.setBindings(updatedBindings.asJava))
        executeGoogleRequest(cloudResManager.projects().setIamPolicy(projectName.value, policyRequest))
      })
    } yield ()
  }

  override def addPolicyBindings(projectName: RawlsBillingProjectName, policiesToAdd: Map[String, List[String]]): Future[Boolean] = {
    val cloudResManager = getCloudResourceManager(getBillingServiceAccountCredential)
    implicit val service = GoogleInstrumentedService.CloudResourceManager

    for {
      updated <- retryWhen500orGoogleError(() => {
        // it is important that we call getIamPolicy within the same retry block as we call setIamPolicy
        // getIamPolicy gets the etag that is used in setIamPolicy, the etag is used to detect concurrent
        // modifications and if that happens we need to be sure to get a new etag before retrying setIamPolicy
        val existingPolicy = executeGoogleRequest(cloudResManager.projects().getIamPolicy(projectName.value, null))
        val existingPolicies: Map[String, List[String]] = existingPolicy.getBindings.asScala.map { policy => policy.getRole -> policy.getMembers.asScala.toList }.toMap

        // |+| is a semigroup: it combines a map's keys by combining their values' members instead of replacing them
        import cats.implicits._
        val newPolicies = existingPolicies |+| policiesToAdd.filter(_._2.nonEmpty) // ignore empty lists

        if (newPolicies.equals(existingPolicies)) {
          false
        } else {

          val updatedBindings = newPolicies.collect { case (role, members) if members.nonEmpty =>
            new Binding().setRole(role).setMembers(members.distinct.asJava)
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
    addPolicyBindings(projectName, Map(s"roles/$role" -> List(s"group:${groupEmail.value}")))
  }

  override def removeRoleFromGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Unit] = {
    removePolicyBindings(projectName, Map(s"roles/$role" -> Seq(s"group:${groupEmail.value}")))
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

}

class GoogleStorageLogException(message: String) extends RawlsException(message)