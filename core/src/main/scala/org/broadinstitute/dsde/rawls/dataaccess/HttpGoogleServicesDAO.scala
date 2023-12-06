package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import cats.data.NonEmptyList
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Temporal}
import cats.syntax.all._
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer, HttpResponseException, InputStreamContent}
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.cloudbilling.Cloudbilling
import com.google.api.services.cloudbilling.model.{
  BillingAccount,
  ListBillingAccountsResponse,
  ProjectBillingInfo,
  TestIamPermissionsRequest
}
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.directory.model.{Group, Member}
import com.google.api.services.directory.{Directory, DirectoryScopes}
import com.google.api.services.genomics.v2alpha1.{Genomics, GenomicsScopes}
import com.google.api.services.iam.v1.Iam
import com.google.api.services.iamcredentials.v1.IAMCredentials
import com.google.api.services.lifesciences.v2beta.{CloudLifeSciences, CloudLifeSciencesScopes}
import com.google.api.services.oauth2.Oauth2.Builder
import com.google.api.services.storage.model.Bucket.Lifecycle
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.{Action, Condition}
import com.google.api.services.storage.model._
import com.google.api.services.storage.{Storage, StorageScopes}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.Identity
import com.google.cloud.storage.Storage.BucketSourceOption
import com.google.cloud.storage.{StorageClass, StorageException}
import io.opencensus.scala.Tracing._
import io.opencensus.trace.{AttributeValue, Span}
import org.apache.commons.lang3.StringUtils
import org.broadinstitute.dsde.rawls.dataaccess.CloudResourceManagerV2Model.{Folder, FolderSearchResponse}
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO._
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, GoogleUtilities}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented.GoogleCounters
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, HttpGoogleIamDAO}
import org.broadinstitute.dsde.workbench.google2._
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, GoogleResourceTypes, IamPermission}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.joda.time.DateTime
import org.typelevel.log4cats.slf4j.Slf4jLogger
import spray.json._

import java.io._
import java.util.UUID
import scala.collection.mutable
import scala.concurrent._
import scala.io.Source
import scala.jdk.CollectionConverters._

class HttpGoogleServicesDAO(val clientSecrets: GoogleClientSecrets,
                            clientEmail: String,
                            subEmail: String,
                            pemFile: String,
                            appsDomain: String,
                            groupsPrefix: String,
                            appName: String,
                            serviceProject: String,
                            billingPemEmail: String,
                            billingPemFile: String,
                            val billingEmail: String,
                            val billingGroupEmail: String,
                            maxPageSize: Int = 200,
                            googleStorageService: GoogleStorageService[IO],
                            override val workbenchMetricBaseName: String,
                            proxyNamePrefix: String,
                            terraBucketReaderRole: String,
                            terraBucketWriterRole: String,
                            override val accessContextManagerDAO: AccessContextManagerDAO,
                            resourceBufferJsonFile: String
)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  implicit val executionContext: ExecutionContext,
  implicit val timer: Temporal[IO]
) extends GoogleServicesDAO(groupsPrefix)
    with FutureSupport
    with GoogleUtilities {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()
  implicit val log4CatsLogger = Slf4jLogger.getLogger[IO]

  val groupMemberRole =
    "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)
  val cloudBillingInfoReadTimeout = 40 * 1000 // socket read timeout when updating billing info

  // modify these if we need more granular access in the future
  val workbenchLoginScopes =
    Seq("https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile")
  val storageScopes = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE) ++ workbenchLoginScopes
  val directoryScopes = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)
  val genomicsScopes = Seq(
    GenomicsScopes.GENOMICS
  ) // google requires GENOMICS, not just GENOMICS_READONLY, even though we're only doing reads
  val lifesciencesScopes = Seq(CloudLifeSciencesScopes.CLOUD_PLATFORM)
  val billingScopes = Seq("https://www.googleapis.com/auth/cloud-billing")

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = GsonFactory.getDefaultInstance
  val BILLING_ACCOUNT_PERMISSION = "billing.resourceAssociations.create"

  val SingleRegionLocationType: String = "region"

  val REQUESTER_PAYS_ERROR_SUBSTRINGS = Seq("requester pays", "UserProjectMissing")

  override def updateBucketIam(bucketName: GcsBucketName,
                               policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                               userProject: Option[GoogleProjectId],
                               iamPolicyVersion: Int = 1
  ): Future[Unit] = {
    // default object ACLs are no longer used. bucket only policy is enabled on buckets to ensure that objects
    // do not have separate permissions that deviate from the bucket-level permissions.
    //
    // project owner - organizations/$ORG_ID/roles/terraBucketWriter
    // workspace owner - organizations/$ORG_ID/roles/terraBucketWriter
    // workspace writer - organizations/$ORG_ID/roles/terraBucketWriter
    // workspace reader - organizations/$ORG_ID/roles/terraBucketReader
    // bucket service account - organizations/$ORG_ID/roles/terraBucketWriter + roles/storage.admin

    val customTerraBucketReaderRole = StorageRole.CustomStorageRole(terraBucketReaderRole)
    val customTerraBucketWriterRole = StorageRole.CustomStorageRole(terraBucketWriterRole)

    val workspaceAccessToStorageRole = Map(
      ProjectOwner -> customTerraBucketWriterRole,
      Owner -> customTerraBucketWriterRole,
      Write -> customTerraBucketWriterRole,
      Read -> customTerraBucketReaderRole
    )

    val roleToIdentities = policyGroupsByAccessLevel
      .map { case (access, policyEmail) => Identity.group(policyEmail.value) -> workspaceAccessToStorageRole(access) }
      .+(Identity.serviceAccount(clientEmail) -> StorageRole.StorageAdmin)
      .groupBy(_._2)
      .view
      .mapValues(_.keys)
      .collect {
        case (role, identities) if identities.nonEmpty => role -> NonEmptyList.fromListUnsafe(identities.toList)
      }

    // it takes some time for a newly created google group to percolate through the system, if it doesn't fully
    // exist yet the set iam call will return a 400 error, we need to explicitly retry that in addition to the usual

    // it can also take some time for Google to come to a consensus about a bucket's existence when it is newly created.
    // during this time, we may see intermittent 404s indicating that the bucket we just created doesn't exist. we need to
    // retry these 404s in this case.

    // Note that we explicitly override the IAM policy for this bucket with `roleToIdentities`.
    // We do this to ensure that all default bucket IAM is removed from the bucket and replaced entirely with what we want
    googleStorageService
      .overrideIamPolicy(
        bucketName,
        roleToIdentities.toMap,
        retryConfig = RetryPredicates.retryConfigWithPredicates(RetryPredicates.standardGoogleRetryPredicate,
                                                                RetryPredicates.whenStatusCode(400),
                                                                RetryPredicates.whenStatusCode(404)
        ),
        bucketSourceOptions = userProject.map(p => BucketSourceOption.userProject(p.value)).toList,
        version = iamPolicyVersion
      )
      .compile
      .drain
      .unsafeToFuture()
  }

  override def setupWorkspace(userInfo: UserInfo,
                              googleProject: GoogleProjectId,
                              policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                              bucketName: GcsBucketName,
                              labels: Map[String, String],
                              parentSpan: Span = null,
                              bucketLocation: Option[String]
  ): Future[GoogleWorkspaceInfo] = {
    def insertInitialStorageLog: Future[Unit] = {
      implicit val service = GoogleInstrumentedService.Storage
      retryWhen500orGoogleError { () =>
        // manually insert an initial storage log
        val stream: InputStreamContent =
          new InputStreamContent(
            "text/plain",
            new ByteArrayInputStream(s""""bucket","storage_byte_hours"
                                        |"$bucketName","0"
                                        |""".stripMargin.getBytes)
          )
        // use an object name that will always be superseded by a real storage log
        val storageObject = new StorageObject().setName(s"${bucketName}_storage_00_initial_log")
        val objectInserter = getStorage(getBucketServiceAccountCredential)
          .objects()
          .insert(GoogleServicesDAO.getStorageLogsBucketName(googleProject), storageObject, stream)
        executeGoogleRequest(objectInserter)
      }
    }

    // setupWorkspace main logic
    val traceId = TraceId(UUID.randomUUID())

    for {
      _ <- traceWithParent("insertBucket", parentSpan)(_ =>
        googleStorageService
          .insertBucket(
            googleProject = GoogleProject(googleProject.value),
            bucketName = bucketName,
            acl = None,
            labels = labels,
            traceId = Option(traceId),
            bucketPolicyOnlyEnabled = true,
            logBucket = Option(GcsBucketName(GoogleServicesDAO.getStorageLogsBucketName(googleProject))),
            location = bucketLocation,
            autoclassEnabled = true,
            autoclassTerminalStorageClass = Option(StorageClass.ARCHIVE)
          )
          .compile
          .drain
          .unsafeToFuture()
          .recoverWith {
            case e: StorageException if e.getCode == 400 =>
              val message =
                s"Workspace creation failed. Error trying to create bucket `$bucketName` in Google project " +
                  s"`${googleProject.value}` in region `${bucketLocation.getOrElse("US (default)")}`."
              val errorReport = ErrorReport(statusCode = StatusCodes.BadRequest, message)
              throw new RawlsExceptionWithErrorReport(errorReport)
          }
      ) // ACL = None because bucket IAM will be set separately in updateBucketIam
      updateBucketIamFuture = traceWithParent("updateBucketIam", parentSpan)(_ =>
        updateBucketIam(bucketName, policyGroupsByAccessLevel)
      )
      insertInitialStorageLogFuture = traceWithParent("insertInitialStorageLog", parentSpan)(_ =>
        insertInitialStorageLog
      )
      _ <- updateBucketIamFuture
      _ <- insertInitialStorageLogFuture
    } yield GoogleWorkspaceInfo(bucketName.value, policyGroupsByAccessLevel)
  }

  def grantReadAccess(bucketName: String, authBucketReaders: Set[WorkbenchEmail]): Future[String] = {
    implicit val service = GoogleInstrumentedService.Storage

    def insertNewAcls() = for {
      readerEmail <- authBucketReaders

      bucketAcls = newBucketAccessControl(makeGroupEntityString(readerEmail.value), "READER")
      defaultObjectAcls = newObjectAccessControl(makeGroupEntityString(readerEmail.value), "READER")

      inserters = List(
        getStorage(getBucketServiceAccountCredential).bucketAccessControls.insert(bucketName, bucketAcls),
        getStorage(getBucketServiceAccountCredential).defaultObjectAccessControls.insert(bucketName, defaultObjectAcls)
      )

      _ <- inserters.map(inserter => executeGoogleRequest(inserter))
    } yield ()

    retryWithRecoverWhen500orGoogleError { () =>
      insertNewAcls(); bucketName
    } {
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
    // Google doesn't let you delete buckets that are full.
    // You can either remove all the objects manually, or you can set up lifecycle management on the bucket.
    // This can be used to auto-delete all objects next time the Google lifecycle manager runs (~every 24h).
    // We set the lifecycle before we even try to delete the bucket so that if Google 500s trying to delete a bucket
    // with too many files, we've still made progress by setting the lifecycle to delete the objects to help us clean up
    // the bucket on another try. See CA-632.
    // More info: http://bit.ly/1WCYhhf
    val deleteEverythingRule = new Lifecycle.Rule()
      .setAction(new Action().setType("Delete"))
      .setCondition(new Condition().setAge(0))
    val lifecycle = new Lifecycle().setRule(List(deleteEverythingRule).asJava)
    val patcher = buckets.patch(bucketName, new Bucket().setLifecycle(lifecycle))
    retryWhen500orGoogleError(() => executeGoogleRequest(patcher))

    // Now attempt to delete the bucket. If there were still objects in the bucket, we expect this to fail as the
    // lifecycle manager has probably not run yet.
    val deleter = buckets.delete(bucketName)
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(deleter)
      true
    } {
      // Google returns 409 Conflict if the bucket isn't empty.
      case t: HttpResponseException if t.getStatusCode == 409 =>
        false
      // Bucket is already deleted
      case t: HttpResponseException if t.getStatusCode == 404 =>
        true
    }
  }

  override def isAdmin(userEmail: String): Future[Boolean] =
    hasGoogleRole(adminGroupName, userEmail)

  override def isLibraryCurator(userEmail: String): Future[Boolean] =
    hasGoogleRole(curatorGroupName, userEmail)

  override def addLibraryCurator(userEmail: String): Future[Unit] =
    addEmailToGoogleGroup(curatorGroupName, userEmail)

  override def removeLibraryCurator(userEmail: String): Future[Unit] =
    removeEmailFromGoogleGroup(curatorGroupName, userEmail)

  override def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Groups
    val query = getGroupDirectory.members.get(roleGroupName, userEmail)
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(query)
      true
    } {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => false
    }
  }

  override def getGoogleGroup(groupName: String)(implicit executionContext: ExecutionContext): Future[Option[Group]] = {
    implicit val service = GoogleInstrumentedService.Groups
    val getter = getGroupDirectory.groups().get(groupName)
    retryWithRecoverWhen500orGoogleError(() => Option(executeGoogleRequest(getter))) {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }
  }

  override def getBucketUsage(googleProject: GoogleProjectId,
                              bucketName: String,
                              maxResults: Option[Long]
  ): Future[BucketUsageResponse] = {
    implicit val service = GoogleInstrumentedService.Storage

    def usageFromLogObject(o: StorageObject): Future[BucketUsageResponse] =
      streamObject(o.getBucket, o.getName) { inputStream =>
        val content = Source.fromInputStream(inputStream).mkString
        val byteHours = BigInt(content.split('\n')(1).split(',')(1).replace("\"", ""))
        val timestampOpt = Option(o.getUpdated.getValue)
        val dateLastUpdated = timestampOpt.map(timestamp => new DateTime(timestamp))
        // convert byte/hours to byte/days to better match the billing unit of GB/days
        BucketUsageResponse(byteHours / 24, dateLastUpdated)
      }

    def recurse(pageToken: Option[String] = None): Future[BucketUsageResponse] = {
      // Fetch objects with a prefix of "${bucketName}_storage_", (ignoring "_usage_" logs)
      val fetcher = getStorage(getBucketServiceAccountCredential)
        .objects()
        .list(GoogleServicesDAO.getStorageLogsBucketName(googleProject))
        .setPrefix(s"${bucketName}_storage_")
      maxResults.foreach(fetcher.setMaxResults(_))
      pageToken.foreach(fetcher.setPageToken)

      retryWhen500orGoogleError { () =>
        val result = executeGoogleRequest(fetcher)
        (Option(result.getItems), Option(result.getNextPageToken))
      } flatMap {
        case (None, _) =>
          // No storage logs, so make sure that the bucket is actually empty
          val fetcher = getStorage(getBucketServiceAccountCredential).objects.list(bucketName).setMaxResults(1L)
          retryWhen500orGoogleError(() => Option(executeGoogleRequest(fetcher).getItems)) flatMap {
            case Some(items) if !items.isEmpty =>
              Future.failed(
                new RawlsExceptionWithErrorReport(
                  ErrorReport(StatusCodes.NotFound, s"No storage logs found for '$bucketName'.'")
                )
              )
            case _ => Future.successful(BucketUsageResponse(BigInt(0), Option(DateTime.now())))
          }
        case (_, Some(nextPageToken)) => recurse(Option(nextPageToken))
        case (Some(items), None)      =>
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
    retryWithRecoverWhen500orGoogleError(() => Option(executeGoogleRequest(aclGetter).getItems.asScala.toList)) {
      case e: HttpResponseException => None
    }
  }

  override def getBucket(bucketName: String, userProject: Option[GoogleProjectId])(implicit
    executionContext: ExecutionContext
  ): Future[Either[String, Bucket]] = {
    implicit val service = GoogleInstrumentedService.Storage
    retryWithRecoverWhen500orGoogleError[Either[String, Bucket]] { () =>
      val getter = getStorage(getBucketServiceAccountCredential).buckets().get(bucketName)
      userProject.map(p => getter.setUserProject(p.value))

      Right(executeGoogleRequest(getter))
    } { case e: HttpResponseException =>
      Left(s"HTTP ${e.getStatusCode}: ${e.getStatusMessage} (${e.getMessage})")
    }
  }

  override def getRegionForRegionalBucket(bucketName: String,
                                          userProject: Option[GoogleProjectId]
  ): Future[Option[String]] =
    getBucket(bucketName, userProject) map {
      case Right(bucket) =>
        bucket.getLocationType match {
          case SingleRegionLocationType => Option(bucket.getLocation)
          case _                        => None
        }
      case Left(message) => throw new RawlsException(s"Failed to retrieve bucket `$bucketName`. $message")
    }

  override def getComputeZonesForRegion(googleProject: GoogleProjectId, region: String): Future[List[String]] = {
    implicit val service = GoogleInstrumentedService.Storage
    retryWithRecoverWhen500orGoogleError { () =>
      // convert the region to lowercase because the `.region().get()` API expects the region input to match
      // the pattern: /[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?|[1-9][0-9]{0,19}/
      val getter =
        getComputeManager(getBucketServiceAccountCredential).regions().get(googleProject.value, region.toLowerCase)
      val zonesAsResourceUrls = executeGoogleRequest(getter).getZones.asScala.toList

      // `getZones()` returns the zones as resource urls of form `https://www.googleapis.com/compute/v1/projects/project_id/zones/us-central1-b",
      // Hence split it by `/` and get last element of array to get the zone
      zonesAsResourceUrls.map(_.split("/").last)
    } { case e =>
      throw new RawlsException(s"Something went wrong while retrieving zones for region `$region` under Google " +
                                 s"project `${googleProject.value}`.",
                               e
      )
    }
  }

  override def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val inserter =
      getGroupDirectory.members.insert(groupEmail, new Member().setEmail(emailToAdd).setRole(groupMemberRole))
    retryWithRecoverWhen500orGoogleError[Unit](() => executeGoogleRequest(inserter)) { case t: HttpResponseException =>
      StatusCode.int2StatusCode(t.getStatusCode) match {
        case StatusCodes.Conflict => () // it is ok if the email is already there
        case StatusCodes.PreconditionFailed =>
          val msg =
            s"Precondition failed adding user $emailToAdd to group $groupEmail. Is the user a member of too many groups?"
          logger.error(msg)
          throw new RawlsException(msg, t)
        case _ => throw t
      }
    }
  }

  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Groups
    val deleter = getGroupDirectory.members.delete(groupEmail, emailToRemove)
    retryWithRecoverWhen500orGoogleError[Unit](() => executeGoogleRequest(deleter)) {
      case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue =>
        () // it is ok of the email is already missing
    }
  }

  override def copyFile(sourceBucket: String,
                        sourceObject: String,
                        destinationBucket: String,
                        destinationObject: String,
                        userProject: Option[GoogleProjectId]
  )(implicit executionContext: ExecutionContext): Future[Option[StorageObject]] = {
    implicit val service = GoogleInstrumentedService.Storage

    val copier = getStorage(getBucketServiceAccountCredential).objects.copy(sourceBucket,
                                                                            sourceObject,
                                                                            destinationBucket,
                                                                            destinationObject,
                                                                            new StorageObject()
    )
    userProject.map(p => copier.setUserProject(p.value))

    retryWhen500orGoogleError(() => Option(executeGoogleRequest(copier)))
  }

  override def listObjectsWithPrefix(bucketName: String,
                                     objectNamePrefix: String,
                                     userProject: Option[GoogleProjectId]
  ): Future[List[StorageObject]] = {
    implicit val service = GoogleInstrumentedService.Storage
    val getter = getStorage(getBucketServiceAccountCredential)
      .objects()
      .list(bucketName)
      .setPrefix(objectNamePrefix)
      .setMaxResults(maxPageSize.toLong)
    userProject.map(p => getter.setUserProject(p.value))

    listObjectsRecursive(getter) map { pagesOption =>
      pagesOption
        .map { pages =>
          pages.flatMap { page =>
            Option(page.getItems) match {
              case None          => List.empty
              case Some(objects) => objects.asScala.toList
            }
          }
        }
        .getOrElse(List.empty)
    }
  }

  private def listObjectsRecursive(fetcher: Storage#Objects#List,
                                   accumulated: Option[List[Objects]] = Some(Nil)
  ): Future[Option[List[Objects]]] = {
    implicit val service = GoogleInstrumentedService.Storage

    accumulated match {
      // when accumulated has a Nil list then this must be the first request
      case Some(Nil) =>
        retryWithRecoverWhen500orGoogleError(() => Option(executeGoogleRequest(fetcher))) {
          case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
        }.flatMap(firstPage => listObjectsRecursive(fetcher, firstPage.map(List(_))))

      // the head is the Objects object of the prior request which contains next page token
      case Some(head :: _) if head.getNextPageToken != null =>
        retryWhen500orGoogleError(() => executeGoogleRequest(fetcher.setPageToken(head.getNextPageToken))).flatMap(
          nextPage => listObjectsRecursive(fetcher, accumulated.map(pages => nextPage :: pages))
        )

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
      logger.info(
        s"diagnosticBucketRead to $bucketName returned ${httpResponse.status.intValue} " +
          s"as user ${userInfo.userEmail.value}, subjectid ${userInfo.userSubjectId.value}, with token hash ${userInfo.accessToken.token.hashCode} " +
          s"and response entity ${Unmarshal(httpResponse.entity).to[String]}"
      )
      httpResponse.status match {
        case StatusCodes.OK => None
        case x              => Some(ErrorReport(x, x.defaultMessage()))
      }
    } recover { case t: Throwable =>
      logger.warn(s"diagnosticBucketRead to $bucketName encountered unexpected error: ${t.getMessage}")
      Some(ErrorReport(t))
    }
  }

  override def testTerraBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean] =
    testBillingAccountAccess(billingAccountName, getBillingServiceAccountCredential)

  override def testTerraAndUserBillingAccountAccess(billingAccount: RawlsBillingAccountName,
                                                    userInfo: UserInfo
  ): Future[Boolean] = {
    val cred = getUserCredential(userInfo)

    for {
      firecloudHasAccess <- testTerraBillingAccountAccess(billingAccount)
      userHasAccess <- cred.traverse(c => testBillingAccountAccess(billingAccount, c))
    } yield
    // Return false if the user does not have a Google token
    firecloudHasAccess && userHasAccess.getOrElse(false)
  }

  protected def testBillingAccountAccess(billingAccount: RawlsBillingAccountName, credential: Credential)(implicit
    executionContext: ExecutionContext
  ): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Billing
    val testIamPermissionsRequest =
      new TestIamPermissionsRequest().setPermissions(List(BILLING_ACCOUNT_PERMISSION).asJava)
    val fetcher = getCloudBillingManager(credential)
      .billingAccounts()
      .testIamPermissions(billingAccount.value, testIamPermissionsRequest)
    retryWithRecoverWhen500orGoogleError { () =>
      val response = blocking {
        executeGoogleRequest(fetcher)
      }
      Option(response.getPermissions).map(_.asScala).getOrElse(List.empty).nonEmpty
    } {
      case gjre: GoogleJsonResponseException if gjre.getStatusCode / 100 == 4 => false // any 4xx error means no access
    }
  }

  override def testSAGoogleBucketIam(bucketName: GcsBucketName, saKey: String, permissions: Set[IamPermission])(implicit
    executionContext: ExecutionContext
  ): Future[Set[IamPermission]] =
    if (permissions.isEmpty) {
      Future.successful(Set.empty)
    } else {
      implicit val async = IO.asyncForIO
      val storageServiceResource = GoogleStorageService.fromCredentials(
        ServiceAccountCredentials.fromStream(new ByteArrayInputStream(saKey.getBytes))
      )
      storageServiceResource
        .use { storageService =>
          storageService.testIamPermissions(bucketName, permissions.toList).compile.last
        }
        .map(_.getOrElse(List.empty).toSet)
        .unsafeToFuture()
    }

  def testSAGoogleBucketGetLocationOrRequesterPays(googleProject: GoogleProject,
                                                   bucketName: GcsBucketName,
                                                   saKey: String
  )(implicit
    executionContext: ExecutionContext
  ): Future[Boolean] = {
    implicit val async = IO.asyncForIO
    val credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(saKey.getBytes))
    val storageServiceResource = GoogleStorageService.fromCredentials(credentials)
    storageServiceResource
      .use { storageService =>
        storageService.getBucket(googleProject, bucketName, warnOnError = true)
      }
      .map(_.isDefined)
      .unsafeToFuture()
      .recoverWith {
        case t: Throwable if REQUESTER_PAYS_ERROR_SUBSTRINGS.exists(t.getMessage.toLowerCase.contains) =>
          logger.info(
            s"${credentials.getClientEmail} was unable to get bucket location for $googleProject/$bucketName, but it appears this is a requester-pays bucket"
          )
          Future.successful(true)
        case t: Throwable =>
          logger.warn(s"${credentials.getClientEmail} was unable to get bucket location for $googleProject/$bucketName",
                      t
          )
          Future.successful(false)
      }
  }

  override def testSAGoogleProjectIam(project: GoogleProject, saKey: String, permissions: Set[IamPermission])(implicit
    executionContext: ExecutionContext
  ): Future[Set[IamPermission]] =
    if (permissions.isEmpty) {
      Future.successful(Set.empty)
    } else {
      val iamDao = new HttpGoogleIamDAO(appName, GoogleCredentialModes.Json(saKey), workbenchMetricBaseName)
      iamDao.testIamPermission(project, permissions)
    }

  protected def listBillingAccounts(
    credential: Credential
  )(implicit executionContext: ExecutionContext): Future[List[BillingAccount]] = {
    implicit val service = GoogleInstrumentedService.Billing

    type Paginated[T] = (Option[T], Option[String])

    def makeCall(pageToken: Option[String] = None): Future[Paginated[mutable.Buffer[BillingAccount]]] =
      retryWithRecoverWhen500orGoogleError { () =>
        val result = executeGoogleListBillingAccountsRequest(credential, pageToken)
        // option-wrap getBillingAccounts because it returns null for an empty list,
        // and result.getNextPateToken = '' when there are no more pages.
        (Option(result.getBillingAccounts.asScala),
         Option(if (result.getNextPageToken.isEmpty) null else result.getNextPageToken)
        )
      } {
        case gjre: GoogleJsonResponseException
            if gjre.getStatusCode == StatusCodes.Forbidden.intValue &&
              gjre.getDetails.getMessage == "Request had insufficient authentication scopes." =>
          // This error message is purely informational. A client can determine which scopes it has
          // been granted, so an insufficiently-scoped request would generally point to a programming error.
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.Forbidden, BillingAccountScopes(billingScopes).toJson.toString)
          )
      }

    def recurse(acc: List[BillingAccount] = List.empty,
                pageToken: Option[String] = None
    ): Future[List[BillingAccount]] = {
      val result: Future[Paginated[mutable.Buffer[BillingAccount]]] = makeCall(pageToken)
      result.flatMap {
        case (Some(items), Some(nextPage)) => recurse(acc ::: items.toList, Some(nextPage))
        case (Some(items), None)           => Future.successful(acc ::: items.toList)
        case _                             => Future.successful(acc)
      }
    }
    recurse()
  }

  protected def executeGoogleListBillingAccountsRequest(credential: Credential, pageToken: Option[String] = None)(
    implicit counters: GoogleCounters
  ): ListBillingAccountsResponse = {
    // To test with pagination, can call `setPageSize` at the end of the call below.
    val fetcher = getCloudBillingManager(credential).billingAccounts().list()
    pageToken.foreach(fetcher.setPageToken)
    blocking(executeGoogleRequest(fetcher))
  }

  override def listBillingAccounts(userInfo: UserInfo,
                                   firecloudHasAccess: Option[Boolean] = None
  ): Future[Seq[RawlsBillingAccount]] = {
    val cred = getUserCredential(userInfo)

    for {
      // Returns an empty list if the user does not have a Google token.
      accountList <- cred.toList.flatTraverse(listBillingAccounts)

      // some users have TONS of billing accounts, enough to hit quota limits.
      // break the list of billing accounts up into chunks.
      // each chunk executes all its requests in parallel. the chunks themselves are processed serially.
      // this limits the amount of parallelism (to 10 inflight requests at a time), which should should slow
      // our rate of making requests. if this fails to be enough, we may need to upgrade this to an explicit throttle.
      accountChunks: List[Seq[BillingAccount]] = accountList.grouped(10).toList

      // Iterate over each chunk.
      allProcessedChunks: IO[List[Seq[RawlsBillingAccount]]] = accountChunks.traverse { chunk =>
        // Filter out the billing accounts that are closed. They have no value to users
        // and can cause confusion by cluttering their lists
        val filteredChunk = chunk.filter { account =>
          // Wrap in an Option for safety, as getOpen can return null
          val isOpen = Option(account.getOpen)
          // Convert null into true. Google doesn't document the behavior in this case, so it's better to potentially return
          // slightly too many than omit potentially valid billing accounts. nulls should be rare or non-existent.
          isOpen.getOrElse(true) == true
        }

        // Run all tests in the chunk in parallel.
        IO.fromFuture(IO(Future.traverse(filteredChunk) { acct =>
          val acctName = RawlsBillingAccountName(acct.getName)
          testTerraBillingAccountAccess(acctName) map { firecloudHasAccount =>
            RawlsBillingAccount(acctName, firecloudHasAccount, acct.getDisplayName)
          }
        }))
      }

      res <- allProcessedChunks.map(_.flatten).unsafeToFuture()
    } yield res.filter(account => firecloudHasAccess.forall(access => access == account.firecloudHasAccess))
  }

  override def listBillingAccountsUsingServiceCredential(implicit
    executionContext: ExecutionContext
  ): Future[Seq[RawlsBillingAccount]] = {
    val billingSvcCred = getBillingServiceAccountCredential
    listBillingAccounts(billingSvcCred) map { accountList =>
      accountList.map(acct => RawlsBillingAccount(RawlsBillingAccountName(acct.getName), true, acct.getDisplayName))
    }
  }

  /**
   * Explicitly sets the Billing Account on a Google Project to the value given, even if it is empty.  Callers should
   * ensure that the new Billing Account value is valid and non-empty as this method will not perform any input
   * validations.
   * @param googleProjectId
   * @param billingAccountName
   * @return
   */
  override def setBillingAccountName(googleProjectId: GoogleProjectId,
                                     billingAccountName: RawlsBillingAccountName,
                                     span: Span = null
  ): Future[ProjectBillingInfo] = {
    // Since this method should only be called with a non-empty Billing Account Name, then Rawls should also make sure
    // that Billing is enabled for the Google Project, otherwise users' projects could get "stuck" in a Disabled Billing
    // state with no way to reenable because they do not have permissions to do this directly on the Google Project.
    val newProjectBillingInfo =
      new ProjectBillingInfo().setBillingAccountName(billingAccountName.value).setBillingEnabled(true)
    updateBillingInfo(googleProjectId, newProjectBillingInfo, span)
  }

  override def disableBillingOnGoogleProject(googleProjectId: GoogleProjectId): Future[ProjectBillingInfo] = {
    val newProjectBillingInfo = new ProjectBillingInfo().setBillingEnabled(false)
    updateBillingInfo(googleProjectId, newProjectBillingInfo)
  }

  private def updateBillingInfo(googleProjectId: GoogleProjectId,
                                projectBillingInfo: ProjectBillingInfo,
                                parentSpan: Span = null
  ): Future[ProjectBillingInfo] = {
    implicit val service = GoogleInstrumentedService.Billing
    val billingSvcCred = getBillingServiceAccountCredential
    val cloudBillingProjectsApi = getCloudBillingManager(billingSvcCred).projects()

    traceWithParent("cloudBillingProjectsApi.updateBillingInfo", parentSpan) { s =>
      val updater = cloudBillingProjectsApi.updateBillingInfo(s"projects/${googleProjectId.value}", projectBillingInfo)
      retryWithRecoverWhen500orGoogleError { () =>
        blocking {
          val span = startSpanWithParent("executeGoogleRequest", s)
          span.putAttribute("googleProjectId", AttributeValue.stringAttributeValue(googleProjectId.value))
          span.putAttribute("billingAccount",
                            AttributeValue.stringAttributeValue(Option(projectBillingInfo.getBillingAccountName) match {
                              case Some(value) => value
                              case None        => ""
                            })
          )

          try
            executeGoogleRequest(updater)
          finally
            span.end()
        }
      } {
        case e: GoogleJsonResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.Forbidden,
              s"Rawls service account does not have access to Billing Account on Google Project ${googleProjectId}: ${projectBillingInfo}",
              e
            )
          )
      }
    }
  }

  override def getBillingInfoForGoogleProject(
    googleProjectId: GoogleProjectId
  )(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] = {
    val billingSvcCred = getBillingServiceAccountCredential
    implicit val service = GoogleInstrumentedService.Billing
    val googleProjectName = s"projects/${googleProjectId.value}"
    val cloudBillingProjectsApi = getCloudBillingManager(billingSvcCred).projects()

    val fetcher = cloudBillingProjectsApi.getBillingInfo(googleProjectName)

    retryWithRecoverWhen500orGoogleError { () =>
      blocking {
        executeGoogleRequest(fetcher)
      }
    } {
      case e: GoogleJsonResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(
            StatusCodes.Forbidden,
            s"Rawls service account does not have access to Billing Info for project: ${googleProjectId.value}",
            e
          )
        )
    }
  }

  // Note that these APIs only allow for returning the fully qualified name i.e. billingAccounts/01010-01010-01010
  // This will return just the ID of the billing account by stripping off the `billingAccounts/` prefix
  override def getBillingAccountIdForGoogleProject(googleProject: GoogleProject, userInfo: UserInfo)(implicit
    executionContext: ExecutionContext
  ): Future[Option[String]] = {
    implicit val service = GoogleInstrumentedService.Billing

    val fullGoogleProjectName = s"projects/${googleProject.value}"

    for {
      // Fail if the user does not have a Google token
      credential <- IO
        .fromOption(getUserCredential(userInfo))(new RawlsException("Google login required to view billing accounts"))
        .unsafeToFuture()
      fetcher = getCloudBillingManager(credential).projects().getBillingInfo(fullGoogleProjectName)
      billingInfo <- retryWhen500orGoogleError { () =>
        blocking {
          executeGoogleRequest(fetcher)
        }
      }
    } yield Option(billingInfo.getBillingAccountName.stripPrefix("billingAccounts/"))
  }

  override def getGenomicsOperation(opId: String): Future[Option[JsObject]] = {

    def papiv1Handler(opId: String) = {
      // PAPIv1 ids start with "operations". We have to use a direct http call instead of a client library because
      // the client lib does not support PAPIv1 and PAPIv2 concurrently.
      val genomicsServiceAccountCredential = getGenomicsServiceAccountCredential
      genomicsServiceAccountCredential.refreshToken()
      new GenomicsV1DAO().getOperation(opId, OAuth2BearerToken(genomicsServiceAccountCredential.getAccessToken))
    }

    def papiv2Alpha1Handler(opId: String) = {
      val genomicsServiceAccountCredential = getGenomicsServiceAccountCredential
      genomicsServiceAccountCredential.refreshToken()
      val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, genomicsServiceAccountCredential)
        .setApplicationName(appName)
        .build()
      val operationRequest = genomicsApi.projects().operations().get(opId)
      implicit val service = GoogleInstrumentedService.Genomics

      retryWithRecoverWhen500orGoogleError { () =>
        // Google library returns a Map[String,AnyRef], but we don't care about understanding the response
        // So, use Google's functionality to get the json string, then parse it back into a generic json object
        Option(executeGoogleRequest(operationRequest).toPrettyString.parseJson.asJsObject)
      } {
        // Recover from Google 404 errors because it's an expected return status.
        // Here we use `None` to represent a 404 from Google.
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
      }
    }

    def lifeSciencesBetaHandler(opId: String) = {
      val lifeSciencesAccountCredential = getLifeSciencesServiceAccountCredential()
      lifeSciencesAccountCredential.refreshToken()
      val lifeSciencesApi = new CloudLifeSciences.Builder(httpTransport, jsonFactory, lifeSciencesAccountCredential)
        .setApplicationName(appName)
        .build()
      val operationRequest = lifeSciencesApi.projects().locations().operations().get(opId)
      implicit val service = GoogleInstrumentedService.LifeSciences

      retryWithRecoverWhen500orGoogleError { () =>
        // Google library returns a Map[String,AnyRef], but we don't care about understanding the response
        // So, use Google's functionality to get the json string, then parse it back into a generic json object
        Option(executeGoogleRequest(operationRequest).toPrettyString.parseJson.asJsObject)
      } {
        // Recover from Google 404 errors because it's an expected return status.
        // Here we use `None` to represent a 404 from Google.
        case t: HttpResponseException if t.getStatusCode == StatusCodes.NotFound.intValue => None
      }
    }

    def noMatchHandler(opId: String) = Future.failed(new Exception(s"Operation ID '$opId' is not a supported format"))

    handleByOperationIdType(opId, papiv1Handler, papiv2Alpha1Handler, lifeSciencesBetaHandler, noMatchHandler)
  }

  override def checkGenomicsOperationsHealth(implicit executionContext: ExecutionContext): Future[Boolean] = {
    implicit val service = GoogleInstrumentedService.Genomics
    val opId = s"projects/$serviceProject/operations"
    val genomicsApi = new Genomics.Builder(httpTransport, jsonFactory, getGenomicsServiceAccountCredential)
      .setApplicationName(appName)
      .build()
    val operationRequest = genomicsApi.projects().operations().list(opId).setPageSize(1)
    retryWhen500orGoogleError { () =>
      executeGoogleRequest(operationRequest)
      true
    }
  }

  override def getGoogleProject(googleProject: GoogleProjectId): Future[Project] = {
    implicit val service = GoogleInstrumentedService.Billing
    val cloudResManager = getCloudResourceManagerWithBillingServiceAccountCredential
    val statusCodesExcludedFromRetry: Set[StatusCode] = Set(StatusCodes.NotFound, StatusCodes.Forbidden)
    retryExponentially(throwable =>
      when500orGoogleError(throwable) || whenGoogleStatusDoesntContain(throwable, statusCodesExcludedFromRetry)
    )(() => Future(blocking(executeGoogleRequest(cloudResManager.projects().get(googleProject.value)))))
  }

  /**
   * Google is not consistent when dealing with folder ids. Some apis do not want the folder id to start with
   * "folders/" but other apis return that or expect that. This function strips the prefix if it exists.
   *
   * @param folderId
   * @return
   */
  private def folderNumberOnly(folderId: String) = folderId.stripPrefix("folders/")

  override def pollOperation(operationId: OperationId): Future[OperationStatus] =
    // this code is a colossal DRY violation but because the operations collection is different
    // for cloudResManager and servicesManager and they return different but identical Status objects
    // there is not much else to be done... too bad scala does not have duck typing.
    operationId.apiType match {
      case GoogleApiTypes.AccessContextManagerApi =>
        accessContextManagerDAO.pollOperation(operationId.operationId).map { op =>
          OperationStatus(toScalaBool(op.getDone),
                          Option(op.getError).map(error => toErrorMessage(error.getMessage, error.getCode))
          )
        }
    }

  /**
   * converts a possibly null java boolean to a scala boolean, null is treated as false
   */
  private def toScalaBool(b: java.lang.Boolean) = Option(b).contains(java.lang.Boolean.TRUE)

  private def toErrorMessage(message: String, code: String): String =
    s"${Option(message).getOrElse("")} - code ${code}"

  private def toErrorMessage(message: String, code: Int): String =
    s"${Option(message).getOrElse("")} - code ${code}"

  /**
   * Updates policy bindings on a google project.
   * 1) get existing policies
   * 2) call updatePolicies
   * 3) if updated policies are the same as existing policies return false, don't call google
   * 4) if updated policies are different than existing policies update google and return true
   *
   * @param googleProject google project id
   * @param updatePolicies function (existingPolicies => updatedPolicies). May return policies with no members
   *                       which will be handled appropriately when sent to google.
   * @return true if google was called to update policies, false otherwise
   */
  override protected def updatePolicyBindings(
    googleProject: GoogleProjectId
  )(updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]): Future[Boolean] = {
    val cloudResManager = getCloudResourceManagerWithBillingServiceAccountCredential
    implicit val service = GoogleInstrumentedService.CloudResourceManager

    for {
      updated <- retryWhen500orGoogleError { () =>
        // it is important that we call getIamPolicy within the same retry block as we call setIamPolicy
        // getIamPolicy gets the etag that is used in setIamPolicy, the etag is used to detect concurrent
        // modifications and if that happens we need to be sure to get a new etag before retrying setIamPolicy
        val existingPolicy = executeGoogleRequest(cloudResManager.projects().getIamPolicy(googleProject.value, null))
        val existingPolicies: Map[String, Set[String]] = existingPolicy.getBindings.asScala.map { policy =>
          policy.getRole -> policy.getMembers.asScala.toSet
        }.toMap

        val updatedPolicies = updatePolicies(existingPolicies)

        if (updatedPolicies.equals(existingPolicies)) {
          false
        } else {
          val updatedBindings = updatedPolicies.collect {
            case (role, members) if members.nonEmpty => // exclude policies with empty members
              new Binding().setRole(role).setMembers(members.toList.asJava)
          }.toSeq

          // when setting IAM policies, always reuse the existing policy so the etag is preserved.
          val policyRequest = new SetIamPolicyRequest().setPolicy(existingPolicy.setBindings(updatedBindings.asJava))
          executeGoogleRequest(cloudResManager.projects().setIamPolicy(googleProject.value, policyRequest))
          true
        }
      }

    } yield updated
  }

  // TODO - once workspace migration is complete and there are no more v1 workspaces or v1 billing projects, we can remove this https://broadworkbench.atlassian.net/browse/CA-1118
  // V2 workspace projects are managed by rawls SA but the v1 billing google projects are managed by the billing SA.
  override def deleteV1Project(googleProject: GoogleProjectId): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Billing
    val billingServiceAccountCredential = getBillingServiceAccountCredential

    val resMgr = getCloudResourceManagerWithBillingServiceAccountCredential
    val billingManager = getCloudBillingManager(billingServiceAccountCredential)

    for {
      _ <- retryWhen500orGoogleError { () =>
        executeGoogleRequest(
          billingManager
            .projects()
            .updateBillingInfo(s"projects/${googleProject.value}", new ProjectBillingInfo().setBillingEnabled(false))
        )
      }
      _ <- retryWithRecoverWhen500orGoogleError { () =>
        executeGoogleRequest(resMgr.projects().delete(googleProject.value))
      } {
        case e: GoogleJsonResponseException
            if e.getDetails.getCode == 403 && "Cannot delete an inactive project.".equals(e.getDetails.getMessage) =>
          new Empty()
        // stop trying to delete an already deleted project
      }
    } yield {
      // nothing
    }
  }

  /**
   * Updates the project specified by the googleProjectId with any values in googleProjectWithUpdates.
   * @param googleProjectId project to update
   * @param googleProjectWithUpdates [[Project]] with values to update. For example, a (new Project().setName("ex")) will update the name of the googleProjectId project.
   * @return the project passed in as googleProjectWithUpdates
   */
  override def updateGoogleProject(googleProjectId: GoogleProjectId,
                                   googleProjectWithUpdates: Project
  ): Future[Project] = {
    implicit val service = GoogleInstrumentedService.CloudResourceManager
    val cloudResourceManager: CloudResourceManager = getCloudResourceManagerWithBillingServiceAccountCredential

    executeGoogleRequestWithRetry(
      cloudResourceManager.projects().update(googleProjectId.value, googleProjectWithUpdates)
    )
  }

  override def deleteGoogleProject(googleProject: GoogleProjectId): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.Billing
    val billingServiceAccountCredential = getBillingServiceAccountCredential
    val billingManager = getCloudBillingManager(billingServiceAccountCredential)
    val cloudResourceManager: CloudResourceManager = getCloudResourceManagerWithBillingServiceAccountCredential

    for {
      _ <- retryWhen500orGoogleError { () =>
        executeGoogleRequest(
          billingManager
            .projects()
            .updateBillingInfo(s"projects/${googleProject.value}", new ProjectBillingInfo().setBillingEnabled(false))
        )
      }
      _ <- retryWithRecoverWhen500orGoogleError { () =>
        executeGoogleRequest(cloudResourceManager.projects().delete(googleProject.value))
      } {
        case e: GoogleJsonResponseException
            if e.getDetails.getCode == 403 && "Cannot delete an inactive project.".equals(e.getDetails.getMessage) =>
          new Empty()
        // stop trying to delete an already deleted project
      }
    } yield {
      // nothing
    }
  }

  def projectUsageExportBucketName(googleProject: GoogleProjectId) = s"${googleProject.value}-usage-export"

  override def getBucketDetails(bucketName: String, project: GoogleProjectId): Future[WorkspaceBucketOptions] = {
    implicit val service = GoogleInstrumentedService.Storage
    val cloudStorage = getStorage(getBucketServiceAccountCredential)
    for {
      bucketDetails <- retryWhen500orGoogleError { () =>
        executeGoogleRequest(cloudStorage.buckets().get(bucketName).setUserProject(project.value))
      }
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

  def getComputeManager(credential: Credential): Compute =
    new Compute.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()

  def getCloudBillingManager(credential: Credential): Cloudbilling = {
    def setReadTimeout(requestInitializer: HttpRequestInitializer) =
      new HttpRequestInitializer {
        override def initialize(request: HttpRequest): Unit = {
          requestInitializer.initialize(request)
          request.setReadTimeout(cloudBillingInfoReadTimeout)
        }
      }

    new Cloudbilling.Builder(httpTransport, jsonFactory, setReadTimeout(credential))
      .setApplicationName(appName)
      .build()
  }

  def getCloudResourceManager(credential: Credential): CloudResourceManager =
    new CloudResourceManager.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()

  def getIAM(credential: Credential): Iam =
    new Iam.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()

  def getIAMCredentials(credential: Credential): IAMCredentials =
    new IAMCredentials.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()

  def getStorage(credential: Credential) =
    new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()

  def getGroupDirectory =
    new Directory.Builder(httpTransport, jsonFactory, getGroupServiceAccountCredential)
      .setApplicationName(appName)
      .build()

  private def getCloudResourceManagerWithBillingServiceAccountCredential = {
    val billingServiceAccountCredential = getBillingServiceAccountCredential
    val cloudResourceManager = getCloudResourceManager(billingServiceAccountCredential)
    cloudResourceManager
  }

  private def getGroupServiceAccountCredential: Credential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(directoryScopes.asJava)
      .setServiceAccountUser(subEmail)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()

  def getBucketServiceAccountCredential: Credential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(storageScopes.asJava) // grant bucket-creation powers
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()

  def getGenomicsServiceAccountCredential: Credential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(genomicsScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()

  def getLifeSciencesServiceAccountCredential(): Credential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(lifesciencesScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()

  def getBillingServiceAccountCredential: Credential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountScopes(
        Seq(ComputeScopes.CLOUD_PLATFORM).asJava
      ) // need this broad scope to create/manage projects
      .setServiceAccountId(billingPemEmail)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(billingPemFile))
      .setServiceAccountUser(billingEmail)
      .build()

  lazy val getResourceBufferServiceAccountCredential: Credential = {
    val file = new java.io.File(resourceBufferJsonFile)
    val inputStream: InputStream = new FileInputStream(file)
    GoogleCredential.fromStream(inputStream).toBuilder.setServiceAccountScopes(workbenchLoginScopes.asJava).build()
  }

  def toGoogleGroupName(groupName: RawlsGroupName) = s"${proxyNamePrefix}GROUP_${groupName.value}@${appsDomain}"

  def adminGroupName = s"${groupsPrefix}-ADMINS@${appsDomain}"
  def curatorGroupName = s"${groupsPrefix}-CURATORS@${appsDomain}"
  def makeGroupEntityString(groupId: String) = s"group-$groupId"

  private def buildCredentialFromAccessToken(accessToken: String, credentialEmail: String): GoogleCredential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .build()
      .setAccessToken(accessToken)

  def getAccessTokenUsingJson(saKey: String): Future[String] = {
    implicit val service = GoogleInstrumentedService.OAuth
    retryWhen500orGoogleError { () =>
      val keyStream = new ByteArrayInputStream(saKey.getBytes)
      val credential = ServiceAccountCredentials.fromStream(keyStream).createScoped(storageScopes.asJava)
      credential.refreshAccessToken.getTokenValue
    }
  }

  def getUserInfoUsingJson(saKey: String): Future[UserInfo] = {
    implicit val service = GoogleInstrumentedService.OAuth
    retryWhen500orGoogleError { () =>
      val keyStream = new ByteArrayInputStream(saKey.getBytes)
      val credential = ServiceAccountCredentials.fromStream(keyStream).createScoped(storageScopes.asJava)
      UserInfo.buildFromTokens(credential)
    }
  }

  def getRawlsUserForCreds(creds: Credential): Future[RawlsUser] = {
    implicit val service = GoogleInstrumentedService.Groups
    val oauth2 = new Builder(httpTransport, jsonFactory, null).setApplicationName(appName).build()
    Future {
      creds.refreshToken()
      val tokenInfo = executeGoogleRequest(oauth2.tokeninfo().setAccessToken(creds.getAccessToken), logRequest = false)
      RawlsUser(RawlsUserSubjectId(tokenInfo.getUserId), RawlsUserEmail(tokenInfo.getEmail))
    }.recover { case e: GoogleJsonResponseException =>
      // abbreviate the error message to ensure we don't include a token in the exception
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.InternalServerError,
                    s"Failed to get token info: ${StringUtils.abbreviate(e.getMessage, 50)}"
        )
      )
    }
  }

  def getServiceAccountUserInfo(): Future[UserInfo] = {
    val creds = getBucketServiceAccountCredential
    getRawlsUserForCreds(creds).map { rawlsUser =>
      UserInfo(rawlsUser.userEmail,
               OAuth2BearerToken(creds.getAccessToken),
               creds.getExpiresInSeconds,
               rawlsUser.userSubjectId
      )
    }
  }

  private def streamObject[A](bucketName: String, objectName: String)(f: (InputStream) => A): Future[A] = {
    implicit val service = GoogleInstrumentedService.Storage
    val getter = getStorage(getBucketServiceAccountCredential).objects().get(bucketName, objectName).setAlt("media")
    retryWhen500orGoogleError(() => executeGoogleFetch(getter)(is => f(is)))
  }

  override def addProjectToFolder(googleProject: GoogleProjectId, folderId: String): Future[Unit] = {
    implicit val service = GoogleInstrumentedService.CloudResourceManager
    val cloudResourceManager = getCloudResourceManagerWithBillingServiceAccountCredential

    retryWhen500orGoogleError { () =>
      val existingProject = executeGoogleRequest(cloudResourceManager.projects().get(googleProject.value))

      val folderResourceId =
        new ResourceId().setType(GoogleResourceTypes.Folder.value).setId(folderNumberOnly(folderId))
      executeGoogleRequest(
        cloudResourceManager.projects().update(googleProject.value, existingProject.setParent(folderResourceId))
      )
    }
  }

  override def getFolderId(folderName: String): Future[Option[String]] = {
    val credential = getBillingServiceAccountCredential
    credential.refreshToken()

    retryExponentially(when500or400orGoogleError) { () =>
      new CloudResourceManagerV2DAO().getFolderId(folderName, OAuth2BearerToken(credential.getAccessToken))
    }
  }
}

object HttpGoogleServicesDAO {
  def handleByOperationIdType[T](opId: String,
                                 papiV1Handler: String => T,
                                 papiV2alpha1Handler: String => T,
                                 lifeSciencesBetaHandler: String => T,
                                 noMatchHandler: String => T
  ): T = {
    val papiv1AlphaIdRegex = "operations/[^/]*".r
    val papiv2Alpha1IdRegex = "projects/[^/]*/operations/[^/]*".r
    val lifeSciencesBetaIdRegex = "projects/[^/]*/locations/[^/]*/operations/[^/]*".r

    opId match {
      case papiv1AlphaIdRegex()      => papiV1Handler(opId)
      case papiv2Alpha1IdRegex()     => papiV2alpha1Handler(opId)
      case lifeSciencesBetaIdRegex() => lifeSciencesBetaHandler(opId)
      case _                         => noMatchHandler(opId)
    }
  }

  private[dataaccess] def getUserCredential(userInfo: UserInfo): Option[Credential] = {
    // Use the Google token if present to build the credential
    val tokenOpt = if (userInfo.isB2C) userInfo.googleAccessTokenThroughB2C else Some(userInfo.accessToken)
    tokenOpt.map { googleToken =>
      new GoogleCredential().setAccessToken(googleToken.token).setExpiresInSeconds(userInfo.accessTokenExpiresIn)
    }
  }
}

class GenomicsV1DAO(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends DsdeHttpDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getOperation(opId: String, accessToken: OAuth2BearerToken): Future[Option[JsObject]] = {
    import DefaultJsonProtocol._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    executeRequestWithToken[Option[JsObject]](accessToken)(
      RequestBuilding.Get(s"https://genomics.googleapis.com/v1alpha2/$opId")
    )
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
class CloudResourceManagerV2DAO(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends DsdeHttpDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getFolderId(folderName: String, accessToken: OAuth2BearerToken): Future[Option[String]] = {
    import DefaultJsonProtocol._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

    implicit val FolderFormat = jsonFormat1(Folder)
    implicit val FolderSearchResponseFormat = jsonFormat1(FolderSearchResponse)

    executeRequestWithToken[FolderSearchResponse](accessToken)(
      RequestBuilding.Post(s"https://cloudresourcemanager.googleapis.com/v2/folders:search",
                           Map("query" -> s"displayName=$folderName")
      )
    ).map { response =>
      response.folders.flatMap { folders =>
        if (folders.size > 1) {
          throw new RawlsException(
            s"google folder search returned more than one folder with display name $folderName: $folders"
          )
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
