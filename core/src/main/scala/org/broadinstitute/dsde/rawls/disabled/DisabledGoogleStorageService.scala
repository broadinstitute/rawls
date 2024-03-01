package org.broadinstitute.dsde.rawls.disabled

import java.nio.file.Path
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.std.Semaphore
import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken, GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.storage.BucketInfo.LifecycleRule
import com.google.cloud.storage.{Acl, Blob, BlobId, BucketInfo, StorageClass}
import com.google.cloud.{Identity, Policy}
import fs2.{Pipe, Stream}
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption, BlobSourceOption, BlobTargetOption, BlobWriteOption, BucketGetOption, BucketSourceOption, BucketTargetOption}
import org.broadinstitute.dsde.workbench
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{GetMetadataResponse, GoogleStorageService, StorageRole}
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject, IamPermission}
import org.broadinstitute.dsde.workbench.util2.RemoveObjectResult

import java.net.URL
import scala.concurrent.duration.{FiniteDuration, HOURS, TimeUnit}
import scala.language.higherKinds

class DisabledGoogleStorageService[F[_]] extends GoogleStorageService[F] {

  def listObjectsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, isRecursive: Boolean, maxPageSize: Long, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobListOptions: List[BlobListOption]): Stream[F, GcsObjectName] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def listBlobsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, isRecursive: Boolean, maxPageSize: Long, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobListOptions: List[BlobListOption]): Stream[F, Blob] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def createBlob(bucketName: GcsBucketName, objectName: google2.GcsBlobName, objectContents: Array[Byte], objectType: String, metadata: Map[String, String], generation: Option[Long], traceId: Option[TraceId], retryConfig: workbench.RetryConfig): Stream[F, Blob] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def streamUploadBlob(bucketName: GcsBucketName, objectName: google2.GcsBlobName, metadata: Map[String, String], generation: Option[Long], overwrite: Boolean, traceId: Option[TraceId], blobWriteOptions: List[BlobWriteOption]): Pipe[F, Byte, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def setBucketLifecycle(bucketName: GcsBucketName, lifecycleRules: List[LifecycleRule], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketTargetOptions: List[BucketTargetOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def unsafeGetBlobBody(bucketName: GcsBucketName, blobName: google2.GcsBlobName, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobGetOptions: List[BlobGetOption]): F[Option[String]] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def getBlobBody(bucketName: GcsBucketName, blobName: google2.GcsBlobName, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobGetOptions: List[BlobGetOption]): Stream[F, Byte] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def getBlob(bucketName: GcsBucketName, blobName: google2.GcsBlobName, credential: Option[Credentials], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobGetOptions: List[BlobGetOption]): Stream[F, Blob] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def getSignedBlobUrl(bucketName: GcsBucketName, blobName: google2.GcsBlobName, signingCredentials: ServiceAccountCredentials, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, expirationTime: Long, expirationTimeUnit: TimeUnit, queryParams: Map[String, String]): Stream[F, URL] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def downloadObject(blobId: BlobId, path: Path, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobGetOptions: List[BlobGetOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def getObjectMetadata(bucketName: GcsBucketName, blobName: google2.GcsBlobName, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobGetOptions: List[BlobGetOption]): Stream[F, GetMetadataResponse] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def setObjectMetadata(bucketName: GcsBucketName, blobName: google2.GcsBlobName, metadata: Map[String, String], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobTargetOptions: List[BlobTargetOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def removeObject(bucketName: GcsBucketName, blobName: google2.GcsBlobName, generation: Option[Long], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, blobSourceOptions: List[BlobSourceOption]): Stream[F, RemoveObjectResult] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def getBucket(googleProject: GoogleProject, bucketName: GcsBucketName, bucketGetOptions: List[BucketGetOption], traceId: Option[TraceId], warnOnError: Boolean): F[Option[BucketInfo]] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def setRequesterPays(bucketName: GcsBucketName, requesterPaysEnabled: Boolean, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketTargetOptions: List[BucketTargetOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def insertBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], labels: Map[String, String], traceId: Option[TraceId], bucketPolicyOnlyEnabled: Boolean, logBucket: Option[GcsBucketName], retryConfig: workbench.RetryConfig, location: Option[String], bucketTargetOptions: List[BucketTargetOption], autoclassEnabled: Boolean, autoclassTerminalStorageClass: Option[StorageClass]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[BucketSourceOption], traceId: Option[TraceId], retryConfig: workbench.RetryConfig): Stream[F, Boolean] =
    {Stream.emit(true).covary[F]}
  def setBucketPolicyOnly(bucketName: GcsBucketName, bucketPolicyOnlyEnabled: Boolean, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketTargetOptions: List[BucketTargetOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def setBucketLabels(bucketName: GcsBucketName, labels: Map[String, String], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketTargetOptions: List[BucketTargetOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def setIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketSourceOptions: List[BucketSourceOption]): Stream[F, Unit] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def overrideIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketSourceOptions: List[BucketSourceOption], version: Int): Stream[F, Policy] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def getIamPolicy(bucketName: GcsBucketName, traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketSourceOptions: List[BucketSourceOption]): Stream[F, Policy] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
  def testIamPermissions(bucketName: GcsBucketName, permissions: List[IamPermission], traceId: Option[TraceId], retryConfig: workbench.RetryConfig, bucketSourceOptions: List[BucketSourceOption]): Stream[F, List[IamPermission]] =
  throw new NotImplementedError("getServiceAccountKey is not implemented for Azure.")
}
object DisabledGoogleStorageService {
  def resource[F[_]: Async: StructuredLogger](
    pathToCredentialJson: String,
    blockerBound: Option[Semaphore[F]] = None,
    project: Option[GoogleProject] = None
  ): Resource[F, GoogleStorageService[F]] =
    Resource.pure[F, GoogleStorageService[F]](new DisabledGoogleStorageService[F])

  def fromCredentials[F[_]: Async: StructuredLogger](credentials: GoogleCredentials,
                                                     blockerBound: Option[Semaphore[F]] = None
  ): Resource[F, GoogleStorageService[F]] =
    Resource.pure[F, GoogleStorageService[F]](new DisabledGoogleStorageService[F])

  def fromApplicationDefault[F[_]: Async: StructuredLogger](
    blockerBound: Option[Semaphore[F]] = None
  ): Resource[F, GoogleStorageService[F]] =
    Resource.pure[F, GoogleStorageService[F]](new DisabledGoogleStorageService[F])

  def fromAccessToken[F[_]: Async: StructuredLogger](
    accessToken: AccessToken,
    blockerBound: Option[Semaphore[F]] = None
  ): Resource[F, GoogleStorageService[F]] =
    Resource.pure[F, GoogleStorageService[F]](new DisabledGoogleStorageService[F])


}
