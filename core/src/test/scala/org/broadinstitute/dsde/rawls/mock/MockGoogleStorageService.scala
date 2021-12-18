package org.broadinstitute.dsde.rawls.mock

import cats.data.NonEmptyList
import com.google.auth.Credentials
import com.google.cloud.{Identity, Policy}
import com.google.cloud.storage.{Acl, Blob, BlobId, Bucket, BucketInfo, Storage}
import fs2.Pipe
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleStorageService, RemoveObjectResult, StorageRole}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject}

import java.nio.file.Path
import scala.language.higherKinds

abstract class MockGoogleStorageService[F[_]] extends GoogleStorageService[F] {
  override def listObjectsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, isRecursive: Boolean, maxPageSize: Long, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, GcsObjectName] = ???

  override def listBlobsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String, isRecursive: Boolean, maxPageSize: Long, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Blob] = ???

  override def createBlob(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String, metadata: Map[String, String], generation: Option[Long], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Blob] = ???

  override def streamUploadBlob(bucketName: GcsBucketName, objectName: GcsBlobName, metadata: Map[String, String], generation: Option[Long], overwrite: Boolean, traceId: Option[TraceId]): Pipe[F, Byte, Unit] = ???

  override def setBucketLifecycle(bucketName: GcsBucketName, lifecycleRules: List[BucketInfo.LifecycleRule], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Unit] = ???

  override def unsafeGetBlobBody(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId], retryConfig: RetryConfig): F[Option[String]] = ???

  override def getBlobBody(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Byte] = ???

  override def getBlob(bucketName: GcsBucketName, blobName: GcsBlobName, credential: Option[Credentials], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Blob] = ???

  override def downloadObject(blobId: BlobId, path: Path, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Unit] = ???

  override def getObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, GetMetadataResponse] = ???

  override def setObjectMetadata(bucketName: GcsBucketName, blobName: GcsBlobName, metadata: Map[String, String], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Unit] = ???

  override def removeObject(bucketName: GcsBucketName, blobName: GcsBlobName, generation: Option[Long], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, RemoveObjectResult] = ???

  override def getBucket(googleProject: GoogleProject, bucketName: GcsBucketName, bucketGetOptions: List[Storage.BucketGetOption], traceId: Option[TraceId]): F[Option[Bucket]] = ???

  override def insertBucket(googleProject: GoogleProject, bucketName: GcsBucketName, acl: Option[NonEmptyList[Acl]], labels: Map[String, String], traceId: Option[TraceId], bucketPolicyOnlyEnabled: Boolean, logBucket: Option[GcsBucketName], retryConfig: RetryConfig, location: Option[String]): fs2.Stream[F, Unit] = ???

  override def deleteBucket(googleProject: GoogleProject, bucketName: GcsBucketName, isRecursive: Boolean, bucketSourceOptions: List[Storage.BucketSourceOption], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Boolean] = ???

  override def setBucketPolicyOnly(bucketName: GcsBucketName, bucketPolicyOnlyEnabled: Boolean, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Unit] = ???

  override def setBucketLabels(bucketName: GcsBucketName, labels: Map[String, String], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Unit] = ???

  override def setIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Unit] = ???

  override def overrideIamPolicy(bucketName: GcsBucketName, roles: Map[StorageRole, NonEmptyList[Identity]], traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Policy] = ???

  override def getIamPolicy(bucketName: GcsBucketName, traceId: Option[TraceId], retryConfig: RetryConfig): fs2.Stream[F, Policy] = ???
}
