package org.broadinstitute.dsde.rawls.disabled

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import com.google.api.services.storage.model.{Bucket, BucketAccessControls, ObjectAccessControls, Policy => BucketPolicy}
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsLifecycleTypes.{Delete, GcsLifecycleType}
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.GcsRole
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.google.iam.Expr
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GcsObjectName, GoogleProject}

import scala.concurrent.Future


class DisabledHttpGoogleStorageDAO extends GoogleStorageDAO{

  def createBucket(billingProject: GoogleProject,
                   bucketName: GcsBucketName,
                   readers: List[GcsEntity] = List.empty,
                   owners: List[GcsEntity] = List.empty
                  ): Future[GcsBucketName] =
    throw new NotImplementedError("createBucket is not implemented for Azure.")
  def getBucket(bucketName: GcsBucketName): Future[Bucket] =
    throw new NotImplementedError("getBucket is not implemented for Azure.")
  def deleteBucket(bucketName: GcsBucketName, recurse: Boolean): Future[Unit] =
    throw new NotImplementedError("deleteBucket is not implemented for Azure.")
  def bucketExists(bucketName: GcsBucketName): Future[Boolean] =
    throw new NotImplementedError("bucketExists is not implemented for Azure.")

  def storeObject(bucketName: GcsBucketName,
                  objectName: GcsObjectName,
                  objectContents: ByteArrayInputStream,
                  objectType: String
                 ): Future[Unit] =
    throw new NotImplementedError("storeObject is not implemented for Azure.")
  override def storeObject(bucketName: GcsBucketName,
                  objectName: GcsObjectName,
                  objectContents: String,
                  objectType: String
                 ): Future[Unit] =
    throw new NotImplementedError("storeObject is not implemented for Azure.")
  def storeObject(bucketName: GcsBucketName,
                  objectName: GcsObjectName,
                  objectContents: File,
                  objectType: String
                 ): Future[Unit] =
    throw new NotImplementedError("storeObject is not implemented for Azure.")

  def removeObject(bucketName: GcsBucketName, objectName: GcsObjectName): Future[Unit] =
    throw new NotImplementedError("removeObject is not implemented for Azure.")
  def getObject(bucketName: GcsBucketName, objectName: GcsObjectName): Future[Option[ByteArrayOutputStream]] =
    throw new NotImplementedError("getObject is not implemented for Azure.")
  def objectExists(bucketName: GcsBucketName, objectName: GcsObjectName): Future[Boolean] =
    throw new NotImplementedError("objectExists is not implemented for Azure.")
  def setBucketLifecycle(bucketName: GcsBucketName,
                         lifecycleAge: Int,
                         lifecycleType: GcsLifecycleType = Delete
                        ): Future[Unit] =
    throw new NotImplementedError("setBucketLifecycle is not implemented for Azure.")
  def setObjectChangePubSubTrigger(bucketName: GcsBucketName, topicName: String, eventTypes: List[String]): Future[Unit] =
    throw new NotImplementedError("setObjectChangePubSubTrigger is not implemented for Azure.")
  def listObjectsWithPrefix(bucketName: GcsBucketName, objectNamePrefix: String): Future[List[GcsObjectName]] =
    throw new NotImplementedError("listObjectsWithPrefix is not implemented for Azure.")
  def copyObject(srcBucketName: GcsBucketName,
                 srcObjectName: GcsObjectName,
                 destBucketName: GcsBucketName,
                 destObjectName: GcsObjectName
                ): Future[Unit] =
    throw new NotImplementedError("copyObject is not implemented for Azure.")

  def setBucketAccessControl(bucketName: GcsBucketName, entity: GcsEntity, role: GcsRole): Future[Unit] =
    throw new NotImplementedError("setBucketAccessControl is not implemented for Azure.")
  def removeBucketAccessControl(bucketName: GcsBucketName, entity: GcsEntity): Future[Unit] =
    throw new NotImplementedError("removeBucketAccessControl is not implemented for Azure.")

  def setObjectAccessControl(bucketName: GcsBucketName,
                             objectName: GcsObjectName,
                             entity: GcsEntity,
                             role: GcsRole
                            ): Future[Unit] =
    throw new NotImplementedError("setObjectAccessControl is not implemented for Azure.")
  def removeObjectAccessControl(bucketName: GcsBucketName, objectName: GcsObjectName, entity: GcsEntity): Future[Unit] =
    throw new NotImplementedError("removeObjectAccessControl is not implemented for Azure.")

  def setDefaultObjectAccessControl(bucketName: GcsBucketName, entity: GcsEntity, role: GcsRole): Future[Unit] =
    throw new NotImplementedError("setDefaultObjectAccessControl is not implemented for Azure.")
  def removeDefaultObjectAccessControl(bucketName: GcsBucketName, entity: GcsEntity): Future[Unit] =
    throw new NotImplementedError("removeDefaultObjectAccessControl is not implemented for Azure.")

  def getBucketAccessControls(bucketName: GcsBucketName): Future[BucketAccessControls] =
    throw new NotImplementedError("getBucketAccessControls is not implemented for Azure.")
  def getDefaultObjectAccessControls(bucketName: GcsBucketName): Future[ObjectAccessControls] =
    throw new NotImplementedError("getDefaultObjectAccessControls is not implemented for Azure.")

  def setRequesterPays(bucketName: GcsBucketName, requesterPays: Boolean): Future[Unit] =
    throw new NotImplementedError("setRequesterPays is not implemented for Azure.")

  def addIamRoles(bucketName: GcsBucketName,
                  userEmail: WorkbenchEmail,
                  memberType: IamMemberType,
                  rolesToAdd: Set[String],
                  retryIfGroupDoesNotExist: Boolean = false,
                  condition: Option[Expr] = None,
                  userProject: Option[GoogleProject] = None
                 ): Future[Boolean] =
    throw new NotImplementedError("addIamRoles is not implemented for Azure.")

  def removeIamRoles(bucketName: GcsBucketName,
                     userEmail: WorkbenchEmail,
                     memberType: IamMemberType,
                     rolesToRemove: Set[String],
                     retryIfGroupDoesNotExist: Boolean = false,
                     userProject: Option[GoogleProject] = None
                    ): Future[Boolean] =
    throw new NotImplementedError("removeIamRoles is not implemented for Azure.")

  def getBucketPolicy(bucketName: GcsBucketName, userProject: Option[GoogleProject] = None): Future[BucketPolicy] =
    throw new NotImplementedError("getBucketPolicy is not implemented for Azure.")
}
