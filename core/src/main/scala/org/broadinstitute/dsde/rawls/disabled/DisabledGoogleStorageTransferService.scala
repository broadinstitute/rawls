package org.broadinstitute.dsde.rawls.disabled

import cats.effect.{Resource, Sync, Temporal}

import com.google.auth.Credentials
import com.google.storagetransfer.v1.proto.TransferTypes.{TransferJob, TransferOperation}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{
  JobName,
  JobTransferOptions,
  JobTransferSchedule,
  OperationName
}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccount}
import org.typelevel.log4cats.StructuredLogger

class DisabledGoogleStorageTransferService[F[_]] extends GoogleStorageTransferService[F] {
  override def getStsServiceAccount(project: GoogleProject): F[ServiceAccount] =
    throw new NotImplementedError("getStsServiceAccount is not implemented for Azure.")

  override def createTransferJob(jobName: JobName,
                                 jobDescription: String,
                                 projectToBill: GoogleProject,
                                 originBucket: GcsBucketName,
                                 destinationBucket: GcsBucketName,
                                 schedule: JobTransferSchedule,
                                 options: Option[JobTransferOptions] = None
  ): F[TransferJob] =
    throw new NotImplementedError("createTransferJob is not implemented for Azure.")

  override def getTransferJob(jobName: JobName, project: GoogleProject): F[TransferJob] =
    throw new NotImplementedError("getTransferJob is not implemented for Azure.")

  override def listTransferOperations(jobName: JobName, project: GoogleProject): F[Seq[TransferOperation]] =
    throw new NotImplementedError("listTransferOperations is not implemented for Azure.")

  override def getTransferOperation(operationName: OperationName): F[TransferOperation] =
    throw new NotImplementedError("getTransferOperation is not implemented for Azure.")
}

object DisabledGoogleStorageTransferService {
  def resource[F[_]: Sync: Temporal: StructuredLogger]: Resource[F, GoogleStorageTransferService[F]] =
    Resource.pure[F, GoogleStorageTransferService[F]](new DisabledGoogleStorageTransferService[F])

  def resource[F[_]: Sync: Temporal: StructuredLogger](
    credential: Credentials
  ): Resource[F, GoogleStorageTransferService[F]] =
    Resource.pure[F, GoogleStorageTransferService[F]](new DisabledGoogleStorageTransferService[F])
}
