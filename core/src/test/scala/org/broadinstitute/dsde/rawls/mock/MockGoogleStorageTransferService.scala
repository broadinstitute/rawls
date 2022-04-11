package org.broadinstitute.dsde.rawls.mock

import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, ServiceAccount}

abstract class MockGoogleStorageTransferService[F[_]] extends GoogleStorageTransferService[F] {
  override def getStsServiceAccount(project: GoogleProject): F[ServiceAccount] = ???

  override def createTransferJob(jobName: GoogleStorageTransferService.JobName, jobDescription: String, projectToBill: GoogleProject, originBucket: GcsBucketName, destinationBucket: GcsBucketName, schedule: GoogleStorageTransferService.JobTransferSchedule): F[TransferTypes.TransferJob] = ???

  override def getTransferJob(jobName: GoogleStorageTransferService.JobName, project: GoogleProject): F[TransferTypes.TransferJob] = ???

  override def listTransferOperations(jobName: GoogleStorageTransferService.JobName, project: GoogleProject): F[Seq[Operation]] = ???

  override def getTransferOperation(operationName: GoogleStorageTransferService.OperationName): F[Operation] = ???
}
