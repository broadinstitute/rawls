package org.broadinstitute.dsde.rawls.fastpass

import cats.data.NonEmptyList
import cats.effect.IO
import com.google.cloud.Identity
import com.google.cloud.Identity.serviceAccount
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import scala.concurrent.{ExecutionContext, Future}

object FastPassService {
  def constructor(dataSource: SlickDataSource,
                  googleIamDao: GoogleIamDAO,
                  samDAO: SamDAO,
                  googleStorageService: GoogleStorageService[IO],
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  terraBucketReaderRole: String,
                  terraBucketWriterRole: String,
                  workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext): FastPassService =
    new FastPassService(
      ctx,
      dataSource,
      googleIamDao,
      samDAO,
      googleStorageService,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      workbenchMetricBaseName
    )

}

class FastPassService(protected val ctx: RawlsRequestContext,
                      protected val dataSource: SlickDataSource,
                      protected val googleIamDao: GoogleIamDAO,
                      protected val samDAO: SamDAO,
                      protected val googleStorageService: GoogleStorageService[IO],
                      protected val terraBillingProjectOwnerRole: String,
                      protected val terraWorkspaceCanComputeRole: String,
                      protected val terraWorkspaceNextflowRole: String,
                      protected val terraBucketReaderRole: String,
                      protected val terraBucketWriterRole: String,
                      override val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends LazyLogging
    with RawlsInstrumented {
  import cats.effect.unsafe.implicits.global

  def addUserAndPetToProjectIamRole(googleProjectId: GoogleProjectId, organizationRole: String): Future[Unit] =
    for {
      petEmail <- samDAO.getUserPetServiceAccount(ctx, googleProjectId)
      policyWasUpdatedForUser <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        WorkbenchEmail(ctx.userInfo.userEmail.value),
        MemberType.User,
        Set(organizationRole)
      )
      policyWasUpdatedForPet <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        petEmail,
        MemberType.User,
        Set(organizationRole)
      )
    } yield
      if (policyWasUpdatedForUser && policyWasUpdatedForPet) {
        ()
      } else {
        throw new Exception("Failed to add user and pet to project IAM role")
      }
  def addUserAndPetToBucketIamRole(googleProjectId: GoogleProjectId,
                                   gcsBucketName: GcsBucketName,
                                   organizationRole: String
  ): Future[Unit] =
    for {
      petEmail <- samDAO.getUserPetServiceAccount(ctx, googleProjectId)
      petSaIdentity = serviceAccount(petEmail.value)
      userIdentity = Identity.user(ctx.userInfo.userEmail.value)
      _ <- googleStorageService
        .overrideIamPolicy(
          gcsBucketName,
          Map(StorageRole.CustomStorageRole(organizationRole) -> NonEmptyList.one(petSaIdentity))
        )
        .compile
        .drain
        .unsafeToFuture()

      _ <- googleStorageService
        .overrideIamPolicy(
          gcsBucketName,
          Map(StorageRole.StorageAdmin -> NonEmptyList.one(userIdentity))
        )
        .compile
        .drain
        .unsafeToFuture()
    } yield ()
}
