package org.broadinstitute.dsde.rawls.fastpass

import cats.data.NonEmptyList
import cats.effect.IO
import com.google.cloud.Identity
import com.google.cloud.Identity.serviceAccount
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{
  FastPassGrant,
  GcpResourceTypes,
  GoogleProjectId,
  RawlsRequestContext,
  SamResourceRole,
  SamResourceTypeNames,
  SamWorkspaceRoles,
  Workspace
}
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.joda.time.DateTime
import slick.dbio.DBIO

import scala.concurrent.{ExecutionContext, Future}

object FastPassService {
  def constructor(googleIamDao: GoogleIamDAO,
                  samDAO: SamDAO,
                  googleStorageService: GoogleStorageService[IO],
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  terraBucketReaderRole: String,
                  terraBucketWriterRole: String,
                  workbenchMetricBaseName: String
  )(ctx: RawlsRequestContext, dataAccess: DataAccess)(implicit executionContext: ExecutionContext): FastPassService =
    new FastPassService(
      ctx,
      dataAccess,
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
                      protected val dataAccess: DataAccess,
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

  private def samWorkspaceRoleToGoogleProjectIamRoles(samResourceRole: SamResourceRole) =
    samResourceRole match {
      case SamWorkspaceRoles.projectOwner =>
        Set(terraBillingProjectOwnerRole, terraWorkspaceCanComputeRole, terraWorkspaceNextflowRole)
      case SamWorkspaceRoles.owner      => Set(terraWorkspaceCanComputeRole, terraWorkspaceNextflowRole)
      case SamWorkspaceRoles.canCompute => Set(terraWorkspaceCanComputeRole, terraWorkspaceNextflowRole)
      case _                            => Set.empty[String]
    }

  private def samWorkspaceRolesToGoogleBucketIamRoles(samResourceRole: SamResourceRole) =
    samResourceRole match {
      case SamWorkspaceRoles.projectOwner => Set(terraBucketWriterRole)
      case SamWorkspaceRoles.owner        => Set(terraBucketWriterRole)
      case SamWorkspaceRoles.writer       => Set(terraBucketWriterRole)
      case SamWorkspaceRoles.reader       => Set(terraBucketReaderRole)
      case _                              => Set.empty[String]
    }

  def setupFastPassForUserInWorkspace(workspace: Workspace): ReadWriteAction[Unit] =
    for {
      roles <- DBIO.from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      _ <- setupProjectRoles(workspace, roles)
      _ <- setupBucketRoles(workspace, roles)
    } yield ()

  def deleteFastPassGrantsForWorkspace(workspace: Workspace): ReadWriteAction[Unit] =
    for {
      fastPassGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID)
      projectGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Project)
      bucketGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Bucket)
      _ <- removeProjectRoles(workspace, projectGrants)
      _ <- removeBucketRoles(workspace, bucketGrants)
    } yield ()

  private def setupProjectRoles(workspace: Workspace, samResourceRoles: Set[SamResourceRole]): ReadWriteAction[Unit] = {
    val projectIamRoles = samResourceRoles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles)

    DBIO.seq(
      projectIamRoles.toList.map(projectIamRole =>
        DBIO
          .from(addUserAndPetToProjectIamRole(workspace.googleProjectId, projectIamRole))
          .flatMap(expirationDate =>
            writeGrantToDb(workspace.workspaceId,
                           gcpResourceType = GcpResourceTypes.Project,
                           workspace.googleProjectId.value,
                           projectIamRole,
                           expirationDate
            )
          )
      ): _*
    )
  }

  private def setupBucketRoles(workspace: Workspace, samResourceRoles: Set[SamResourceRole]): ReadWriteAction[Unit] = {
    val bucketIamRoles = samResourceRoles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles)
    DBIO.seq(
      bucketIamRoles.toList.map(bucketIamRole =>
        DBIO
          .from(addUserAndPetToBucketIamRole(GcsBucketName(workspace.bucketName), bucketIamRole))
          .flatMap(expirationDate =>
            writeGrantToDb(workspace.workspaceId,
                           gcpResourceType = GcpResourceTypes.Bucket,
                           workspace.bucketName,
                           bucketIamRole,
                           expirationDate
            )
          )
      ): _*
    )
  }

  private def removeProjectRoles(workspace: Workspace, fastPassGrants: Seq[FastPassGrant]): ReadWriteAction[Unit] =
    DBIO.seq(
      fastPassGrants.map(fastPassGrant =>
        DBIO
          .from(removeUserAndPetFromProjectIamRole(workspace.googleProjectId, fastPassGrant.organizationRole))
          .flatMap(_ => removeGrantFromDb(fastPassGrant.id))
      ): _*
    )

  private def removeBucketRoles(workspace: Workspace, fastPassGrants: Seq[FastPassGrant]): ReadWriteAction[Unit] =
    DBIO.seq(
      fastPassGrants.map(fastPassGrant =>
        DBIO
          .from(removeUserAndPetFromBucketIamRole(GcsBucketName(workspace.bucketName), fastPassGrant.organizationRole))
          .flatMap(_ => removeGrantFromDb(fastPassGrant.id))
      ): _*
    )

  private def writeGrantToDb(workspaceId: String,
                             gcpResourceType: GcpResourceType,
                             resourceName: String,
                             organizationRole: String,
                             expiration: DateTime
  ): ReadWriteAction[Unit] = {
    val fastPassGrant = FastPassGrant.newFastPassGrant(
      workspaceId,
      ctx.userInfo.userSubjectId,
      gcpResourceType,
      resourceName,
      organizationRole,
      expiration
    )
    traceDBIOWithParent("insertFastPassGrantToDb", ctx)(_ => dataAccess.fastPassGrantQuery.insert(fastPassGrant))
      .map(_ => ())
  }

  private def removeGrantFromDb(id: Long): ReadWriteAction[Boolean] =
    traceDBIOWithParent("deleteFastPassGrantFromDb", ctx)(_ => dataAccess.fastPassGrantQuery.delete(id))

  def removeUserAndPetFromProjectIamRole(googleProjectId: GoogleProjectId, organizationRole: String): Future[Unit] =
    Future.successful()

  def removeUserAndPetFromBucketIamRole(gcsBucketName: GcsBucketName, organizationRole: String): Future[Unit] =
    Future.successful()

  def addUserAndPetToProjectIamRole(googleProjectId: GoogleProjectId, organizationRole: String): Future[DateTime] =
    // Call Sam to get user's Pet
    // Add user and pet
    Future.successful(DateTime.now().plusHours(2))
  def addUserAndPetToBucketIamRole(gcsBucketName: GcsBucketName, organizationRole: String): Future[DateTime] =
    // Call Sam to get user's Pet
    // Add user and pet to
    Future.successful(DateTime.now().plusHours(2))

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
