package org.broadinstitute.dsde.rawls.fastpass

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
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.workbench.google.IamModel.Expr
import org.joda.time.{DateTime, DateTimeZone}
import slick.dbio.DBIO

import scala.concurrent.{ExecutionContext, Future}

object FastPassService {
  def constructor(googleIamDao: GoogleIamDAO,
                  googleStorageDAO: GoogleStorageDAO,
                  samDAO: SamDAO,
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
      googleStorageDAO,
      samDAO,
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
                      protected val googleStorageDAO: GoogleStorageDAO,
                      protected val samDAO: SamDAO,
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

  def setupFastPassForUserInWorkspace(workspace: Workspace): ReadWriteAction[Unit] = {
    val expirationDate = DateTime.now(DateTimeZone.UTC)
    for {
      roles <- DBIO.from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      _ <- setupProjectRoles(workspace, roles, expirationDate)
      _ <- setupBucketRoles(workspace, roles, expirationDate)
    } yield ()
  }

  def deleteFastPassGrantsForWorkspace(workspace: Workspace): ReadWriteAction[Unit] =
    for {
      fastPassGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID)
      projectGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Project)
      bucketGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Bucket)
      _ <- removeProjectRoles(workspace, projectGrants)
      _ <- removeBucketRoles(workspace, bucketGrants)
    } yield ()

  private def setupProjectRoles(workspace: Workspace,
                                samResourceRoles: Set[SamResourceRole],
                                expirationDate: DateTime
  ): ReadWriteAction[Unit] = {
    val projectIamRoles = samResourceRoles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles)
    val condition = conditionFromExpirationDate(expirationDate)

    for {
      _ <- DBIO.from(addUserAndPetToProjectIamRoles(workspace.googleProjectId, projectIamRoles, condition))
      _ <- DBIO.seq(
        projectIamRoles.toList.map(projectIamRole =>
          writeGrantToDb(
            workspace.workspaceId,
            gcpResourceType = GcpResourceTypes.Project,
            workspace.googleProjectId.value,
            projectIamRole,
            expirationDate
          )
        ): _*
      )
    } yield ()
  }

  private def setupBucketRoles(workspace: Workspace,
                               samResourceRoles: Set[SamResourceRole],
                               expirationDate: DateTime
  ): ReadWriteAction[Unit] = {
    val bucketIamRoles = samResourceRoles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles)
    val condition = conditionFromExpirationDate(expirationDate)

    for {
      _ <- DBIO.from(
        addUserAndPetToBucketIamRole(workspace.googleProjectId,
                                     GcsBucketName(workspace.bucketName),
                                     bucketIamRoles,
                                     condition
        )
      )
      _ <- DBIO.seq(
        bucketIamRoles.toList.map(bucketIamRole =>
          writeGrantToDb(
            workspace.workspaceId,
            gcpResourceType = GcpResourceTypes.Project,
            workspace.googleProjectId.value,
            bucketIamRole,
            expirationDate
          )
        ): _*
      )
    } yield ()
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

  def addUserAndPetToProjectIamRoles(googleProjectId: GoogleProjectId,
                                     organizationRoles: Set[String],
                                     condition: Expr
  ): Future[Unit] =
    for {
      petEmail <- samDAO.getUserPetServiceAccount(ctx, googleProjectId)
      _ <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        WorkbenchEmail(ctx.userInfo.userEmail.value),
        MemberType.User,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        petEmail,
        MemberType.ServiceAccount,
        organizationRoles,
        condition = Some(condition)
      )
    } yield ()

  def addUserAndPetToBucketIamRole(googleProjectId: GoogleProjectId,
                                   gcsBucketName: GcsBucketName,
                                   organizationRoles: Set[String],
                                   condition: Expr
  ): Future[Unit] =
    for {
      petEmail <- samDAO.getUserPetServiceAccount(ctx, googleProjectId)
      _ <- googleStorageDAO.addIamRoles(gcsBucketName,
                                        WorkbenchEmail(ctx.userInfo.userEmail.value),
                                        MemberType.User,
                                        organizationRoles,
                                        condition = Some(condition)
      )
      _ <- googleStorageDAO.addIamRoles(gcsBucketName,
                                        petEmail,
                                        MemberType.ServiceAccount,
                                        organizationRoles,
                                        condition = Some(condition)
      )
    } yield ()

  def removeUserAndPetFromProjectIamRole(googleProjectId: GoogleProjectId, organizationRole: String): Future[Unit] =
    Future.successful()

  def removeUserAndPetFromBucketIamRole(gcsBucketName: GcsBucketName, organizationRole: String): Future[Unit] =
    Future.successful()

  private def conditionFromExpirationDate(expirationDate: DateTime): Expr =
    Expr(
      s"FastPass access to ${ctx.userInfo.userEmail} for while IAM propagates through Google Groups",
      s"""request.time < timestamp("${expirationDate.toString}")""",
      null,
      s"FastPass access for ${ctx.userInfo.userEmail}"
    )

}
