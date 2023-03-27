package org.broadinstitute.dsde.rawls.fastpass

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.FastPassConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{
  FastPassGrant,
  GcpResourceTypes,
  GoogleProjectId,
  MemberTypes,
  RawlsRequestContext,
  RawlsUserEmail,
  SamResourceRole,
  SamResourceTypeNames,
  SamWorkspaceRoles,
  Workspace
}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.{MemberType => GoogleIamDAOMemberType}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.broadinstitute.dsde.rawls.model.MemberTypes.MemberType
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.workbench.google.IamModel.Expr
import org.joda.time.{DateTime, DateTimeZone}
import slick.dbio.DBIO

import scala.concurrent.{ExecutionContext, Future}

object FastPassService {
  def constructor(config: FastPassConfig,
                  googleIamDao: GoogleIamDAO,
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
      config,
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
                      protected val config: FastPassConfig,
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

  def setupFastPassForUserInNewWorkspace(workspace: Workspace): ReadWriteAction[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${workspace.toWorkspaceName}")
      return DBIO.successful()
    }
    logger.info(s"Adding FastPass access for ${ctx.userInfo.userEmail} in workspace ${workspace.toWorkspaceName}")
    val expirationDate = DateTime.now(DateTimeZone.UTC).plus(config.grantPeriod.toMillis)
    for {
      roles <- DBIO.from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, workspace.googleProjectId))
      userAndPet = UserAndPetEmails(WorkbenchEmail(ctx.userInfo.userEmail.value), petEmail)
      _ <- setupProjectRoles(workspace, roles, userAndPet, expirationDate)
      _ <- setupBucketRoles(workspace, roles, userAndPet, expirationDate)
    } yield ()
  }

  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace,
                                            childWorkspace: Workspace
  ): ReadWriteAction[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${parentWorkspace.toWorkspaceName}")
      return DBIO.successful()
    }

    logger.info(
      s"Adding FastPass access for ${ctx.userInfo.userEmail} in workspace being cloned ${parentWorkspace.toWorkspaceName}"
    )
    val expirationDate = DateTime.now(DateTimeZone.UTC).plus(config.grantPeriod.toMillis)
    for {
      petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, childWorkspace.googleProjectId))
      userAndPet = UserAndPetEmails(WorkbenchEmail(ctx.userInfo.userEmail.value), petEmail)
      _ <- setupBucketRoles(parentWorkspace, Set(SamWorkspaceRoles.reader), userAndPet, expirationDate)
    } yield ()

  }

  def deleteFastPassGrantsForWorkspace(workspace: Workspace): ReadWriteAction[Unit] = {
    logger.info(s"Removing FastPass access for ${ctx.userInfo.userEmail} in workspace ${workspace.toWorkspaceName}")
    for {
      fastPassGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID)
      projectGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Project)
      bucketGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Bucket)
      petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, workspace.googleProjectId))
      userAndPet = UserAndPetEmails(WorkbenchEmail(ctx.userInfo.userEmail.value), petEmail)
      _ <- removeProjectRoles(workspace, projectGrants, userAndPet)
      _ <- removeBucketRoles(workspace, bucketGrants, userAndPet)
    } yield ()
  }

  private def setupProjectRoles(workspace: Workspace,
                                samResourceRoles: Set[SamResourceRole],
                                userAndPet: UserAndPetEmails,
                                expirationDate: DateTime
  ): ReadWriteAction[Unit] = {
    val projectIamRoles = samResourceRoles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles)
    val condition = conditionFromExpirationDate(expirationDate)

    for {
      _ <- DBIO.from(addUserAndPetToProjectIamRoles(workspace.googleProjectId, projectIamRoles, userAndPet, condition))
      _ <- writeGrantsToDb(
        workspace.workspaceId,
        userAndPet,
        gcpResourceType = GcpResourceTypes.Project,
        workspace.googleProjectId.value,
        projectIamRoles,
        expirationDate
      )
    } yield ()
  }

  private def setupBucketRoles(workspace: Workspace,
                               samResourceRoles: Set[SamResourceRole],
                               userAndPet: UserAndPetEmails,
                               expirationDate: DateTime
  ): ReadWriteAction[Unit] = {
    val bucketIamRoles = samResourceRoles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles)
    val condition = conditionFromExpirationDate(expirationDate)

    for {
      _ <- DBIO.from(
        addUserAndPetToBucketIamRole(GcsBucketName(workspace.bucketName),
                                     bucketIamRoles,
                                     userAndPet,
                                     condition,
                                     workspace.googleProjectId
        )
      )
      _ <- writeGrantsToDb(
        workspace.workspaceId,
        userAndPet,
        gcpResourceType = GcpResourceTypes.Bucket,
        workspace.googleProjectId.value,
        bucketIamRoles,
        expirationDate
      )
    } yield ()
  }

  private def removeProjectRoles(workspace: Workspace,
                                 fastPassGrants: Seq[FastPassGrant],
                                 userAndPet: UserAndPetEmails
  ): ReadWriteAction[Unit] = {
    val projectIamRoles = fastPassGrants.map(_.organizationRole).toSet

    for {
      _ <- DBIO.from(removeUserAndPetFromProjectIamRole(workspace.googleProjectId, projectIamRoles, userAndPet))
      _ <- DBIO.seq(fastPassGrants.toList.map(fastPassGrant => removeGrantFromDb(fastPassGrant.id)): _*)
    } yield ()
  }

  private def removeBucketRoles(workspace: Workspace,
                                fastPassGrants: Seq[FastPassGrant],
                                userAndPet: UserAndPetEmails
  ): ReadWriteAction[Unit] = {
    val bucketIamRoles = fastPassGrants.map(_.organizationRole).toSet

    for {
      _ <- DBIO.from(
        removeUserAndPetFromBucketIamRole(GcsBucketName(workspace.bucketName),
                                          bucketIamRoles,
                                          userAndPet,
                                          workspace.googleProjectId
        )
      )
      _ <- DBIO.seq(fastPassGrants.map(fastPassGrant => removeGrantFromDb(fastPassGrant.id)): _*)
    } yield ()
  }

  private def writeGrantsToDb(workspaceId: String,
                              userAndPet: UserAndPetEmails,
                              gcpResourceType: GcpResourceType,
                              resourceName: String,
                              organizationRoles: Set[String],
                              expiration: DateTime
  ): ReadWriteAction[Unit] = {
    val rolesToWrite =
      Seq((userAndPet.userEmail, MemberTypes.User), (userAndPet.petEmail, MemberTypes.ServiceAccount)).flatMap(tuple =>
        organizationRoles.map(r => (tuple._1, tuple._2, r))
      )
    DBIO.seq(rolesToWrite.map { tuple =>
      val fastPassGrant = FastPassGrant.newFastPassGrant(
        workspaceId,
        ctx.userInfo.userSubjectId,
        RawlsUserEmail(tuple._1.value),
        tuple._2,
        gcpResourceType,
        resourceName,
        tuple._3,
        expiration
      )
      traceDBIOWithParent("insertFastPassGrantToDb", ctx)(_ => dataAccess.fastPassGrantQuery.insert(fastPassGrant))
        .map(_ => ())
    }: _*)
  }

  private def removeGrantFromDb(id: Long): ReadWriteAction[Boolean] =
    traceDBIOWithParent("deleteFastPassGrantFromDb", ctx)(_ => dataAccess.fastPassGrantQuery.delete(id))

  private def addUserAndPetToProjectIamRoles(googleProjectId: GoogleProjectId,
                                             organizationRoles: Set[String],
                                             userAndPet: UserAndPetEmails,
                                             condition: Expr
  ): Future[Unit] = {
    logger.info(
      s"Adding project-level FastPass access for $userAndPet in $googleProjectId [${organizationRoles.mkString(" ")}]"
    )
    for {
      _ <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.userEmail,
        GoogleIamDAOMemberType.User,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.petEmail,
        GoogleIamDAOMemberType.ServiceAccount,
        organizationRoles,
        condition = Some(condition)
      )
    } yield ()
  }

  private def removeUserAndPetFromProjectIamRole(googleProjectId: GoogleProjectId,
                                                 organizationRoles: Set[String],
                                                 userAndPet: UserAndPetEmails
  ): Future[Unit] = {
    logger.info(
      s"Removing project-level FastPass access for $userAndPet in $googleProjectId [${organizationRoles.mkString(" ")}]"
    )

    for {
      _ <- googleIamDao.removeIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.userEmail,
        GoogleIamDAOMemberType.User,
        organizationRoles
      )
      _ <- googleIamDao.removeIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.petEmail,
        GoogleIamDAOMemberType.ServiceAccount,
        organizationRoles
      )
    } yield ()
  }

  private def addUserAndPetToBucketIamRole(gcsBucketName: GcsBucketName,
                                           organizationRoles: Set[String],
                                           userAndPet: UserAndPetEmails,
                                           condition: Expr,
                                           googleProjectId: GoogleProjectId
  ): Future[Unit] = {
    logger.info(
      s"Adding bucket-level FastPass access for $userAndPet in $gcsBucketName [${organizationRoles.mkString(" ")}]"
    )
    for {
      _ <- googleStorageDAO.addIamRoles(
        gcsBucketName,
        userAndPet.userEmail,
        GoogleIamDAOMemberType.User,
        organizationRoles,
        condition = Some(condition),
        userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- googleStorageDAO.addIamRoles(
        gcsBucketName,
        userAndPet.petEmail,
        GoogleIamDAOMemberType.ServiceAccount,
        organizationRoles,
        condition = Some(condition),
        userProject = Some(GoogleProject(googleProjectId.value))
      )
    } yield ()
  }

  private def removeUserAndPetFromBucketIamRole(gcsBucketName: GcsBucketName,
                                                organizationRoles: Set[String],
                                                userAndPet: UserAndPetEmails,
                                                googleProjectId: GoogleProjectId
  ): Future[Unit] = {
    logger.info(
      s"Removing bucket-level FastPass access for $userAndPet in $gcsBucketName [${organizationRoles.mkString(" ")}]"
    )
    for {
      _ <- googleStorageDAO.removeIamRoles(gcsBucketName,
                                           userAndPet.userEmail,
                                           GoogleIamDAOMemberType.User,
                                           organizationRoles,
                                           userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- googleStorageDAO.removeIamRoles(gcsBucketName,
                                           userAndPet.petEmail,
                                           GoogleIamDAOMemberType.ServiceAccount,
                                           organizationRoles,
                                           userProject = Some(GoogleProject(googleProjectId.value))
      )
    } yield ()
  }

  private def conditionFromExpirationDate(expirationDate: DateTime): Expr =
    Expr(
      s"FastPass access for ${ctx.userInfo.userEmail.value} for while IAM propagates through Google Groups",
      s"""request.time < timestamp("${expirationDate.toString}")""",
      null,
      s"FastPass access for ${ctx.userInfo.userEmail.value}"
    )

  private case class UserAndPetEmails(userEmail: WorkbenchEmail, petEmail: WorkbenchEmail) {
    override def toString: String = s"User:$userEmail and Pet:$petEmail"
  }

}
