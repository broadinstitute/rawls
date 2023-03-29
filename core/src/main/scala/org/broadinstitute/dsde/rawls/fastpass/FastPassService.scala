package org.broadinstitute.dsde.rawls.fastpass

import cats.effect.IO
import cats.effect.unsafe.implicits.global
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
import org.broadinstitute.dsde.rawls.fastpass.FastPassService.{
  maxBucketRoleBindingsPerUser,
  maxProjectRoleBindingsPerUser,
  policyBindingsQuotaLimit
}
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.workbench.google.IamModel.Expr
import org.joda.time.{DateTime, DateTimeZone}
import slick.dbio.DBIO
import org.broadinstitute.dsde.workbench.google.HttpGoogleIamDAO.fromProjectPolicy
import org.broadinstitute.dsde.workbench.google.HttpGoogleStorageDAO.fromBucketPolicy
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

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
  )(ctx: RawlsRequestContext, dataAccess: DataAccess)(implicit
    executionContext: ExecutionContext,
    openTelemetry: OpenTelemetryMetrics[IO]
  ): FastPassService =
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

  val policyBindingsQuotaLimit = 1500
  val maxProjectRoleBindingsPerUser = 6 // 3 possible roles for each user and pet service account
  val maxBucketRoleBindingsPerUser = 2 // 1 possible role for each user and pet service account

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
)(implicit protected val executionContext: ExecutionContext, val openTelemetry: OpenTelemetryMetrics[IO])
    extends LazyLogging
    with RawlsInstrumented {

  private val openTelemetryTags: Map[String, String] = Map("service" -> "FastPassService")
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

    DBIO.from(quotaAvailableForNewWorkspaceFastPassGrants(workspace)).flatMap { quotaAvailable =>
      if (quotaAvailable) {
        logger
          .info(s"Adding FastPass access for ${ctx.userInfo.userEmail.value} in workspace ${workspace.toWorkspaceName}")
        val expirationDate = DateTime.now(DateTimeZone.UTC).plus(config.grantPeriod.toMillis)
        for {
          roles <- DBIO
            .from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
          petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, workspace.googleProjectId))
          userAndPet = UserAndPetEmails(WorkbenchEmail(ctx.userInfo.userEmail.value), petEmail)
          _ <- setupProjectRoles(workspace, roles, userAndPet, expirationDate)
          _ <- setupBucketRoles(workspace, roles, userAndPet, expirationDate)
          _ <- DBIO
            .from(openTelemetry.incrementCounter("fastpass-granted-user", tags = openTelemetryTags).unsafeToFuture())
        } yield ()
      } else {
        logger.info(
          s"Not enough IAM Policy Role Binding quota available to add FastPass access for ${ctx.userInfo.userEmail.value} in workspace ${workspace.toWorkspaceName}"
        )
        DBIO.successful()
      }
    }
  }

  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace,
                                            childWorkspace: Workspace
  ): ReadWriteAction[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${parentWorkspace.toWorkspaceName}")
      return DBIO.successful()
    }

    DBIO.from(quotaAvailableForClonedWorkspaceFastPassGrants(parentWorkspace)).flatMap { quotaAvailable =>
      if (quotaAvailable) {
        logger.info(
          s"Adding FastPass access for ${ctx.userInfo.userEmail} in workspace being cloned ${parentWorkspace.toWorkspaceName}"
        )
        val expirationDate = DateTime.now(DateTimeZone.UTC).plus(config.grantPeriod.toMillis)
        for {
          petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, childWorkspace.googleProjectId))
          userAndPet = UserAndPetEmails(WorkbenchEmail(ctx.userInfo.userEmail.value), petEmail)
          _ <- setupBucketRoles(parentWorkspace, Set(SamWorkspaceRoles.reader), userAndPet, expirationDate)
          _ <- DBIO
            .from(openTelemetry.incrementCounter("fastpass-granted-user", tags = openTelemetryTags).unsafeToFuture())
        } yield ()
      } else {
        logger.info(
          s"Not enough IAM Policy Role Binding quota available to add FastPass access for ${ctx.userInfo.userEmail.value} in parent workspace ${parentWorkspace.toWorkspaceName}"
        )
        DBIO.successful()
      }
    }
  }

  def deleteFastPassGrantsForWorkspace(workspace: Workspace): ReadWriteAction[Unit] = {
    logger.info(
      s"Removing FastPass grants in workspace ${workspace.toWorkspaceName}"
    )
    for {
      fastPassGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID)
      byUserSubjectId = fastPassGrants.groupBy(_.userSubjectId).values.toList
      _ <- DBIO.seq(byUserSubjectId.map(grouped => removeFastPassGrantsForUserInWorkspace(grouped, workspace)): _*)
    } yield ()
  }

  private def removeFastPassGrantsForUserInWorkspace(fastPassGrants: Seq[FastPassGrant],
                                                     workspace: Workspace
  ): ReadWriteAction[Unit] = {
    val userEmails =
      fastPassGrants.filter(_.accountType == MemberTypes.User).map(g => WorkbenchEmail(g.accountEmail.value)).toSet
    val petEmails = fastPassGrants
      .filter(_.accountType == MemberTypes.ServiceAccount)
      .map(g => WorkbenchEmail(g.accountEmail.value))
      .toSet
    if (userEmails.size != 1 || petEmails.size != 1) {
      throw new IllegalArgumentException("Cannot remove FastPass grants for more than one user/pet combo at a time")
    }

    val userAndPet = UserAndPetEmails(userEmails.find(_ => true).orNull, petEmails.find(_ => true).orNull)
    val projectGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Project)
    val bucketGrants = fastPassGrants.filter(fastPassGrant => fastPassGrant.resourceType == GcpResourceTypes.Bucket)

    for {
      _ <- removeProjectRoles(workspace, projectGrants, userAndPet)
      _ <- removeBucketRoles(workspace, bucketGrants, userAndPet)
      _ <- DBIO.from(openTelemetry.incrementCounter("fastpass-revoked-user", tags = openTelemetryTags).unsafeToFuture())
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
      s"Adding project-level FastPass access for $userAndPet in ${googleProjectId.value} [${organizationRoles.mkString(" ")}]"
    )
    for {
      _ <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.userEmail,
        GoogleIamDAOMemberType.User,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleIamDao.addIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.petEmail,
        GoogleIamDAOMemberType.ServiceAccount,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-pet", tags = openTelemetryTags).unsafeToFuture()
    } yield ()
  }

  private def removeUserAndPetFromProjectIamRole(googleProjectId: GoogleProjectId,
                                                 organizationRoles: Set[String],
                                                 userAndPet: UserAndPetEmails
  ): Future[Unit] = {
    logger.info(
      s"Removing project-level FastPass access for $userAndPet in ${googleProjectId.value} [${organizationRoles.mkString(" ")}]"
    )

    for {
      _ <- googleIamDao.removeIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.userEmail,
        GoogleIamDAOMemberType.User,
        organizationRoles
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-revoked-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleIamDao.removeIamRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.petEmail,
        GoogleIamDAOMemberType.ServiceAccount,
        organizationRoles
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-revoked-pet", tags = openTelemetryTags).unsafeToFuture()
    } yield ()
  }

  private def addUserAndPetToBucketIamRole(gcsBucketName: GcsBucketName,
                                           organizationRoles: Set[String],
                                           userAndPet: UserAndPetEmails,
                                           condition: Expr,
                                           googleProjectId: GoogleProjectId
  ): Future[Unit] = {
    logger.info(
      s"Adding bucket-level FastPass access for $userAndPet in ${gcsBucketName.value} [${organizationRoles.mkString(" ")}]"
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
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleStorageDAO.addIamRoles(
        gcsBucketName,
        userAndPet.petEmail,
        GoogleIamDAOMemberType.ServiceAccount,
        organizationRoles,
        condition = Some(condition),
        userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-pet", tags = openTelemetryTags).unsafeToFuture()

    } yield ()
  }

  private def removeUserAndPetFromBucketIamRole(gcsBucketName: GcsBucketName,
                                                organizationRoles: Set[String],
                                                userAndPet: UserAndPetEmails,
                                                googleProjectId: GoogleProjectId
  ): Future[Unit] = {
    logger.info(
      s"Removing bucket-level FastPass access for $userAndPet in ${gcsBucketName.value} [${organizationRoles.mkString(" ")}]"
    )
    for {
      _ <- googleStorageDAO.removeIamRoles(gcsBucketName,
                                           userAndPet.userEmail,
                                           GoogleIamDAOMemberType.User,
                                           organizationRoles,
                                           userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-revoked-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleStorageDAO.removeIamRoles(gcsBucketName,
                                           userAndPet.petEmail,
                                           GoogleIamDAOMemberType.ServiceAccount,
                                           organizationRoles,
                                           userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-revoked-pet", tags = openTelemetryTags).unsafeToFuture()
    } yield ()
  }

  private def quotaAvailableForNewWorkspaceFastPassGrants(workspace: Workspace): Future[Boolean] =
    for {
      projectPolicy <- googleIamDao
        .getProjectPolicy(GoogleProject(workspace.googleProjectId.value))
        .map(fromProjectPolicy)
      bucketPolicy <- googleStorageDAO.getBucketPolicy(GcsBucketName(workspace.bucketName)).map(fromBucketPolicy)
    } yield {
      // Role binding quotas do not de-duplicate member emails, hence the conversion of Sets to Lists
      val numProjectRoleBindings = projectPolicy.bindings.toList.flatMap(_.members.toList).size
      val expectedProjectBindings = numProjectRoleBindings + maxProjectRoleBindingsPerUser

      val numBucketRoleBindings = bucketPolicy.bindings.toList.flatMap(_.members.toList).size
      val expectedBucketBindings = numBucketRoleBindings + maxBucketRoleBindingsPerUser

      expectedProjectBindings < policyBindingsQuotaLimit && expectedBucketBindings < policyBindingsQuotaLimit
    }

  private def quotaAvailableForClonedWorkspaceFastPassGrants(workspace: Workspace): Future[Boolean] =
    for {
      bucketPolicy <- googleStorageDAO.getBucketPolicy(GcsBucketName(workspace.bucketName)).map(fromBucketPolicy)
    } yield {
      // Role binding quotas do not de-duplicate member emails, hence the conversion of Sets to Lists
      val numBucketRoleBindings = bucketPolicy.bindings.toList.flatMap(_.members.toList).size
      val expectedBucketBindings = numBucketRoleBindings + maxBucketRoleBindingsPerUser

      expectedBucketBindings < policyBindingsQuotaLimit
    }

  private def conditionFromExpirationDate(expirationDate: DateTime): Expr =
    Expr(
      s"FastPass access for ${ctx.userInfo.userEmail.value} for while IAM propagates through Google Groups",
      s"""request.time < timestamp("${expirationDate.toString}")""",
      null,
      s"FastPass access for ${ctx.userInfo.userEmail.value}"
    )

  private case class UserAndPetEmails(userEmail: WorkbenchEmail, petEmail: WorkbenchEmail) {
    override def toString: String = s"User:${userEmail.value} and Pet:${petEmail.value}"
  }

}
