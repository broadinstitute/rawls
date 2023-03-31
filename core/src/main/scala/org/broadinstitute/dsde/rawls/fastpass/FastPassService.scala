package org.broadinstitute.dsde.rawls.fastpass

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.FastPassConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO.User
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.fastpass.FastPassService.{
  policyBindingsQuotaLimit,
  possibleBucketRoleBindingsPerUser,
  possibleProjectRoleBindingsPerUser
}
import org.broadinstitute.dsde.rawls.model.{
  FastPassGrant,
  GoogleProjectId,
  RawlsRequestContext,
  SamResourcePolicyName,
  SamResourceRole,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceRoles,
  UserIdInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.broadinstitute.dsde.workbench.google.HttpGoogleIamDAO.fromProjectPolicy
import org.broadinstitute.dsde.workbench.google.HttpGoogleStorageDAO.fromBucketPolicy
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.google.iam.IamResourceTypes.IamResourceType
import org.broadinstitute.dsde.workbench.model.google.iam.{Expr, IamMemberTypes, IamResourceTypes}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
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
                  terraBucketWriterRole: String
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
      terraBucketWriterRole
    )

  val policyBindingsQuotaLimit = 1500
  val possibleProjectRoleBindingsPerUser = 6 // 3 possible roles for each user and pet service account
  val possibleBucketRoleBindingsPerUser = 2 // 1 possible role for each user and pet service account

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
                      protected val terraBucketWriterRole: String
)(implicit protected val executionContext: ExecutionContext, val openTelemetry: OpenTelemetryMetrics[IO])
    extends LazyLogging {

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
          maybeUserStatus <- DBIO.from(samDAO.getUserStatus(ctx))
          if maybeUserStatus.isDefined
          samUserInfo = maybeUserStatus.map(SamUserInfo.fromSamUserStatus).orNull

          roles <- DBIO
            .from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
          petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, workspace.googleProjectId))
          userAndPet = UserAndPetEmails(samUserInfo.userEmail, petEmail)
          _ <- setupProjectRoles(workspace, roles, userAndPet, samUserInfo, expirationDate)
          _ <- setupBucketRoles(workspace, roles, userAndPet, samUserInfo, expirationDate)
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

    DBIO.from(quotaAvailableForClonedWorkspaceFastPassGrants(parentWorkspace, childWorkspace)).flatMap {
      quotaAvailable =>
        if (quotaAvailable) {
          logger.info(
            s"Adding FastPass access for ${ctx.userInfo.userEmail} in workspace being cloned ${parentWorkspace.toWorkspaceName}"
          )
          val expirationDate = DateTime.now(DateTimeZone.UTC).plus(config.grantPeriod.toMillis)
          for {
            maybeUserStatus <- DBIO.from(samDAO.getUserStatus(ctx))
            if maybeUserStatus.isDefined
            samUserInfo = maybeUserStatus.map(SamUserInfo.fromSamUserStatus).orNull

            petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, childWorkspace.googleProjectId))
            userAndPet = UserAndPetEmails(samUserInfo.userEmail, petEmail)
            _ <- setupBucketRoles(parentWorkspace,
                                  Set(SamWorkspaceRoles.reader),
                                  userAndPet,
                                  samUserInfo,
                                  expirationDate
            )
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

  def syncSamPolicyFastPassesForUser(workspace: Workspace, email: String): ReadWriteAction[Unit] = {
    logger.info(s"Syncing FastPass grants for $email in ${workspace.toWorkspaceName} because of policy changes")
    for {
      maybeSamUserInfo <- DBIO.from(samDAO.getUserIdInfo(email, ctx)).map {
        case User(userIdInfo) => Some(SamUserInfo.fromSamUserIdInfo(userIdInfo))
        case _                => None
      }
      if maybeSamUserInfo.isDefined
      samUserInfo = maybeSamUserInfo.get

      roles <- DBIO
        .from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      rolesFromSam = roles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles) ++ roles.flatMap(
        samWorkspaceRolesToGoogleBucketIamRoles
      )
      existingFastPassGrantsForUser <- dataAccess.fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(
        workspace.workspaceIdAsUUID,
        samUserInfo.userSubjectId
      )
      existingRoles = existingFastPassGrantsForUser.map(_.organizationRole).toSet

      rolesToKeep = existingRoles.intersect(rolesFromSam)
      rolesToRemove = existingRoles -- rolesToKeep
      // In the event of user going from owner -> writer we want to remove some roles but not all
      userGrantsToRemove = existingFastPassGrantsForUser.filter(grant => rolesToRemove.contains(grant.organizationRole))
      _ <- removeFastPassGrantsInWorkspaceProject(userGrantsToRemove, workspace.googleProjectId)
    } yield ()
  }

  def removeFastPassGrantsForWorkspace(workspace: Workspace): ReadWriteAction[Unit] = {
    logger.info(
      s"Removing FastPass grants in workspace ${workspace.toWorkspaceName}"
    )
    for {
      fastPassGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID)
      _ <- removeFastPassGrantsInWorkspaceProject(fastPassGrants, workspace.googleProjectId)
    } yield ()
  }

  private def removeFastPassGrantsInWorkspaceProject(fastPassGrants: Seq[FastPassGrant],
                                                     googleProjectId: GoogleProjectId
  ): ReadWriteAction[Unit] = {
    logger.info(
      s"Removing ${fastPassGrants.size} FastPass grants in Google Project: ${googleProjectId.value}"
    )
    val grantsByUserAndResource =
      fastPassGrants.groupBy(g => (g.accountEmail, g.accountType, g.resourceType, g.resourceName))
    val iamUpdates = grantsByUserAndResource.toSeq.map { grouped =>
      val ((accountEmail, accountType, resourceType, resourceName), grants) = grouped
      val organizationRoles = grants.map(_.organizationRole).toSet
      val workbenchEmail = WorkbenchEmail(accountEmail.value)
      val googleProject = GoogleProject(googleProjectId.value)
      () =>
        removeFastPassGrants(resourceType, resourceName, workbenchEmail, accountType, organizationRoles, googleProject)
    }
    for {
      // this `flatMap` is necessary to run the IAM Updates in sequence, as to not sent conflicting policy updates to GCP
      _ <- DBIO.from(iamUpdates.foldLeft(Future.successful())((a, b) => a.flatMap(_ => b())))
      _ <- DBIO.seq(fastPassGrants.map(_.id).map(removeGrantFromDb): _*)
    } yield ()
  }

  private def removeFastPassGrants(resourceType: IamResourceType,
                                   resourceName: String,
                                   workbenchEmail: WorkbenchEmail,
                                   memberType: IamMemberType,
                                   organizationRoles: Set[String],
                                   googleProject: GoogleProject
  ): Future[Unit] = {
    logger.info(
      s"Removing FastPass IAM bindings for ${workbenchEmail.value} on ${resourceType.value} $resourceName"
    )
    for {
      _ <- resourceType match {
        case IamResourceTypes.Bucket =>
          googleStorageDAO.removeIamRoles(
            GcsBucketName(resourceName),
            workbenchEmail,
            memberType,
            organizationRoles,
            userProject = Some(googleProject)
          )
        case IamResourceTypes.Project =>
          googleIamDao.removeRoles(
            googleProject,
            workbenchEmail,
            memberType,
            organizationRoles
          )
      }
      _ <- openTelemetry
        .incrementCounter(s"fastpass-iam-revoked-${memberType.value}", tags = openTelemetryTags)
        .unsafeToFuture()
    } yield ()
  }

  private def setupProjectRoles(workspace: Workspace,
                                samResourceRoles: Set[SamResourceRole],
                                userAndPet: UserAndPetEmails,
                                samUserInfo: SamUserInfo,
                                expirationDate: DateTime
  ): ReadWriteAction[Unit] = {
    val projectIamRoles = samResourceRoles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles)
    val condition = conditionFromExpirationDate(samUserInfo, expirationDate)

    for {
      _ <- DBIO.from(addUserAndPetToProjectIamRoles(workspace.googleProjectId, projectIamRoles, userAndPet, condition))
      _ <- writeGrantsToDb(
        workspace.workspaceId,
        userAndPet,
        samUserInfo.userSubjectId,
        gcpResourceType = IamResourceTypes.Project,
        workspace.googleProjectId.value,
        projectIamRoles,
        expirationDate
      )
    } yield ()
  }

  private def setupBucketRoles(workspace: Workspace,
                               samResourceRoles: Set[SamResourceRole],
                               userAndPet: UserAndPetEmails,
                               samUserInfo: SamUserInfo,
                               expirationDate: DateTime
  ): ReadWriteAction[Unit] = {
    val bucketIamRoles = samResourceRoles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles)
    val condition = conditionFromExpirationDate(samUserInfo, expirationDate)

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
        samUserInfo.userSubjectId,
        gcpResourceType = IamResourceTypes.Bucket,
        workspace.bucketName,
        bucketIamRoles,
        expirationDate
      )
    } yield ()
  }

  private def writeGrantsToDb(workspaceId: String,
                              userAndPet: UserAndPetEmails,
                              samUserSubjectId: WorkbenchUserId,
                              gcpResourceType: IamResourceType,
                              resourceName: String,
                              organizationRoles: Set[String],
                              expiration: DateTime
  ): ReadWriteAction[Unit] = {
    val rolesToWrite =
      Seq((userAndPet.userEmail, IamMemberTypes.User), (userAndPet.petEmail, IamMemberTypes.ServiceAccount)).flatMap(
        tuple => organizationRoles.map(r => (tuple._1, tuple._2, r))
      )
    DBIO.seq(rolesToWrite.map { tuple =>
      val fastPassGrant = FastPassGrant.newFastPassGrant(
        workspaceId,
        samUserSubjectId,
        WorkbenchEmail(tuple._1.value),
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
      _ <- googleIamDao.addRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.userEmail,
        IamMemberTypes.User,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleIamDao.addRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.petEmail,
        IamMemberTypes.ServiceAccount,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-pet", tags = openTelemetryTags).unsafeToFuture()
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
        IamMemberTypes.User,
        organizationRoles,
        condition = Some(condition),
        userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleStorageDAO.addIamRoles(
        gcsBucketName,
        userAndPet.petEmail,
        IamMemberTypes.ServiceAccount,
        organizationRoles,
        condition = Some(condition),
        userProject = Some(GoogleProject(googleProjectId.value))
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-pet", tags = openTelemetryTags).unsafeToFuture()
    } yield ()
  }

  /*
   * Add the number of policy bindings we are going to with the current number of policy bindings,
   * and make sure the total is below the max allowed policy bindings quota.
   */
  private def quotaAvailableForNewWorkspaceFastPassGrants(workspace: Workspace): Future[Boolean] =
    for {
      projectPolicy <- googleIamDao
        .getProjectPolicy(GoogleProject(workspace.googleProjectId.value))
        .map(fromProjectPolicy)
      bucketPolicy <- googleStorageDAO
        .getBucketPolicy(GcsBucketName(workspace.bucketName), Some(GoogleProject(workspace.googleProjectId.value)))
        .map(fromBucketPolicy)
    } yield {
      // Role binding quotas do not de-duplicate member emails, hence the conversion of Sets to Lists
      val existingProjectRoleBindings = projectPolicy.bindings.toList.flatMap(_.members.toList).size
      val expectedProjectBindings = existingProjectRoleBindings + possibleProjectRoleBindingsPerUser

      val existingBucketRoleBindings = bucketPolicy.bindings.toList.flatMap(_.members.toList).size
      val expectedBucketBindings = existingBucketRoleBindings + possibleBucketRoleBindingsPerUser

      expectedProjectBindings < policyBindingsQuotaLimit && expectedBucketBindings < policyBindingsQuotaLimit
    }

  private def quotaAvailableForClonedWorkspaceFastPassGrants(parentWorkspace: Workspace,
                                                             childWorkspace: Workspace
  ): Future[Boolean] =
    for {
      bucketPolicy <- googleStorageDAO
        .getBucketPolicy(GcsBucketName(parentWorkspace.bucketName),
                         Some(GoogleProject(childWorkspace.googleProjectId.value))
        )
        .map(fromBucketPolicy)
    } yield {
      // Role binding quotas do not de-duplicate member emails, hence the conversion of Sets to Lists
      val numBucketRoleBindings = bucketPolicy.bindings.toList.flatMap(_.members.toList).size
      val expectedBucketBindings = numBucketRoleBindings + possibleBucketRoleBindingsPerUser

      expectedBucketBindings < policyBindingsQuotaLimit
    }

  private def conditionFromExpirationDate(samUserInfo: SamUserInfo, expirationDate: DateTime): Expr =
    Expr(
      s"FastPass access for ${samUserInfo.userEmail.value} for while IAM propagates through Google Groups",
      s"""request.time < timestamp("${expirationDate.toString}")""",
      null,
      s"FastPass access for ${samUserInfo.userEmail.value}"
    )

  private case class UserAndPetEmails(userEmail: WorkbenchEmail, petEmail: WorkbenchEmail) {
    override def toString: String = s"User:${userEmail.value} and Pet:${petEmail.value}"
  }

  private object SamUserInfo {
    def fromSamUserStatus(samUserStatusResponse: SamUserStatusResponse) =
      SamUserInfo(WorkbenchEmail(samUserStatusResponse.userEmail), WorkbenchUserId(samUserStatusResponse.userSubjectId))

    def fromSamUserIdInfo(userIdInfo: UserIdInfo) =
      SamUserInfo(WorkbenchEmail(userIdInfo.userEmail), WorkbenchUserId(userIdInfo.userSubjectId))
  }
  private case class SamUserInfo(userEmail: WorkbenchEmail, userSubjectId: WorkbenchUserId)
}
