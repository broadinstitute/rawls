package org.broadinstitute.dsde.rawls.fastpass

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.FastPassConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
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
  RawlsUserEmail,
  SamResourceRole,
  SamResourceTypeNames,
  SamUserStatusResponse,
  SamWorkspaceRoles,
  UserIdInfo,
  UserInfo,
  Workspace,
  WorkspaceACL,
  WorkspaceAccessLevels
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
                  googleIamDAO: GoogleIamDAO,
                  googleStorageDAO: GoogleStorageDAO,
                  googleServicesDAO: GoogleServicesDAO,
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
      googleIamDAO,
      googleStorageDAO,
      googleServicesDAO,
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
                      protected val googleIamDAO: GoogleIamDAO,
                      protected val googleStorageDAO: GoogleStorageDAO,
                      protected val googleServicesDAO: GoogleServicesDAO,
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

    for {
      maybeUserStatus <- DBIO.from(samDAO.getUserStatus(ctx))
      if maybeUserStatus.isDefined
      samUserInfo = maybeUserStatus.map(SamUserInfo.fromSamUserStatus).orNull

      roles <- DBIO
        .from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, workspace.googleProjectId))
      userAndPet = UserAndPetEmails(samUserInfo.userEmail, petEmail)
      _ <- addFastPassGrantsForRoles(samUserInfo, userAndPet, workspace, roles)
    } yield ()
  }

  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace,
                                            childWorkspace: Workspace
  ): ReadWriteAction[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${parentWorkspace.toWorkspaceName}")
      return DBIO.successful()
    }
    for {
      maybeUserStatus <- DBIO.from(samDAO.getUserStatus(ctx))
      if maybeUserStatus.isDefined
      samUserInfo = maybeUserStatus.map(SamUserInfo.fromSamUserStatus).orNull

      petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, childWorkspace.googleProjectId))
      userAndPet = UserAndPetEmails(samUserInfo.userEmail, petEmail)
      _ <- addFastPassGrantsForRoles(samUserInfo, userAndPet, parentWorkspace, Set(SamWorkspaceRoles.reader))
    } yield ()
  }

  def syncFastPassesForUserInWorkspace(workspace: Workspace, email: String): ReadWriteAction[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${workspace.toWorkspaceName}")
      return DBIO.successful()
    }
    for {
      maybeSamUserInfo <- DBIO.from(samDAO.getUserIdInfo(email, ctx)).map {
        case User(userIdInfo) => Some(SamUserInfo.fromSamUserIdInfo(userIdInfo))
        case _                => None
      }
      if maybeSamUserInfo.isDefined
      samUserInfo = maybeSamUserInfo.get

      _ = logger.info(s"Syncing FastPass grants for $email in ${workspace.toWorkspaceName}")

      _ <- removeFastPassesForUserInWorkspace(workspace, samUserInfo)
      petSAJson <- DBIO.from(
        samDAO.getPetServiceAccountKeyForUser(workspace.googleProjectId, RawlsUserEmail(samUserInfo.userEmail.value))
      )
      petUserInfo <- DBIO.from(googleServicesDAO.getUserInfoUsingJson(petSAJson))
      petCtx = ctx.copy(userInfo = petUserInfo)

      userAndPet = UserAndPetEmails(samUserInfo.userEmail, WorkbenchEmail(petUserInfo.userEmail.value))

      roles <- DBIO
        .from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, petCtx))
      _ <- addFastPassGrantsForRoles(samUserInfo, userAndPet, workspace, roles)
    } yield ()
  }

  private def addFastPassGrantsForRoles(samUserInfo: SamUserInfo,
                                        userAndPet: UserAndPetEmails,
                                        workspace: Workspace,
                                        roles: Set[SamResourceRole]
  ): ReadWriteAction[Unit] =
    DBIO.from(quotaAvailableForFastPassGrants(workspace, roles)).flatMap { quotaAvailable =>
      if (quotaAvailable) {
        logger
          .info(s"Adding FastPass access for ${samUserInfo.userEmail.value} in workspace ${workspace.toWorkspaceName}")
        val expirationDate = DateTime.now(DateTimeZone.UTC).plus(config.grantPeriod.toMillis)
        for {
          _ <- setupProjectRoles(workspace, roles, userAndPet, samUserInfo, expirationDate)
          _ <- setupBucketRoles(workspace, roles, userAndPet, samUserInfo, expirationDate)
          _ <- DBIO
            .from(openTelemetry.incrementCounter("fastpass-granted-user", tags = openTelemetryTags).unsafeToFuture())
        } yield ()
      } else {
        logger.info(
          s"Not enough IAM Policy Role Binding quota available to add FastPass access for ${samUserInfo.userEmail.value} in workspace ${workspace.toWorkspaceName}"
        )
        DBIO.successful()
      }
    }

  def removeFastPassesForUserInWorkspace(workspace: Workspace, samUserInfo: SamUserInfo): ReadWriteAction[Unit] = {
    logger.info(s"Removing FastPass grants for ${samUserInfo.userEmail.value} in ${workspace.toWorkspaceName}")
    for {
      existingFastPassGrantsForUser <- dataAccess.fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(
        workspace.workspaceIdAsUUID,
        samUserInfo.userSubjectId
      )
      _ <- removeFastPassGrantsInWorkspaceProject(existingFastPassGrantsForUser, workspace.googleProjectId)
      _ <- DBIO
        .from(openTelemetry.incrementCounter("fastpass-revoked-user", tags = openTelemetryTags).unsafeToFuture())
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
          googleIamDAO.removeRoles(
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
      _ <- googleIamDAO.addRoles(
        GoogleProject(googleProjectId.value),
        userAndPet.userEmail,
        IamMemberTypes.User,
        organizationRoles,
        condition = Some(condition)
      )
      _ <- openTelemetry.incrementCounter("fastpass-iam-granted-user", tags = openTelemetryTags).unsafeToFuture()
      _ <- googleIamDAO.addRoles(
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
  private def quotaAvailableForFastPassGrants(workspace: Workspace, roles: Set[SamResourceRole]): Future[Boolean] =
    for {
      projectPolicy <- googleIamDAO
        .getProjectPolicy(GoogleProject(workspace.googleProjectId.value))
        .map(fromProjectPolicy)
      bucketPolicy <- googleStorageDAO
        .getBucketPolicy(GcsBucketName(workspace.bucketName), Some(GoogleProject(workspace.googleProjectId.value)))
        .map(fromBucketPolicy)
    } yield {
      // Role binding quotas do not de-duplicate member emails, hence the conversion of Sets to Lists
      val existingProjectRoleBindings = projectPolicy.bindings.toList.flatMap(_.members.toList).size
      val newProjectRoleBindings = roles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles).size
      val totalProjectBindings = existingProjectRoleBindings + newProjectRoleBindings

      val existingBucketRoleBindings = bucketPolicy.bindings.toList.flatMap(_.members.toList).size
      val newBucketRoleBindings = roles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles).size
      val totalBucketBindings = existingBucketRoleBindings + newBucketRoleBindings

      totalProjectBindings < policyBindingsQuotaLimit && totalBucketBindings < policyBindingsQuotaLimit
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
