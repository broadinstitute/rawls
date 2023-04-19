package org.broadinstitute.dsde.rawls.fastpass

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.FastPassConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.fastpass.FastPassService.{
  openTelemetryTags,
  policyBindingsQuotaLimit,
  RemovalFailure,
  SAdomain,
  UserAndPetEmails
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
import slick.dbio.DBIO
import spray.json._

import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.matching.Regex

object FastPassService extends LazyLogging {
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
  )(ctx: RawlsRequestContext, dataSource: SlickDataSource)(implicit
    executionContext: ExecutionContext,
    openTelemetry: OpenTelemetryMetrics[IO]
  ): FastPassService =
    new FastPassService(
      ctx,
      dataSource,
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

  // Copied from https://github.com/broadinstitute/sam/blob/d9b1fda2273ee76de717f8bf932ed8d01b817340/src/main/scala/org/broadinstitute/dsde/workbench/sam/api/StandardSamUserDirectives.scala#L80
  val SAdomain: Regex = "(\\S+@\\S*gserviceaccount\\.com$)".r

  val openTelemetryTags: Map[String, String] = Map("service" -> "FastPassService")

  type RemovalFailure = (Throwable, Seq[FastPassGrant])

  /**
    * Remove the FastPass grants in a Google Project.
    *
    * This method will remove all FastPass grants that it can, collecting errors as it goes.
    * Once its done processing FastPass grant removals, it will look for any errors that occurred, and if found,
    * return them along with the FastPassGrants that failed removal.
    *
    * This method is set up to only remove FastPass grants from the DB if IAM Policy removals in Google succeed.
    * Any failed IAM Policy removals will remain present in the DB.
    *
    * @param fastPassGrants FastPass grants that all belong to the same Google Project.
    *                       These grants can be project or bucket grants.
    * @param googleProjectId Google Project ID for the FastPass grants. For Project grants, the Google Project ID is
    *                        the Google Project in which those grants exist in.
    *                        For Bucket grants, the Google Project ID is used as the Requester Pays user project.
    * @param dataAccess Database access
    * @param googleIamDAO Google IAM API access
    * @param googleStorageDAO Google Storage API access
    * @param optCtx An optional RawlsRequestContext for tracing
    * @param executionContext An implicit Execution Context for Futures
    * @param openTelemetry An implicit OpenTelemetryMetrics for, well, metrics.
    * @return
    */
  def removeFastPassGrantsInWorkspaceProject(fastPassGrants: Seq[FastPassGrant],
                                             googleProjectId: GoogleProjectId,
                                             dataAccess: DataAccess,
                                             googleIamDAO: GoogleIamDAO,
                                             googleStorageDAO: GoogleStorageDAO,
                                             optCtx: Option[RawlsRequestContext]
  )(implicit
    executionContext: ExecutionContext,
    openTelemetry: OpenTelemetryMetrics[IO]
  ): ReadWriteAction[Seq[RemovalFailure]] = {

    type RemovalResult = Either[RemovalFailure, Seq[FastPassGrant]]

    logger.info(
      s"Removing ${fastPassGrants.size} FastPass grants in Google Project: ${googleProjectId.value}"
    )
    val grantsByUserAndResource =
      fastPassGrants.groupBy(g => (g.accountEmail, g.accountType, g.resourceType, g.resourceName))
    val iamUpdates: Seq[() => Future[RemovalResult]] = grantsByUserAndResource.toSeq.map { grouped =>
      val ((accountEmail, accountType, resourceType, resourceName), grants) = grouped
      val organizationRoles = grants.map(_.organizationRole).toSet
      val workbenchEmail = WorkbenchEmail(accountEmail.value)
      val googleProject = GoogleProject(googleProjectId.value)
      () =>
        removeFastPassGrants(resourceType,
                             resourceName,
                             workbenchEmail,
                             accountType,
                             organizationRoles,
                             googleProject,
                             googleIamDAO,
                             googleStorageDAO
        ).transform {
          case Failure(e) =>
            logger.error(
              s"Encountered an error while removing FastPass grants for ${accountEmail.value} in ${googleProjectId.value}",
              e
            )
            openTelemetry.incrementCounter("fastpass-removal-failure").unsafeRunSync()
            Success(Left((e, grants)))
          case Success(_) => Success(Right(grants))
        }
    }
    for {
      // this `flatMap` is necessary to run the IAM Updates in sequence, as to not sent conflicting policy updates to GCP
      iamRemovals <-
        DBIO.from(
          iamUpdates
            .foldLeft(Future.successful[Seq[RemovalResult]](Seq.empty[RemovalResult]))((a, b) =>
              a.flatMap(results => b().map(_ +: results))
            )
        )
      successfulIamRemovals = iamRemovals.collect { case Right(seq) => seq }.flatten
      failedIamRemovals = iamRemovals.collect { case Left(e) => e }
      _ <- removeGrantsFromDb(successfulIamRemovals.map(_.id), dataAccess, optCtx)
    } yield failedIamRemovals
  }

  private def removeFastPassGrants(resourceType: IamResourceType,
                                   resourceName: String,
                                   workbenchEmail: WorkbenchEmail,
                                   memberType: IamMemberType,
                                   organizationRoles: Set[String],
                                   googleProject: GoogleProject,
                                   googleIamDAO: GoogleIamDAO,
                                   googleStorageDAO: GoogleStorageDAO
  )(implicit executionContext: ExecutionContext, openTelemetry: OpenTelemetryMetrics[IO]): Future[Unit] = {
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

  protected def removeGrantsFromDb(ids: Seq[Long], dataAccess: DataAccess, optCtx: Option[RawlsRequestContext] = None)(
    implicit executionContext: ExecutionContext
  ): ReadWriteAction[Boolean] =
    optCtx match {
      case Some(ctx) =>
        traceDBIOWithParent("deleteFastPassGrantsFromDb", ctx)(_ => dataAccess.fastPassGrantQuery.deleteMany(ids))
      case None => dataAccess.fastPassGrantQuery.deleteMany(ids)
    }

  /**
    * org.broadinstitute.dsde.rawls.dataaccess.GoogleServicesDAO#getUserInfoUsingJson(java.lang.String) doesn't actually
    * return the email of the pet account in its UserInfo response, so we need to parse it ourselves.
    *
    * @param petSaKey
    * @return The Pet Account Email
    */
  def getEmailFromPetSaKey(petSaKey: String): WorkbenchEmail =
    WorkbenchEmail(petSaKey.parseJson.asJsObject.fields("client_email").asInstanceOf[JsString].value)

  private case class UserAndPetEmails(userEmail: WorkbenchEmail, userType: IamMemberType, petEmail: WorkbenchEmail) {
    override def toString: String = s"${userType.value}:${userEmail.value} and Pet:${petEmail.value}"
  }
}

class FastPassService(protected val ctx: RawlsRequestContext,
                      protected val dataSource: SlickDataSource,
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
      case SamWorkspaceRoles.projectOwner                           => Set(terraBucketWriterRole)
      case SamWorkspaceRoles.owner                                  => Set(terraBucketWriterRole)
      case SamWorkspaceRoles.writer | SamWorkspaceRoles.shareWriter => Set(terraBucketWriterRole)
      case SamWorkspaceRoles.reader | SamWorkspaceRoles.shareReader => Set(terraBucketReaderRole)
      case _                                                        => Set.empty[String]
    }

  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace, childWorkspace: Workspace): Future[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${parentWorkspace.toWorkspaceName}")
      return Future.successful()
    }
    dataSource
      .inTransaction { implicit dataAccess =>
        for {
          maybeUserStatus <- DBIO.from(samDAO.getUserStatus(ctx))
          if maybeUserStatus.isDefined
          samUserInfo = maybeUserStatus.map(SamUserInfo.fromSamUserStatus).orNull

          petEmail <- DBIO.from(samDAO.getUserPetServiceAccount(ctx, childWorkspace.googleProjectId))
          userType = getUserType(samUserInfo.userEmail)
          userAndPet = UserAndPetEmails(samUserInfo.userEmail, userType, petEmail)
          _ <- removeParentBucketReaderGrant(parentWorkspace, samUserInfo)
          _ <- addFastPassGrantsForRoles(samUserInfo, userAndPet, parentWorkspace, Set(SamWorkspaceRoles.reader))
        } yield ()
      }
      .transform {
        case Failure(e) =>
          logger.error(s"Failed to setup FastPass grants in cloned workspace ${parentWorkspace.toWorkspaceName}", e)
          openTelemetry.incrementCounter("fastpass-failure").unsafeRunSync()
          Success()
        case Success(_) => Success()
      }
  }
  def syncFastPassesForUserInWorkspace(workspace: Workspace): Future[Unit] =
    syncFastPassesForUserInWorkspace(workspace, ctx.userInfo.userEmail.value)

  def syncFastPassesForUserInWorkspace(workspace: Workspace, email: String): Future[Unit] = {
    if (!config.enabled) {
      logger.debug(s"FastPass is disabled. Will not grant FastPass access to ${workspace.toWorkspaceName}")
      return Future.successful()
    }
    dataSource
      .inTransaction { implicit dataAccess =>
        for {
          rawlsServiceAccountUserInfo <- DBIO.from(googleServicesDAO.getServiceAccountUserInfo())
          maybeSamUserInfo <- DBIO.from(
            samDAO.getUserIdInfo(email, RawlsRequestContext(rawlsServiceAccountUserInfo)).map {
              case SamDAO.NotFound =>
                logger.warn(s"$email not found in Sam. Cannot setup FastPass.")
                None
              case SamDAO.NotUser =>
                logger.warn(s"$email is not a user. Might be a group. Cannot setup FastPass.")
                None
              case SamDAO.User(userIdInfo) => Some(SamUserInfo.fromSamUserIdInfo(userIdInfo))
            }
          )
          if maybeSamUserInfo.isDefined
          samUserInfo = maybeSamUserInfo.orNull
          _ = logger.info(s"Syncing FastPass grants for $email in ${workspace.toWorkspaceName}")

          _ <- removeFastPassesForUserInWorkspace(workspace, samUserInfo)

          petSAJson <- DBIO.from(
            samDAO.getPetServiceAccountKeyForUser(workspace.googleProjectId,
                                                  RawlsUserEmail(samUserInfo.userEmail.value)
            )
          )
          petUserInfo <- DBIO.from(googleServicesDAO.getUserInfoUsingJson(petSAJson))
          petCtx = ctx.copy(userInfo = petUserInfo)
          samPetUserInfo <- DBIO.from(samDAO.getUserStatus(petCtx))
          if samPetUserInfo.exists(_.enabled)
          userType = getUserType(samUserInfo.userEmail)
          petEmail = FastPassService.getEmailFromPetSaKey(petSAJson)
          userAndPet = UserAndPetEmails(samUserInfo.userEmail, userType, petEmail)
          roles <- DBIO
            .from(samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, petCtx))

          _ <- addFastPassGrantsForRoles(samUserInfo, userAndPet, workspace, roles)
        } yield ()
      }
      .transform {
        case Failure(e) =>
          logger.error(s"Failed to sync FastPass grants for $email in ${workspace.toWorkspaceName}", e)
          openTelemetry.incrementCounter("fastpass-failure").unsafeRunSync()
          Success()
        case Success(_) => Success()
      }
  }

  private def addFastPassGrantsForRoles(samUserInfo: SamUserInfo,
                                        userAndPet: UserAndPetEmails,
                                        workspace: Workspace,
                                        roles: Set[SamResourceRole]
  )(implicit dataAccess: DataAccess): ReadWriteAction[Unit] =
    DBIO.from(quotaAvailableForFastPassGrants(workspace, roles)).flatMap { quotaAvailable =>
      if (quotaAvailable) {
        logger
          .info(
            s"Adding FastPass access for ${samUserInfo.userEmail.value} in workspace ${workspace.toWorkspaceName}"
          )
        val expirationDate = OffsetDateTime.now(ZoneOffset.UTC).plus(config.grantPeriod)
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
        openTelemetry.incrementCounter("fastpass-role-binding-quota-exceeded").unsafeRunSync()
        DBIO.successful()
      }
    }

  def removeFastPassGrantsForWorkspace(workspace: Workspace): Future[Unit] = {
    logger.info(
      s"Removing FastPass grants in workspace ${workspace.toWorkspaceName}"
    )

    dataSource
      .inTransaction { implicit dataAccess =>
        for {
          fastPassGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForWorkspace(workspace.workspaceIdAsUUID)
          _ <- removeFastPassGrantsInWorkspaceProject(fastPassGrants, workspace.googleProjectId)
        } yield ()
      }
      .transform {
        case Failure(e) =>
          logger.error(s"Failed to remove FastPass grants in ${workspace.toWorkspaceName}", e)
          openTelemetry.incrementCounter("fastpass-failure").unsafeRunSync()
          Success()
        case Success(_) => Success()
      }
  }

  private def removeFastPassesForUserInWorkspace(workspace: Workspace, samUserInfo: SamUserInfo)(implicit
    dataAccess: DataAccess
  ): ReadWriteAction[Unit] = {
    logger.info(s"Removing FastPass grants for ${samUserInfo.userEmail.value} in ${workspace.toWorkspaceName}")
    for {
      existingFastPassGrantsForUser <- dataAccess.fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(
        workspace.workspaceIdAsUUID,
        samUserInfo.userSubjectId
      )
      _ <- removeFastPassGrantsInWorkspaceProject(existingFastPassGrantsForUser, workspace.googleProjectId)
    } yield ()
  }

  private def removeFastPassGrantsInWorkspaceProject(fastPassGrants: Seq[FastPassGrant],
                                                     googleProjectId: GoogleProjectId
  )(implicit dataAccess: DataAccess): ReadWriteAction[Seq[RemovalFailure]] =
    FastPassService.removeFastPassGrantsInWorkspaceProject(fastPassGrants,
                                                           googleProjectId,
                                                           dataAccess,
                                                           googleIamDAO,
                                                           googleStorageDAO,
                                                           Some(ctx)
    )

  private def setupProjectRoles(workspace: Workspace,
                                samResourceRoles: Set[SamResourceRole],
                                userAndPet: UserAndPetEmails,
                                samUserInfo: SamUserInfo,
                                expirationDate: OffsetDateTime
  )(implicit dataAccess: DataAccess): ReadWriteAction[Unit] = {
    val projectIamRoles = samResourceRoles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles)
    val condition = conditionFromExpirationDate(samUserInfo, expirationDate)

    if (projectIamRoles.nonEmpty) {
      for {
        _ <- DBIO.from(
          addUserAndPetToProjectIamRoles(workspace.googleProjectId, projectIamRoles, userAndPet, condition)
        )
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
    } else {
      DBIO.successful()
    }
  }

  private def setupBucketRoles(workspace: Workspace,
                               samResourceRoles: Set[SamResourceRole],
                               userAndPet: UserAndPetEmails,
                               samUserInfo: SamUserInfo,
                               expirationDate: OffsetDateTime
  )(implicit dataAccess: DataAccess): ReadWriteAction[Unit] = {
    val bucketIamRoles = samResourceRoles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles)
    val condition = conditionFromExpirationDate(samUserInfo, expirationDate)

    if (bucketIamRoles.nonEmpty) {
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
    } else {
      DBIO.successful()
    }
  }

  protected def writeGrantsToDb(workspaceId: String,
                                userAndPet: UserAndPetEmails,
                                samUserSubjectId: WorkbenchUserId,
                                gcpResourceType: IamResourceType,
                                resourceName: String,
                                organizationRoles: Set[String],
                                expiration: OffsetDateTime
  )(implicit dataAccess: DataAccess): ReadWriteAction[Unit] = {
    val rolesToWrite =
      Seq((userAndPet.userEmail, userAndPet.userType), (userAndPet.petEmail, IamMemberTypes.ServiceAccount)).flatMap(
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
        userAndPet.userType,
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
        userAndPet.userType,
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

  private def removeParentBucketReaderGrant(parentWorkspace: Workspace, samUserInfo: SamUserInfo)(implicit
    dataAccess: DataAccess
  ): ReadWriteAction[Unit] = {
    val predicate = (g: FastPassGrant) =>
      g.resourceType.equals(IamResourceTypes.Bucket) &&
        g.resourceName.equals(parentWorkspace.bucketName) &&
        g.organizationRole.equals(terraBucketReaderRole)
    for {
      existingGrants <- dataAccess.fastPassGrantQuery.findFastPassGrantsForUserInWorkspace(
        parentWorkspace.workspaceIdAsUUID,
        samUserInfo.userSubjectId
      )
      existingBucketReaderGrant = existingGrants.filter(predicate)
      _ <- removeFastPassGrantsInWorkspaceProject(existingBucketReaderGrant, parentWorkspace.googleProjectId)
    } yield ()
  }

  /*
   * Add the number of policy bindings we are going to with the current number of policy bindings,
   * and make sure the total is below the max allowed policy bindings quota.
   */
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
      // We also have 2 role bindings per role: User and Pet
      val existingProjectRoleBindings = projectPolicy.bindings.toList.flatMap(_.members.toList).size
      val newProjectRoleBindings = roles.flatMap(samWorkspaceRoleToGoogleProjectIamRoles).size * 2
      val totalProjectBindings = existingProjectRoleBindings + newProjectRoleBindings

      val existingBucketRoleBindings = bucketPolicy.bindings.toList.flatMap(_.members.toList).size
      val newBucketRoleBindings = roles.flatMap(samWorkspaceRolesToGoogleBucketIamRoles).size * 2
      val totalBucketBindings = existingBucketRoleBindings + newBucketRoleBindings

      totalProjectBindings < policyBindingsQuotaLimit && totalBucketBindings < policyBindingsQuotaLimit
    }

  private def conditionFromExpirationDate(samUserInfo: SamUserInfo, expirationDate: OffsetDateTime): Expr =
    Expr(
      s"FastPass access for ${samUserInfo.userEmail.value} for while IAM propagates through Google Groups",
      s"""request.time < timestamp("${expirationDate.truncatedTo(ChronoUnit.SECONDS)}")""",
      null,
      s"FastPass access for ${samUserInfo.userEmail.value}"
    )

  private def getUserType(userEmail: WorkbenchEmail): IamMemberType =
    if (SAdomain.matches(userEmail.value)) {
      IamMemberTypes.ServiceAccount
    } else {
      IamMemberTypes.User
    }

  private object SamUserInfo {
    def fromSamUserStatus(samUserStatusResponse: SamUserStatusResponse) =
      SamUserInfo(WorkbenchEmail(samUserStatusResponse.userEmail), WorkbenchUserId(samUserStatusResponse.userSubjectId))

    def fromSamUserIdInfo(userIdInfo: UserIdInfo) =
      SamUserInfo(WorkbenchEmail(userIdInfo.userEmail), WorkbenchUserId(userIdInfo.userSubjectId))
  }
  private case class SamUserInfo(userEmail: WorkbenchEmail, userSubjectId: WorkbenchUserId)
}
