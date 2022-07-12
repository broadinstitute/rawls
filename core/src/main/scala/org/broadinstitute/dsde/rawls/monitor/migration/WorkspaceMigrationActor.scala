package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.Applicative
import cats.Invariant.catsApplicativeForArrow
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.implicits.{global => ioruntime}
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.Identity.serviceAccount
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.{BucketGetOption, BucketSourceOption, BucketTargetOption}
import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import net.ceedubs.ficus.Ficus.{finiteDurationReader, toFicusConfig}
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success, toTuple}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectDeletionOption.DeleteSourceObjectsAfterTransfer
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobTransferOptions, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService, StorageRole}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._


/**
 * The `WorkspaceMigrationActor` is an Akka Typed Actor [1] that migrates a v1 Workspace to a
 * "rawls" v2 Workspace by moving the Workspace's Google Cloud resources into a dedicated
 * Google Project.
 *
 * The actor migrates workspaces by executing a series of operations (or pipeline) successively.
 * These operations are self-contained units defined by some initial and final state in which the
 * actor
 *  - reads a migration attempt from the database in some initial state
 *  - performs some action to advance its state
 *  - writes the new state back to the database.
 *
 * Thus, each operation can be executed independently; successive pipeline execution will eventually
 * migrate a Workspace successfully or report a failure. In any particular invocation, the actor
 * might do any and all of
 *  - update a Workspace record with a new Google Project and unlock the Workspace
 *  - start Storage Transfer jobs to "move" a Workspace's storage bucket to another Google Project
 *  - configure another Google Project for a Workspace
 *  - mark a migration attempt as a failure with an appropriate cause
 *  - start migrating a workspace, if one has been scheduled for migration
 *
 * Between migrating Workspaces, the actor maintains a table of Storage Transfer jobs that transfer
 * a Workspace's bucket to another bucket. When a job completes, the actor updates the associated
 * migration attempt with the outcome of the job. You should be aware of the Storage Transfer
 * Service quotas and limits [2] when configuring the actor.
 *
 * [1]: https://doc.akka.io/docs/akka/2.5.32/typed-actors.html
 * [2]: https://cloud.google.com/storage-transfer/quotas
 */
object WorkspaceMigrationActor {

  final case class Config
  (
    /** The interval between pipeline invocations. */
    pollingInterval: FiniteDuration,

    /** The interval between updating the status of ongoing storage transfer jobs. */
    transferJobRefreshInterval: FiniteDuration,

    /** The Google Project to bill all cloud operations to. */
    googleProjectToBill: GoogleProject,

    /** The parent folder to move re-purposed v1 Billing Project Google Projects into. */
    googleProjectParentFolder: GoogleFolderId,

    /** The maximum number of migration attempts that can be active at any one time. */
    maxConcurrentMigrationAttempts: Int
  )


  implicit val configReader: ValueReader[Config] = ValueReader.relative { config =>
    Config(
      pollingInterval = config.as[FiniteDuration]("polling-interval"),
      transferJobRefreshInterval = config.as[FiniteDuration]("transfer-job-refresh-interval"),
      googleProjectToBill = GoogleProject(config.getString("google-project-id-to-bill")),
      googleProjectParentFolder = GoogleFolderId(config.getString("google-project-parent-folder-id")),
      maxConcurrentMigrationAttempts = config.getInt("max-concurrent-migrations")
    )
  }


  final case class MigrationDeps(dataSource: SlickDataSource,
                                 googleProjectToBill: GoogleProject,
                                 parentFolder: GoogleFolderId,
                                 maxConcurrentAttempts: Int,
                                 workspaceService: UserInfo => WorkspaceService,
                                 storageService: GoogleStorageService[IO],
                                 storageTransferService: GoogleStorageTransferService[IO],
                                 gcsDao: GoogleServicesDAO,
                                 googleIamDAO: GoogleIamDAO,
                                 samDao: SamDAO
                                )


  type MigrateAction[A] = ReaderT[OptionT[IO, *], MigrationDeps, A]

  object MigrateAction {

    final def apply[A](f: MigrationDeps => OptionT[IO, A]): MigrateAction[A] =
      ReaderT { f }

    // lookup a value in the environment using `selector`
    final def asks[A](selector: MigrationDeps => A): MigrateAction[A] =
      ReaderT.ask[OptionT[IO, *], MigrationDeps].map(selector)

    final def fromFuture[A](future: => Future[A]): MigrateAction[A] =
      MigrateAction.liftIO(future.io)

    // create a MigrateAction that ignores its input and returns the OptionT
    final def liftF[A](optionT: OptionT[IO, A]): MigrateAction[A] =
      ReaderT.liftF(optionT)

    // lift an IO action into the context of a MigrateAction
    final def liftIO[A](ioa: IO[A]): MigrateAction[A] =
      ReaderT.liftF(OptionT.liftF(ioa))

    // modify the environment that action is evaluated in
    final def local[A](f: MigrationDeps => MigrationDeps)(action: MigrateAction[A]): MigrateAction[A] =
      ReaderT.local(f)(action)

    // Raises the error when the condition is true, otherwise returns unit
    final def raiseWhen(condition: Boolean)(t: => Throwable): MigrateAction[Unit] =
      MigrateAction.liftIO(IO.raiseWhen(condition)(t))

    // empty action
    final def unit: MigrateAction[Unit] =
      pure()

    final def pure[A](a: A): MigrateAction[A] =
      ReaderT.pure(a)

    final def ifM[A](predicate: MigrateAction[Boolean])
                    (ifTrue: => MigrateAction[A], ifFalse: => MigrateAction[A]): MigrateAction[A] =
      predicate.ifM(ifTrue, ifFalse)

    // Stop executing this Migrate Action
    final def pass[A]: MigrateAction[A] =
      MigrateAction.liftF(OptionT.none)
  }


  implicit class MigrateActionOps[A](action: MigrateAction[A]) {
    final def handleErrorWith(f: Throwable => MigrateAction[A]): MigrateAction[A] =
      MigrateAction { env =>
        OptionT(action.run(env).value.handleErrorWith(f(_).run(env).value))
      }
  }


  // Read workspace migrations in various states, attempt to advance their state forward by one
  // step and write the outcome of each step to the database.
  // These operations are combined backwards for testing so only one step is run at a time. This
  // has the effect of eagerly finishing migrations rather than starting new ones.
  final def migrate: MigrateAction[Unit] =
    List(
      startMigration,
      removeWorkspaceBucketIam,
      configureGoogleProject,
      createTempBucket,
      issueTransferJobToTmpBucket,
      deleteWorkspaceBucket,
      createFinalWorkspaceBucket,
      issueTransferJobToFinalWorkspaceBucket,
      deleteTemporaryBucket,
      restoreIamPoliciesAndUpdateWorkspaceRecord
    )
      .reverse
      .traverse_(runStep)


  // Sequence the action and return an empty MigrateAction if the action succeeded
  def runStep(action: MigrateAction[Unit]): MigrateAction[Unit] =
    action.mapF(optionT => OptionT(optionT.value.as(().some)))


  import slick.jdbc.MySQLProfile.api._
  val storageTransferJobs = PpwStorageTransferJobs.storageTransferJobs

  final def startMigration: MigrateAction[Unit] =
    for {
      maxAttempts <- MigrateAction.asks(_.maxConcurrentAttempts)
      now <- nowTimestamp
      _ <- inTransactionT { dataAccess =>
        import dataAccess._
        import dataAccess.workspaceMigrationQuery._
        (for {
          // Use `OptionT` to guard starting more migrations when we're at capacity and
          // to encode non-determinism in picking a workspace to migrate
          _ <- OptionT.liftF(getNumActiveMigrations).filter(_ < maxAttempts)
          (id, workspaceId, isLocked) <- OptionT(nextMigration)
          _ <- OptionT.liftF[ReadWriteAction, Int] {
            update2(id, startedCol, now, unlockOnCompletionCol, !isLocked) *>
              workspaceQuery.withWorkspaceId(workspaceId).setIsLocked(true)
          }
        } yield ()).value
      }
    } yield ()


  final def removeWorkspaceBucketIam: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.removeWorkspaceBucketIamCondition) {
      (migration, workspace) =>
        for {
          (storageService, gcsDao, googleProjectToBill) <- MigrateAction.asks { d => (d.storageService, d.gcsDao, d.googleProjectToBill) }
          _ <- MigrateAction.liftIO {
            for {
              userInfo <- gcsDao.getServiceAccountUserInfo().io
              actorSaIdentity = serviceAccount(userInfo.userEmail.value)
              _ <- storageService.overrideIamPolicy(
                GcsBucketName(workspace.bucketName),
                Map(StorageRole.StorageAdmin -> NonEmptyList.one(actorSaIdentity)),
                bucketSourceOptions = List(BucketSourceOption.userProject(googleProjectToBill.value))
              ).compile.drain
            } yield ()
          }
          now <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id, dataAccess.workspaceMigrationQuery.workspaceBucketIamRemovedCol, now.some)
          }
        } yield ()
    }


  final def configureGoogleProject: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.configureGoogleProjectCondition) {
      (migration, workspace) =>

        val isSoleWorkspaceInBillingProjectGoogleProject: MigrateAction[Boolean] =
          MigrateAction.pure(Seq(workspace.workspaceIdAsUUID) == _) ap inTransaction { dataAccess =>
            import dataAccess.{WorkspaceExtensions, workspaceQuery}
            workspaceQuery
              .withBillingProject(RawlsBillingProjectName(workspace.namespace))
              .map(_.id)
              .result
          }

        val makeError = (message: String, data: Map[String, Any]) => WorkspaceMigrationException(
          message = s"The workspace migration failed while configuring a new Google Project: $message.",
          data = Map(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName
          ) ++ data
        )

        for {
          _ <- MigrateAction.raiseWhen(workspace.billingAccountErrorMessage.isDefined) {
            makeError("a billing account error exists on workspace", Map(
              "billingAccountErrorMessage" -> workspace.billingAccountErrorMessage.get
            ))
          }

          workspaceBillingAccount = workspace.currentBillingAccountOnGoogleProject.getOrElse(
            throw makeError("no billing account on workspace", Map.empty)
          )

          // Safe to assume that this exists if the workspace exists
          billingProject <- inTransactionT {
            _.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspace.namespace))
          }

          billingProjectBillingAccount = billingProject.billingAccount.getOrElse(
            throw makeError("no billing account on billing project", Map(
              "billingProject" -> billingProject.projectName
            ))
          )

          _ <- MigrateAction.raiseWhen(billingProject.invalidBillingAccount) {
            makeError("invalid billing account on billing project", Map(
              "billingProject" -> billingProject.projectName,
              "billingProjectBillingAccount" -> billingProjectBillingAccount
            ))
          }

          _ <- MigrateAction.raiseWhen(workspaceBillingAccount != billingProjectBillingAccount) {
            makeError("billing account on workspace differs from billing account on billing project", Map(
              "workspaceBillingAccount" -> workspaceBillingAccount,
              "billingProject" -> billingProject.projectName,
              "billingProjectBillingAccount" -> billingProjectBillingAccount
            ))
          }

          gcsDao <- MigrateAction.asks(_.gcsDao)
          userInfo <- MigrateAction.fromFuture(gcsDao.getServiceAccountUserInfo())
          workspaceService <- MigrateAction.asks(_.workspaceService(userInfo))

          (googleProjectId, googleProjectNumber) <-
            MigrateAction.ifM(isSoleWorkspaceInBillingProjectGoogleProject)(
              // when there's only one v1 workspace in a v1 billing project, we can re-use the
              // google project associated with the billing project and forgo the need to transfer
              // the workspace bucket to a new google project. Thus, the billing project will become
              // a v2 billing project as its association with a google project will be removed.
              for {
                samDao <- MigrateAction.asks(_.samDao)
                // delete the google project resource in Sam while minimising length of time as admin
                _ <- samDao.asResourceAdmin(SamResourceTypeNames.billingProject,
                  billingProject.googleProjectId.value,
                  SamBillingProjectPolicyNames.owner,
                  userInfo
                ) {
                  MigrateAction.fromFuture {
                    samDao.deleteResource(SamResourceTypeNames.googleProject,
                      billingProject.googleProjectId.value,
                      userInfo
                    )
                  }
                }

                parentFolder <- MigrateAction.asks(_.parentFolder)
                _ <- Applicative[MigrateAction].unlessA(billingProject.servicePerimeter.isDefined) {
                  MigrateAction.fromFuture {
                    gcsDao.addProjectToFolder(billingProject.googleProjectId, parentFolder.value)
                  }
                }

                billingProjectPolicies <- MigrateAction.fromFuture {
                  samDao.admin.listPolicies(SamResourceTypeNames.billingProject,
                    billingProject.projectName.value,
                    userInfo
                  )
                }

                policiesAddedByDeploymentManager =
                  Set(SamBillingProjectPolicyNames.owner, SamBillingProjectPolicyNames.canComputeUser)

                _ <- removeIdentitiesFromGoogleProjectIam(
                  GoogleProject(billingProject.googleProjectId.value),
                  billingProjectPolicies
                    .filter(p => policiesAddedByDeploymentManager.contains(p.policyName))
                    .map(p => Identity.group(p.email.value))
                )

                now <- nowTimestamp

                // short-circuit the bucket creation and transfer
                _ <- inTransaction { dataAccess =>
                  import dataAccess.workspaceMigrationQuery._
                  update8(migration.id,
                    tmpBucketCreatedCol, now,
                    workspaceBucketTransferJobIssuedCol, now,
                    workspaceBucketTransferredCol, now,
                    workspaceBucketDeletedCol, now,
                    finalBucketCreatedCol, now,
                    tmpBucketTransferJobIssuedCol, now,
                    tmpBucketTransferredCol, now,
                    tmpBucketDeletedCol, now
                  )
                }

                googleProjectId = billingProject.googleProjectId
                googleProjectNumber <- billingProject.googleProjectNumber.map(MigrateAction.pure).getOrElse(
                  MigrateAction.fromFuture(gcsDao.getGoogleProject(googleProjectId).map(gcsDao.getGoogleProjectNumber))
                )
              } yield (googleProjectId, googleProjectNumber),
              MigrateAction.fromFuture {
                workspaceService.createGoogleProject(
                  billingProject,
                  // Use a combination of the workspaceId and the current workspace google project id
                  // as the the resource buffer service (RBS) idempotence token. Why? So that we can
                  // test this actor with v2 workspaces whose Google Projects have already been claimed
                  // from RBS via the WorkspaceService.
                  // The actual value doesn't matter, it just has to be different to whatever the
                  // WorkspaceService uses otherwise we'll keep getting back the same google project id.
                  // Adding on the current project id means that this call will be idempotent for all
                  // attempts at migrating a workspace (until one succeeds, then this will change).
                  rbsHandoutRequestId = workspace.workspaceId ++ workspace.googleProjectId.value
                )
              }
            )

          _ <- MigrateAction.fromFuture {
            workspaceService.setupGoogleProject(
              googleProjectId,
              billingProject,
              workspaceBillingAccount,
              workspace.workspaceId,
              workspace.toWorkspaceName
            )
          }

          configured <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            import dataAccess.workspaceMigrationQuery._
            update3(migration.id,
              newGoogleProjectIdCol, googleProjectId.value,
              newGoogleProjectNumberCol, googleProjectNumber.value,
              newGoogleProjectConfiguredCol, configured
            )
          }
        } yield ()
    }


  final def createTempBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.createTempBucketConditionCondition) {
      (migration, workspace) =>
        for {
          tmpBucketName <- MigrateAction.liftIO(randomSuffix("terra-workspace-migration-"))
          googleProjectId = migration.newGoogleProjectId.getOrElse(
            throw noGoogleProjectError(migration, workspace)
          )
          _ <- createBucketInSameRegion(
            migration,
            workspace,
            sourceGoogleProject = GoogleProject(workspace.googleProjectId.value),
            sourceBucketName = GcsBucketName(workspace.bucketName),
            destGoogleProject = GoogleProject(googleProjectId.value),
            destBucketName = GcsBucketName(tmpBucketName)
          )

          created <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update2(migration.id,
              dataAccess.workspaceMigrationQuery.tmpBucketCol, tmpBucketName.some,
              dataAccess.workspaceMigrationQuery.tmpBucketCreatedCol, created.some)
          }
        } yield ()
    }


  final def issueTransferJobToTmpBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.issueTransferJobToTmpBucketCondition) {
      (migration, workspace) =>
        for {
          tmpBucketName <- MigrateAction.liftIO(IO {
            migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration, workspace))
          })

          _ <- startBucketTransferJob(
            migration,
            workspace,
            GcsBucketName(workspace.bucketName),
            tmpBucketName
          )

          issued <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id, dataAccess.workspaceMigrationQuery.workspaceBucketTransferJobIssuedCol, issued.some)
          }
        } yield ()
    }


  final def deleteWorkspaceBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.deleteWorkspaceBucketCondition) {
      (migration, workspace) =>
        for {
          (storageService, googleProjectToBill) <- MigrateAction.asks(s => (s.storageService, s.googleProjectToBill))

          bucketInfo <- MigrateAction.liftIO {
            for {
              bucketOpt <- storageService.getBucket(
                GoogleProject(workspace.googleProjectId.value),
                GcsBucketName(workspace.bucketName),
                bucketGetOptions = List(
                  Storage.BucketGetOption.fields(Storage.BucketField.BILLING),
                  BucketGetOption.userProject(googleProjectToBill.value)),
              )
              bucketInfo <- IO.fromOption(bucketOpt)(noWorkspaceBucketError(migration, workspace))
            } yield bucketInfo
          }

          // commit requester pays state before deleting bucket in case there is a failure
          // and the bucket ends up being deleted and the not persisted which would be unrecoverable
          _ <- inTransaction { dataAccess =>
            val requesterPaysEnabled = Option(bucketInfo.requesterPays()).exists(_.booleanValue())
            dataAccess.workspaceMigrationQuery.update(migration.id,
              dataAccess.workspaceMigrationQuery.requesterPaysEnabledCol, requesterPaysEnabled)
          }

          _ <- MigrateAction.liftIO {
            storageService.deleteBucket(
              GoogleProject(workspace.googleProjectId.value),
              GcsBucketName(workspace.bucketName),
              bucketSourceOptions = List(BucketSourceOption.userProject(googleProjectToBill.value))
            ).compile.last
          }

          deleted <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id,
              dataAccess.workspaceMigrationQuery.workspaceBucketDeletedCol, deleted.some)
          }
        } yield ()
    }

  final def createFinalWorkspaceBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.createFinalWorkspaceBucketCondition) {
      (migration, workspace) =>
        for {
          (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)
          destGoogleProject = GoogleProject(googleProjectId.value)
          _ <- createBucketInSameRegion(
            migration,
            workspace,
            sourceGoogleProject = destGoogleProject,
            sourceBucketName = tmpBucketName,
            destGoogleProject = destGoogleProject,
            destBucketName = GcsBucketName(workspace.bucketName)
          )

          created <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id,
              dataAccess.workspaceMigrationQuery.finalBucketCreatedCol, created.some)
          }
        } yield ()
    }


  final def issueTransferJobToFinalWorkspaceBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.issueTransferJobToFinalWorkspaceBucketCondition) {
      (migration, workspace) =>
        for {
          tmpBucketName <- MigrateAction.liftIO(IO {
            migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration, workspace))
          })

          _ <- startBucketTransferJob(
            migration,
            workspace,
            tmpBucketName,
            GcsBucketName(workspace.bucketName)
          )

          issued <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id, dataAccess.workspaceMigrationQuery.tmpBucketTransferJobIssuedCol, issued.some)
          }
        } yield ()
    }


  final def deleteTemporaryBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.deleteTemporaryBucketCondition) {
      (migration, workspace) =>
        for {
          (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)
          (storageService, googleProjectToBill) <- MigrateAction.asks(s => (s.storageService, s.googleProjectToBill))
          successOpt <- MigrateAction.liftIO {
            storageService.deleteBucket(
              GoogleProject(googleProjectId.value),
              tmpBucketName,
              bucketSourceOptions = List(BucketSourceOption.userProject(googleProjectToBill.value))
            ).compile.last
          }

          _ <- MigrateAction.raiseWhen(!successOpt.contains(true)) {
            noTmpBucketError(migration, workspace)
          }

          deleted <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id, dataAccess.workspaceMigrationQuery.tmpBucketDeletedCol, deleted.some)
          }
        } yield ()
    }


  final def restoreIamPoliciesAndUpdateWorkspaceRecord: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.restoreIamPoliciesAndUpdateWorkspaceRecordCondition) { (migration, workspace) =>
      for {
        MigrationDeps(_, googleProjectToBill, _, _, workspaceService, storageService, _, gcsDao, _, samDao) <-
          MigrateAction.asks(identity)

        googleProjectId = migration.newGoogleProjectId.getOrElse(
          throw noGoogleProjectError(migration, workspace)
        )

        (userInfo, billingProjectOwnerPolicyGroup, workspacePolicies) <- MigrateAction.fromFuture {
          import SamBillingProjectPolicyNames.owner
          for {
            userInfo <- gcsDao.getServiceAccountUserInfo()

            billingProjectPolicies <- samDao.admin.listPolicies(
              SamResourceTypeNames.billingProject,
              workspace.namespace,
              userInfo
            )

            billingProjectOwnerPolicyGroup = billingProjectPolicies
              .find(_.policyName == owner)
              .getOrElse(throw WorkspaceMigrationException(
                message = s"""Workspace migration failed: no "$owner" policy on billing project.""",
                data = Map("migrationId" -> migration.id, "billingProject" -> workspace.namespace)
              ))
              .email

            // The `can-compute` policy group is sync'ed for v2 workspaces. This
            // was done at the billing project level only for v1 workspaces.
            _ <- samDao.syncPolicyToGoogle(SamResourceTypeNames.workspace,
              workspace.workspaceId,
              SamWorkspacePolicyNames.canCompute
            )

            workspacePolicies <- samDao.admin.listPolicies(
              SamResourceTypeNames.workspace,
              workspace.workspaceId,
              userInfo
            )

            workspacePoliciesByName = workspacePolicies.map(p => p.policyName -> p.email).toMap
            _ <- workspaceService(userInfo).setupGoogleProjectIam(
              googleProjectId,
              workspacePoliciesByName,
              billingProjectOwnerPolicyGroup
            )
          } yield (userInfo, billingProjectOwnerPolicyGroup, workspacePoliciesByName)
        }

        // Now we'll update the workspace record with the new google project id.
        // Why? Because the WorkspaceService does it in this order when creating the workspace.
        _ <- inTransaction {
          _.workspaceQuery
            .filter(_.id === workspace.workspaceIdAsUUID)
            .map(w => (w.googleProjectId, w.googleProjectNumber))
            .update((googleProjectId.value, migration.newGoogleProjectNumber.map(_.toString)))
        }

        authDomains <- MigrateAction.liftIO {
          samDao.asResourceAdmin(SamResourceTypeNames.workspace,
            workspace.workspaceId,
            SamWorkspacePolicyNames.owner,
            userInfo
          ) {
            samDao.createResourceFull(
              SamResourceTypeNames.googleProject,
              googleProjectId.value,
              Map.empty,
              Set.empty,
              userInfo,
              Some(SamFullyQualifiedResourceId(workspace.workspaceId, SamResourceTypeNames.workspace.value))
            ).io *>
              samDao.getResourceAuthDomain(
                SamResourceTypeNames.workspace,
                workspace.workspaceId,
                userInfo
              ).io
          }
        }

        // when there isn't an auth domain, the billing project owners group is used in attempt
        // to reduce an individual's google group membership below the limit of 2000.
        _ <- MigrateAction.liftIO {
          val bucketPolices = (if (authDomains.isEmpty)
            workspacePolicies.updated(SamWorkspacePolicyNames.projectOwner, billingProjectOwnerPolicyGroup) else
            workspacePolicies)
            .map { case (policyName, group) =>
              WorkspaceAccessLevels.withPolicyName(policyName.value).map(_ -> group)
            }
            .flatten
            .toMap

          val bucket = GcsBucketName(workspace.bucketName)

          gcsDao.updateBucketIam(bucket, bucketPolices).io *> storageService.setRequesterPays(bucket,
            migration.requesterPaysEnabled,
            bucketTargetOptions = List(BucketTargetOption.userProject(googleProjectToBill.value))
          ).compile.drain
        }

        _ <- inTransaction {
          _.workspaceQuery
            .filter(_.id === workspace.workspaceIdAsUUID)
            .map(w => (w.isLocked, w.workspaceVersion))
            .update((!migration.unlockOnCompletion, WorkspaceVersions.V2.value))
        }

        _ <- migrationFinished(migration.id, Success)
      } yield ()
    }


  def removeIdentitiesFromGoogleProjectIam(googleProject: GoogleProject,
                                           identities: Set[Identity]
                                           ): MigrateAction[Unit] =
    MigrateAction.asks(_.googleIamDAO).flatMap { googleIamDao =>
      MigrateAction.fromFuture {
        for {
          googleProjectPolicy <- googleIamDao.getProjectPolicy(googleProject)

          rolesToRemove = googleProjectPolicy.getBindings.asScala
            .map { b =>
              b.getMembers.asScala
                .map(Identity.valueOf)
                .filter(identities.contains)
                .map(_ -> Set(b.getRole))
                .toMap
            }
            .reduce(_ |+| _)
            .toList

          _ <- rolesToRemove.traverse_ { case (identity, roles) =>
            val Array(memberType, email) = identity.toString.split(":")
            googleIamDao.removeIamRoles(googleProject,
              WorkbenchEmail(email),
              GoogleIamDAO.MemberType.stringToMemberType(memberType),
              roles
            )
          }
        } yield ()
      }
    }


  final def createBucketInSameRegion(migration: WorkspaceMigration,
                                     workspace: Workspace,
                                     sourceGoogleProject: GoogleProject,
                                     sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProject,
                                     destBucketName: GcsBucketName)
  : MigrateAction[Unit] =
    MigrateAction { env =>

      def pollForBucketToBeCreated(interval: FiniteDuration, deadline: Deadline): IO[Unit] =
        IO.sleep(interval).whileM_ {
          for {
            _ <- IO.raiseWhen(deadline.isOverdue()) {
              WorkspaceMigrationException(
                message = "Workspace migration failed: timed out waiting for bucket creation",
                data = Map(
                  "migrationId" -> migration.id,
                  "workspace" -> workspace.toWorkspaceName,
                  "googleProject" -> destGoogleProject,
                  "bucketName" -> destBucketName
                )
              )
            }
            bucket <- env.storageService.getBucket(
              destGoogleProject,
              destBucketName,
              List(BucketGetOption.userProject(env.googleProjectToBill.value))
            )
          } yield bucket.isEmpty
        }

      OptionT.liftF {
        for {
          sourceBucketOpt <- env.storageService.getBucket(
            sourceGoogleProject,
            sourceBucketName,
            List(BucketGetOption.userProject(env.googleProjectToBill.value))
          )

          sourceBucket = sourceBucketOpt.getOrElse(
            throw WorkspaceMigrationException(
              message = "Workspace migration failed: cannot create bucket in same region as one that does not exist.",
              data = Map(
                "migrationId" -> migration.id,
                "workspace" -> workspace.toWorkspaceName,
                "sourceBucket" -> sourceBucketName
              )
            )
          )

          // todo: CA-1637 do we need to transfer the storage logs for this workspace? the logs are prefixed
          // with the ws bucket name, so we COULD do it, but do we HAVE to? it's a csv with the bucket
          // and the storage_byte_hours in it that is kept for 180 days
          _ <- env.storageService.insertBucket(
            googleProject = destGoogleProject,
            bucketName = destBucketName,
            labels = Option(sourceBucket.getLabels).map(_.asScala.toMap).getOrElse(Map.empty),
            bucketPolicyOnlyEnabled = true,
            logBucket = GcsBucketName(GoogleServicesDAO.getStorageLogsBucketName(GoogleProjectId(destGoogleProject.value))).some,
            location = Option(sourceBucket.getLocation)
          ).compile.drain

          // Poll for bucket to be created
          _ <- pollForBucketToBeCreated(interval = 100.milliseconds, deadline = 10.seconds.fromNow)
        } yield ()
      }
    }

  final def startBucketTransferJob(migration: WorkspaceMigration,
                                   workspace: Workspace,
                                   originBucket: GcsBucketName,
                                   destBucket: GcsBucketName)
  : MigrateAction[TransferJob] =
    for {
      (storageTransferService, storageService, googleProject) <- MigrateAction.asks { env =>
        (env.storageTransferService, env.storageService, env.googleProjectToBill)
      }

      serviceAccount <- MigrateAction.liftIO(storageTransferService.getStsServiceAccount(googleProject))
      serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))
      transferJob <- MigrateAction.liftIO {
        for {
          // STS requires the following to read from the origin bucket and delete objects after
          // transfer
          _ <- storageService.setIamPolicy(originBucket,
            Map(
              StorageRole.LegacyBucketWriter -> serviceAccountList,
              StorageRole.ObjectViewer -> serviceAccountList),
            bucketSourceOptions = List(BucketSourceOption.userProject(googleProject.value))
          ).compile.drain

          // STS requires the following to write to the destination bucket
          _ <- storageService.setIamPolicy(destBucket,
            Map(
              StorageRole.LegacyBucketWriter -> serviceAccountList,
              StorageRole.ObjectCreator -> serviceAccountList),
            bucketSourceOptions = List(BucketSourceOption.userProject(googleProject.value))
          ).compile.drain

          jobName <- randomSuffix("transferJobs/terra-workspace-migration-")

          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription =
              s"""Terra workspace migration transferring workspace bucket contents from "${originBucket}" to "${destBucket}"
                 |(workspace: "${workspace.toWorkspaceName}", "migration: ${migration.id}")"""".stripMargin,
            projectToBill = googleProject,
            originBucket,
            destBucket,
            JobTransferSchedule.Immediately,
            options = JobTransferOptions(whenToDelete = DeleteSourceObjectsAfterTransfer).some
          )
        } yield transferJob
      }.recoverWith {
        // If permissions changes haven't been applied to the STS service account yet, we'll stop
        // processing the current migration attempt and try to re-issue the transfer job later.
        case ex: Exception
          if Option(ex.getMessage).exists(_.contains(
            s"FAILED_PRECONDITION: Service account ${serviceAccount.email} does not have required permissions"
          )) =>
          nowTimestamp.flatMap { now =>
            inTransaction { dataAccess =>
              import dataAccess.workspaceMigrationQuery._
              // Send the record to the back of the queue by bumping its updated timestamp. This
              // maximises time between initially updating IAM policies and issuing transfer jobs.
              update(migration.id, updatedCol, now)
            }
          } *> MigrateAction.pass
      }

      _ <- inTransaction { _ =>
        storageTransferJobs
          .map(job => (job.jobName, job.migrationId, job.destBucket, job.originBucket))
          .insert((transferJob.getName, migration.id, destBucket.value, originBucket.value))
      }

    } yield transferJob


  final def refreshTransferJobs: MigrateAction[PpwStorageTransferJob] =
    for {
      transferJob <- peekTransferJob
      (storageTransferService, googleProject) <- MigrateAction.asks { env =>
        (env.storageTransferService, env.googleProjectToBill)
      }

      // Transfer operations are listed after they've been started.
      // For bucket-to-bucket transfers we expect at least one operation.
      outcome <- MigrateAction.liftF {
        OptionT {
          storageTransferService
            .listTransferOperations(transferJob.jobName, googleProject)
            .map(_.toList.foldMapK(getOperationOutcome))
        }
      }

      (status, message) = toTuple(outcome)
      finished <- nowTimestamp.map(_.some)
      _ <- inTransaction { _ =>
        storageTransferJobs
          .filter(_.id === transferJob.id)
          .map(row => (row.finished, row.outcome, row.message))
          .update(finished, status.some, message)
      }
    } yield transferJob.copy(finished = finished, outcome = outcome.some)


  final def getOperationOutcome(operation: Operation): Option[Outcome] =
    operation.getDone.some.collect { case true =>
      if (!operation.hasError)
        Success.asInstanceOf[Outcome]
      else {
        val status = operation.getError
        if (status.getDetailsCount == 0)
          Failure(status.getMessage) else
          Failure(status.getMessage ++ " : " ++ status.getDetailsList.toString)
      }
    }


  final def updateMigrationTransferJobStatus(transferJob: PpwStorageTransferJob): MigrateAction[Unit] =
    transferJob.outcome match {
      case Some(Success) => transferJobSucceeded(transferJob)
      case Some(failure) => migrationFinished(transferJob.migrationId, failure)
      case _ => MigrateAction.unit
    }


  final def transferJobSucceeded(transferJob: PpwStorageTransferJob): MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.withMigrationId(transferJob.migrationId)) { (migration, _) =>
      for {
        (storageTransferService, storageService, googleProject) <- MigrateAction.asks { env =>
          (env.storageTransferService, env.storageService, env.googleProjectToBill)
        }

        _ <- MigrateAction.liftIO {
          for {
            serviceAccount <- storageTransferService.getStsServiceAccount(googleProject)
            serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))

            _ <- storageService.removeIamPolicy(transferJob.originBucket,
              Map(
                StorageRole.LegacyBucketReader -> serviceAccountList,
                StorageRole.ObjectViewer -> serviceAccountList),
              bucketSourceOptions = List(BucketSourceOption.userProject(googleProject.value))
            ).compile.drain

            _ <- storageService.removeIamPolicy(transferJob.destBucket,
              Map(
                StorageRole.LegacyBucketWriter -> serviceAccountList,
                StorageRole.ObjectCreator -> serviceAccountList),
              bucketSourceOptions = List(BucketSourceOption.userProject(googleProject.value))
            ).compile.drain

          } yield ()
        }

        transferred <- nowTimestamp.map(_.some)
        _ <- inTransaction { dataAccess =>
          import dataAccess.workspaceMigrationQuery._
          update(migration.id,
            if (migration.workspaceBucketTransferred.isEmpty) workspaceBucketTransferredCol
            else tmpBucketTransferredCol,
            transferred
          )
        }
      } yield ()
    }


  final def migrationFinished(migrationId: Long, outcome: Outcome): MigrateAction[Unit] =
    nowTimestamp.flatMap { finished =>
      inTransaction { dataAccess =>
        dataAccess.workspaceMigrationQuery.migrationFinished(migrationId, finished, outcome).ignore
      }
    }


  final def withMigration(selectMigrations: DataAccess => ReadAction[Seq[WorkspaceMigration]])
                         (attempt: (WorkspaceMigration, Workspace) => MigrateAction[Unit])
  : MigrateAction[Unit] =
    for {
      (migration, workspace) <- inTransactionT { dataAccess =>
        (for {
          migrations <- selectMigrations(dataAccess)

          workspaces <- dataAccess
            .workspaceQuery
            .listByIds(migrations.map(_.workspaceId))

        } yield migrations.zip(workspaces)).map(_.headOption)
      }

      _ <- attempt(migration, workspace).handleErrorWith { t =>
        migrationFinished(migration.id, Failure(t.getMessage))
      }
    } yield ()


  final def peekTransferJob: MigrateAction[PpwStorageTransferJob] =
    nowTimestamp.flatMap { now =>
      inTransactionT { _ =>
        for {
          job <- storageTransferJobs
            .filter(_.finished.isEmpty)
            .sortBy(_.updated.asc)
            .take(1)
            .result
            .headOption

          // touch the job so that next call to peek returns another
          _ <- if (job.isDefined)
            storageTransferJobs.filter(_.id === job.get.id).map(_.updated).update(now) else
            DBIO.successful()

        } yield job.map(_.copy(updated = now))
      }
    }


  final def getGoogleProjectAndTmpBucket(migration: WorkspaceMigration, workspace: Workspace)
  : MigrateAction[(GoogleProjectId, GcsBucketName)] =
    MigrateAction.liftIO {
      for {
        googleProjectId <- IO.fromOption(migration.newGoogleProjectId)(noGoogleProjectError(migration, workspace))
        bucketName <- IO.fromOption(migration.tmpBucketName)(noTmpBucketError(migration, workspace))
      } yield (googleProjectId, bucketName)
    }


  final def inTransactionT[A](action: DataAccess => ReadWriteAction[Option[A]]): MigrateAction[A] =
    inTransaction(action).mapF(optT => OptionT(optT.value.map(_.flatten)))


  final def inTransaction[A](action: DataAccess => ReadWriteAction[A]): MigrateAction[A] =
    for {
      dataSource <- MigrateAction.asks(_.dataSource)
      result <- MigrateAction.liftIO(dataSource.inTransaction(action).io)
    } yield result


  final def getWorkspace(workspaceId: UUID): MigrateAction[Workspace] =
    inTransaction {
      _.workspaceQuery.findByIdOrFail(workspaceId.toString)
    }


  final def nowTimestamp: MigrateAction[Timestamp] =
    MigrateAction.liftIO(IO {
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC))
    })


  final def randomSuffix(str: String): IO[String] = IO {
    str ++ UUID.randomUUID.toString.replace("-", "")
  }


  final def noGoogleProjectError(migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Google Project not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "googleProjectId" -> migration.newGoogleProjectId
      )
    )


  final def noWorkspaceBucketError(migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Workspace cloud bucket not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "workspaceBucket" -> workspace.bucketName
      )
    )


  final def noTmpBucketError[A](migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Temporary cloud storage bucket not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "tmpBucket" -> migration.tmpBucketName
      )
    )


  sealed trait Message

  case object RunMigration extends Message

  case object RefreshTransferJobs extends Message


  def apply(actorConfig: Config,
            dataSource: SlickDataSource,
            workspaceService: UserInfo => WorkspaceService,
            storageService: GoogleStorageService[IO],
            storageTransferService: GoogleStorageTransferService[IO],
            gcsDao: GoogleServicesDAO,
            iamDao: GoogleIamDAO,
            samDao: SamDAO)
  : Behavior[Message] =
    Behaviors.setup { context =>

      def unsafeRunMigrateAction[A](action: MigrateAction[A]): Behavior[Message] = {
        try {
          action
            .run(
              MigrationDeps(
                dataSource,
                actorConfig.googleProjectToBill,
                actorConfig.googleProjectParentFolder,
                actorConfig.maxConcurrentMigrationAttempts,
                workspaceService,
                storageService,
                storageTransferService,
                gcsDao,
                iamDao,
                samDao
              )
            )
            .value
            .void
            .unsafeRunSync
        } catch {
          case failure: Throwable => context.executionContext.reportFailure(failure)
        }
        Behaviors.same
      }

      Behaviors.withTimers { scheduler =>
        scheduler.startTimerAtFixedRate(RunMigration, actorConfig.pollingInterval)
        scheduler.startTimerAtFixedRate(RefreshTransferJobs, actorConfig.transferJobRefreshInterval)

        Behaviors.receiveMessage { message =>
          unsafeRunMigrateAction {
            message match {
              case RunMigration =>
                migrate

              case RefreshTransferJobs =>
                refreshTransferJobs >>= updateMigrationTransferJobStatus
            }
          }
        }
      }
    }

}

