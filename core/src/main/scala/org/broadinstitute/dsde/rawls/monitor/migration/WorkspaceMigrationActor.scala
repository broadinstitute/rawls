package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.implicits.{global => ioruntime}
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.Identity.serviceAccount
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BucketGetOption
import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingProjectName, SamBillingProjectPolicyNames, SamFullyQualifiedResourceId, SamResourceTypeNames, SamWorkspacePolicyNames, UserInfo, Workspace, WorkspaceAccessLevels}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success, toTuple}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectDeletionOption.DeleteSourceObjectsAfterTransfer
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobTransferOptions, JobTransferSchedule}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService, StorageRole}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import slick.jdbc.SQLActionBuilder

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._


object WorkspaceMigrationActor {

  import slick.jdbc.MySQLProfile.api._

  val storageTransferJobs = PpwStorageTransferJobs.storageTransferJobs

  final case class MigrationDeps(dataSource: SlickDataSource,
                                 googleProjectToBill: GoogleProject,
                                 workspaceService: UserInfo => WorkspaceService,
                                 storageService: GoogleStorageService[IO],
                                 storageTransferService: GoogleStorageTransferService[IO],
                                 gcsDao: GoogleServicesDAO,
                                 samDao: SamDAO)


  type MigrateAction[A] = ReaderT[OptionT[IO, *], MigrationDeps, A]

  object MigrateAction {

    final def apply[A](f: MigrationDeps => OptionT[IO, A]): MigrateAction[A] =
      ReaderT { env => f(env) }

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
      ReaderT.pure()
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
      claimAndConfigureGoogleProject,
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


  final def startMigration: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.startCondition) { (migration, _) =>
      for {
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(migration.id, dataAccess.workspaceMigrationQuery.startedCol, now.some)
        }
      } yield ()
    }


  final def removeWorkspaceBucketIam: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.removeWorkspaceBucketIamCondition) {
      (migration, workspace) =>
        for {
          (storageService, gcsDao) <- MigrateAction.asks { d => (d.storageService, d.gcsDao) }
          _ <- MigrateAction.liftIO {
            for {
              userInfo <- gcsDao.getServiceAccountUserInfo().io
              actorSaIdentity = serviceAccount(userInfo.userEmail.value)
              _ <- storageService.overrideIamPolicy(
                GcsBucketName(workspace.bucketName),
                Map(StorageRole.StorageAdmin -> NonEmptyList.one(actorSaIdentity))
              ).compile.drain
            } yield ()
          }
          now <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update(migration.id, dataAccess.workspaceMigrationQuery.workspaceBucketIamRemovedCol, now.some)
          }
        } yield ()
    }


  final def claimAndConfigureGoogleProject: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.claimAndConfigureGoogleProjectCondition) {
      (migration, workspace) =>
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

          (gcsDao, workspaceService) <- MigrateAction.asks(d => (d.gcsDao, d.workspaceService))
          (googleProjectId, googleProjectNumber) <- MigrateAction.fromFuture {
            for {
              userInfo <- gcsDao.getServiceAccountUserInfo()
              googleInfo <- workspaceService(userInfo).setupGoogleProject(
                billingProject,
                workspaceBillingAccount,
                workspace.workspaceId,
                workspace.toWorkspaceName,
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
            } yield googleInfo
          }

          configured <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            dataAccess.workspaceMigrationQuery.update3(migration.id,
              dataAccess.workspaceMigrationQuery.newGoogleProjectIdCol, googleProjectId.value.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectNumberCol, googleProjectNumber.value.some,
              dataAccess.workspaceMigrationQuery.newGoogleProjectConfiguredCol, configured.some)
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
          storageService <- MigrateAction.asks(_.storageService)

          bucketInfo <- MigrateAction.liftIO {
            for {
              bucketOpt <- storageService.getBucket(
                GoogleProject(workspace.googleProjectId.value),
                GcsBucketName(workspace.bucketName),
                bucketGetOptions = List(Storage.BucketGetOption.fields(Storage.BucketField.BILLING))
              )
              bucketInfo <- IO.fromOption(bucketOpt)(noWorkspaceBucketError(migration, workspace))
              _ <- storageService.deleteBucket(
                GoogleProject(workspace.googleProjectId.value),
                GcsBucketName(workspace.bucketName),
                isRecursive = true
              ).compile.last
            } yield bucketInfo
          }

          deleted <- nowTimestamp
          _ <- inTransaction { dataAccess =>
            val requesterPaysEnabled = Option(bucketInfo.requesterPays()).exists(_.booleanValue())
            dataAccess.workspaceMigrationQuery.update2(migration.id,
              dataAccess.workspaceMigrationQuery.workspaceBucketDeletedCol, deleted.some,
              dataAccess.workspaceMigrationQuery.requesterPaysEnabledCol, requesterPaysEnabled)
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
          storageService <- MigrateAction.asks(_.storageService)
          successOpt <- MigrateAction.liftIO {
            storageService.deleteBucket(
              GoogleProject(googleProjectId.value),
              tmpBucketName,
              isRecursive = true
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
        MigrationDeps(_, _, workspaceService, storageService, _, gcsDao, samDao) <- MigrateAction.asks(identity)

        _ <- MigrateAction.liftIO(storageService.setRequesterPays(
          GcsBucketName(workspace.bucketName),
          migration.requesterPaysEnabled).compile.drain)

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

            workspacePolicies <- samDao
              .admin
              .listPolicies(
                SamResourceTypeNames.workspace,
                workspace.workspaceId,
                userInfo
              )
              .map(_.map(p => p.policyName -> p.email).toMap)

            _ <- workspaceService(userInfo).setupGoogleProjectIam(
              googleProjectId,
              workspacePolicies,
              billingProjectOwnerPolicyGroup
            )
          } yield (userInfo, billingProjectOwnerPolicyGroup, workspacePolicies)
        }

        // Now we'll update the workspace record with the new google project id.
        // Why? Because the WorkspaceService does it in this order when creating the workspace.
        _ <- inTransaction {
          _.workspaceQuery
            .filter(_.id === workspace.workspaceIdAsUUID)
            .map(w => (w.googleProjectId, w.googleProjectNumber))
            .update((googleProjectId.value, migration.newGoogleProjectNumber.map(_.toString)))
        }

        _ <- MigrateAction.fromFuture {
          for {
            // We need `add_child` on the workspace to set the parent of the google project
            // resource to be the workspace and to read its auth domains
            _ <- samDao.admin.addUserToPolicy(
              SamResourceTypeNames.workspace,
              workspace.workspaceId,
              SamWorkspacePolicyNames.owner,
              userInfo.userEmail.value,
              userInfo
            )

            _ <- samDao.createResourceFull(
              SamResourceTypeNames.googleProject,
              googleProjectId.value,
              Map.empty,
              Set.empty,
              userInfo,
              Some(SamFullyQualifiedResourceId(workspace.workspaceId, SamResourceTypeNames.workspace.value))
            )

            authDomains <- samDao.getResourceAuthDomain(
              SamResourceTypeNames.workspace,
              workspace.workspaceId,
              userInfo
            )

            // when there isn't an auth domain, the billing project owners group is used in attempt
            // to reduce an individual's google group membership below the limit of 2000.
            bucketPolices = (if (authDomains.isEmpty)
              workspacePolicies.updated(SamWorkspacePolicyNames.projectOwner, billingProjectOwnerPolicyGroup) else
              workspacePolicies)
              .map { case (policyName, group) =>
                WorkspaceAccessLevels.withPolicyName(policyName.value).map(_ -> group)
              }
              .flatten
              .toMap

            _ <- gcsDao.updateBucketIam(GcsBucketName(workspace.bucketName), bucketPolices)
            _ <- samDao.admin.removeUserFromPolicy(
              SamResourceTypeNames.workspace,
              workspace.workspaceId,
              SamWorkspacePolicyNames.owner,
              userInfo.userEmail.value,
              userInfo
            )
          } yield ()
        }

        _ <- inTransaction {
          _.workspaceQuery
            .filter(_.id === workspace.workspaceIdAsUUID)
            .map(_.isLocked)
            .update(!migration.unlockOnCompletion)
        }

        _ <- migrationFinished(migration.id, Success)
      } yield ()
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

      transferJob <- MigrateAction.liftIO {
        for {
          serviceAccount <- storageTransferService.getStsServiceAccount(googleProject)
          serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))

          // STS requires the following to read from the origin bucket and delete objects after
          // transfer
          _ <- storageService.setIamPolicy(originBucket, Map(
            StorageRole.LegacyBucketWriter -> serviceAccountList,
            StorageRole.ObjectViewer -> serviceAccountList
          )).compile.drain

          // STS requires the following to write to the destination bucket
          _ <- storageService.setIamPolicy(destBucket, Map(
            StorageRole.LegacyBucketWriter -> serviceAccountList,
            StorageRole.ObjectCreator -> serviceAccountList
          )).compile.drain

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
      }

      _ <- inTransaction { dataAccess =>
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

            _ <- storageService.removeIamPolicy(transferJob.originBucket, Map(
              StorageRole.LegacyBucketReader -> serviceAccountList,
              StorageRole.ObjectViewer -> serviceAccountList
            )).compile.drain

            _ <- storageService.removeIamPolicy(transferJob.destBucket, Map(
              StorageRole.LegacyBucketWriter -> serviceAccountList,
              StorageRole.ObjectCreator -> serviceAccountList
            )).compile.drain

          } yield ()
        }

        transferred <- nowTimestamp.map(_.some)
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(migration.id,
            if (migration.workspaceBucketTransferred.isEmpty) {
              dataAccess.workspaceMigrationQuery.workspaceBucketTransferredCol
            } else {
              dataAccess.workspaceMigrationQuery.tmpBucketTransferredCol
            }, transferred)
        }
      } yield ()
    }


  final def migrationFinished(migrationId: Long, outcome: Outcome): MigrateAction[Unit] =
    nowTimestamp.flatMap { finished =>
      inTransaction { dataAccess =>
        dataAccess.workspaceMigrationQuery.migrationFinished(migrationId, finished, outcome).ignore
      }
    }


  final def withMigration(conditions: DataAccess => SQLActionBuilder)
                         (attempt: (WorkspaceMigration, Workspace) => MigrateAction[Unit])
  : MigrateAction[Unit] =
    for {
      (migration, workspace) <- inTransactionT { dataAccess =>
        (for {
          migrations <- dataAccess.workspaceMigrationQuery.selectMigrations(conditions(dataAccess))

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


  def apply(pollingInterval: FiniteDuration,
            dataSource: SlickDataSource,
            googleProjectToBill: GoogleProject,
            workspaceService: UserInfo => WorkspaceService,
            storageService: GoogleStorageService[IO],
            storageTransferService: GoogleStorageTransferService[IO],
            gcsDao: GoogleServicesDAO,
            samDao: SamDAO)
  : Behavior[Message] =
    Behaviors.setup { context =>

      def unsafeRunMigrateAction[A](action: MigrateAction[A]): Behavior[Message] = {
        try {
          action
            .run(
              MigrationDeps(
                dataSource,
                googleProjectToBill,
                workspaceService,
                storageService,
                storageTransferService,
                gcsDao,
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
        scheduler.startTimerAtFixedRate(RunMigration, pollingInterval)
        // two sts jobs are created per migration so run at twice frequency
        scheduler.startTimerAtFixedRate(RefreshTransferJobs, pollingInterval / 2)

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

