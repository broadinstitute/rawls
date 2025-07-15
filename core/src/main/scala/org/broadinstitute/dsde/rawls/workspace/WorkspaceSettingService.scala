package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.storage.BucketInfo.LifecycleRule.{LifecycleAction, LifecycleCondition}
import com.google.cloud.storage.BucketInfo.{LifecycleRule, SoftDeletePolicy}
import com.google.cloud.storage.Storage
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig._
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.WorkspaceSettingType
import org.broadinstitute.dsde.rawls.model.{
  CompactDataTablesSetting,
  ErrorReport,
  GcpBucketLifecycleSetting,
  GcpBucketRequesterPaysSetting,
  GcpBucketSoftDeleteSetting,
  PubliclyReadableSetting,
  RawlsRequestContext,
  SamResourceTypeNames,
  SamWorkspaceActions,
  SamWorkspacePolicyNames,
  SeparateSubmissionFinalOutputsSetting,
  UseCromwellGcpBatchBackendSetting,
  Workspace,
  WorkspaceName,
  WorkspaceSetting,
  WorkspaceSettingResponse,
  WorkspaceSettingTypes
}
import org.broadinstitute.dsde.rawls.util.WorkspaceSupport
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class WorkspaceSettingService(protected val ctx: RawlsRequestContext,
                              workspaceSettingRepository: WorkspaceSettingRepository,
                              val workspaceRepository: WorkspaceRepository,
                              gcsDAO: GoogleServicesDAO,
                              val samDAO: SamDAO,
                              googleStorageService: GoogleStorageService[IO],
                              entityService: EntityService
)(implicit protected val executionContext: ExecutionContext, ioRuntime: IORuntime)
    extends WorkspaceSupport
    with LazyLogging {

  private def getAllUsersRoleMapping(ctx: RawlsRequestContext) =
    samDAO.getAllUsersGroup(ctx).map { allUsersGroup =>
      Map[StorageRole, NonEmptyList[Identity]](
        StorageRole.CustomStorageRole(gcsDAO.terraBucketReaderRole) -> NonEmptyList.one(
          Identity.group(allUsersGroup.value)
        )
      )
    }

  def getWorkspaceSettingOfType(workspaceName: WorkspaceName,
                                settingType: WorkspaceSettingType
  ): Future[Option[WorkspaceSetting]] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.readSettings).flatMap { workspace =>
      workspaceSettingRepository.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, settingType)
    }

  // Returns applied settings on a workspace.
  def getWorkspaceSettings(workspaceName: WorkspaceName): Future[List[WorkspaceSetting]] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.readSettings).flatMap { workspace =>
      workspaceSettingRepository.getWorkspaceSettings(workspace.workspaceIdAsUUID)
    }

  // Returns true if the workspace has any pending settings.
  def workspaceHasPendingSettings(workspaceName: WorkspaceName): Future[Boolean] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.readSettings).flatMap { workspace =>
      workspaceSettingRepository.hasPendingSettings(workspace.workspaceIdAsUUID)
    }
  def setWorkspaceSettings(workspaceName: WorkspaceName,
                           workspaceSettings: List[WorkspaceSetting]
  ): Future[WorkspaceSettingResponse] = {

    /**
      * Perform basic validation checks on requested settings.
      */
    def validateSettings(requestedSettings: List[WorkspaceSetting]): Unit = {
      def validationErrorReport(settingType: WorkspaceSettingType, reason: String): ErrorReport = ErrorReport(
        s"Invalid $settingType configuration: $reason."
      )
      val validationErrors = requestedSettings.flatMap { setting =>
        setting match {
          case GcpBucketLifecycleSetting(GcpBucketLifecycleConfig(rules)) =>
            rules.flatMap { rule =>
              val actionValidation = rule.action.actionType match {
                case actionType if actionType.equals("Delete") => None
                case actionType =>
                  Some(validationErrorReport(setting.settingType, s"unsupported lifecycle action $actionType"))
              }
              val ageValidation = rule.conditions.age.collect {
                case age if age < 0 =>
                  validationErrorReport(setting.settingType, "age must be a non-negative integer")
              }
              val atLeastOneConditionValidation = rule.conditions match {
                case GcpBucketLifecycleCondition(None, None) =>
                  Some(validationErrorReport(setting.settingType, "at least one condition must be specified"))
                case GcpBucketLifecycleCondition(Some(prefixes), None) if prefixes.isEmpty =>
                  Some(
                    validationErrorReport(setting.settingType,
                                          "at least one prefix must be specified if matchesPrefix is the only condition"
                    )
                  )
                case _ => None
              }
              actionValidation ++ ageValidation ++ atLeastOneConditionValidation
            }
          case GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(retentionDuration)) =>
            retentionDuration match {
              case duration if (duration < 7.days.toSeconds || duration > 90.days.toSeconds) && duration != 0 =>
                Some(
                  validationErrorReport(
                    setting.settingType,
                    "retention duration must be from 7 to 90 days, or 0 to disable soft delete retention"
                  )
                )
              case _ => None
            }
          case GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(_))                 => None
          case SeparateSubmissionFinalOutputsSetting(SeparateSubmissionFinalOutputsConfig(_)) => None
          case UseCromwellGcpBatchBackendSetting(UseCromwellGcpBatchBackendConfig(_))         => None
          case PubliclyReadableSetting(PubliclyReadableConfig(_))                             => None
          case CompactDataTablesSetting(CompactDataTablesConfig(_))                           => None
        }
      }

      if (validationErrors.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest, "Invalid settings requested.", validationErrors)
        )
      }
    }

    /**
      * Apply a setting to a workspace. If the setting is successfully applied, update the database
      * and return None. If the setting fails to apply, remove the failed setting from the database
      * and return the setting type with an error report. If the setting is not supported, throw an
      * exception. We make more trips to the database here than necessary, but we support a small
      * number of setting types and it's easier to reason about this way.
      */
    def applySetting(workspace: Workspace,
                     setting: WorkspaceSetting
    ): Future[Option[(WorkspaceSettingType, ErrorReport)]] =
      (for {
        _ <- applySettingToBucket(workspace, setting)
        _ <- workspaceSettingRepository.markWorkspaceSettingApplied(workspace.workspaceIdAsUUID, setting.settingType)
      } yield None).recoverWith { case e =>
        logger.error(
          s"Failed to apply settings. [workspaceId=${workspace.workspaceIdAsUUID},settingType=${setting.settingType}]",
          e
        )
        val statusCode = e match {
          case re: RawlsExceptionWithErrorReport => re.errorReport.statusCode.getOrElse(StatusCodes.InternalServerError)
          case _                                 => StatusCodes.InternalServerError
        }
        workspaceSettingRepository
          .removePendingSetting(workspace.workspaceIdAsUUID, setting.settingType)
          .map(_ => Some((setting.settingType, ErrorReport(statusCode, e.getMessage))))
      }

    def applySettingToBucket(workspace: Workspace, workspaceSetting: WorkspaceSetting): Future[Unit] =
      workspaceSetting match {
        case GcpBucketLifecycleSetting(GcpBucketLifecycleConfig(rules)) =>
          val googleRules = rules.map { rule =>
            val conditionBuilder = LifecycleCondition.newBuilder()
            rule.conditions.matchesPrefix.map(prefixes => conditionBuilder.setMatchesPrefix(prefixes.toList.asJava))
            rule.conditions.age.map(age => conditionBuilder.setAge(age))

            val action = rule.action.actionType match {
              case actionType if actionType.equals("Delete") => LifecycleAction.newDeleteAction()

              // validated earlier but needed for completeness
              case _ =>
                throw new RawlsException(
                  "unsupported lifecycle action"
                )
            }

            new LifecycleRule(action, conditionBuilder.build())
          }
          gcsDAO.setBucketLifecycle(workspace.bucketName, googleRules, workspace.googleProjectId)

        case GcpBucketSoftDeleteSetting(GcpBucketSoftDeleteConfig(retentionDuration)) =>
          val policyBuilder = SoftDeletePolicy.newBuilder()
          policyBuilder.setRetentionDuration(Duration.ofSeconds(retentionDuration))
          val softDeletePolicy = policyBuilder.build()
          gcsDAO.setSoftDeletePolicy(workspace.bucketName, softDeletePolicy, workspace.googleProjectId)

        case GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(enabled)) =>
          gcsDAO.setRequesterPays(workspace.bucketName, enabled, workspace.googleProjectId)

        case PubliclyReadableSetting(PubliclyReadableConfig(enabled)) =>
          applyPublicReadableSetting(workspace, enabled)

        // SeparateSubmissionFinalOutputsSetting, UseCromwellGcpBatchBackendSetting, and CompactDataTablesSetting
        // are not bucket settings, so we do not need to apply anything here

        case CompactDataTablesSetting(CompactDataTablesConfig(enabled)) =>
          applyCompactDataTablesSetting(WorkspaceName(workspace.namespace, workspace.name), enabled)

        case SeparateSubmissionFinalOutputsSetting(SeparateSubmissionFinalOutputsConfig(_)) =>
          Future.successful(())

        case UseCromwellGcpBatchBackendSetting(UseCromwellGcpBatchBackendConfig(_)) =>
          Future.successful(())
      }

    validateSettings(workspaceSettings)
    for {
      workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.writeSettings)
      currentSettings <- workspaceSettingRepository.getWorkspaceSettings(workspace.workspaceIdAsUUID)
      newSettings = workspaceSettings.filterNot(currentSettings.contains(_))
      _ <- workspaceSettingRepository.createWorkspaceSettingsRecords(workspace.workspaceIdAsUUID,
                                                                     newSettings,
                                                                     ctx.userInfo.userSubjectId
      )
      applyFailures <- newSettings.traverse(s => applySetting(workspace, s))
    } yield {
      val successes = newSettings.filterNot { s =>
        applyFailures.flatten.exists { case (failedSettingType, _) =>
          failedSettingType == s.settingType
        }
      }
      WorkspaceSettingResponse(successes, applyFailures.flatten.toMap)
    }
  }

  /**
   * Calls sam to update the reader policy then update the buckets IAM to add or remove the allUsers group
   */
  private def applyPublicReadableSetting(workspace: Workspace, enabled: Boolean): Future[Unit] =
    for {
      // setPolicyPublic will only work if the caller has the appropriate permissions in Sam
      _ <- samDAO
        .setPolicyPublic(SamResourceTypeNames.workspace,
                         workspace.workspaceId,
                         SamWorkspacePolicyNames.reader,
                         enabled,
                         ctx
        )
        .recover {
          case e: RawlsExceptionWithErrorReport
              if e.errorReport.statusCode
                .contains(StatusCodes.NotFound) || e.errorReport.statusCode.contains(StatusCodes.Forbidden) =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Forbidden, "User does not have permission to update publicly readable setting")
            )
        }
      allUsersRoleMapping <- getAllUsersRoleMapping(ctx)
      requesterPays = List(Storage.BucketSourceOption.userProject(workspace.googleProjectId.value))
      iamPolicyAction =
        if (enabled) {
          googleStorageService.setIamPolicy(
            GcsBucketName(workspace.bucketName),
            allUsersRoleMapping,
            bucketSourceOptions = requesterPays
          )
        } else {
          googleStorageService.removeIamPolicy(
            GcsBucketName(workspace.bucketName),
            allUsersRoleMapping,
            bucketSourceOptions = requesterPays
          )
        }
      _ <- iamPolicyAction.compile.drain.unsafeToFuture()
    } yield ()

  /**
   * Call to handle entity attributes migration when compact data tables setting enabled.
   */
  private def applyCompactDataTablesSetting(workspaceName: WorkspaceName, enabled: Boolean): Future[Unit] =
    if (!enabled) {
      // Check if the setting is already enabled in the database
      getWorkspaceSettingOfType(workspaceName, WorkspaceSettingTypes.CompactDataTables).flatMap {
        case Some(CompactDataTablesSetting(CompactDataTablesConfig(true))) =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, "Cannot disable compact data tables setting once enabled.")
            )
          )
        case _ =>
          Future.successful(())
      }
    } else {
      // If compact data tables setting is enabled, we need to migrate the entity attributes.
      Future {
        entityService
          .quicksilverMigration(workspaceName = workspaceName)
          .map(_ => ())
          .recover { case e: Exception =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.InternalServerError, s"Quicksilver migration failed: ${e.getMessage}")
            )
          }
      }.flatten
    }
}
