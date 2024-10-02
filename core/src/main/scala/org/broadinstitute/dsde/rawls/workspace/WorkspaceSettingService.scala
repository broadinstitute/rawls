package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import cats.implicits._
import com.google.cloud.storage.BucketInfo.LifecycleRule.{LifecycleAction, LifecycleCondition}
import com.google.cloud.storage.BucketInfo.{LifecycleRule, SoftDeletePolicy}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig._
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.WorkspaceSettingType
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  GcpBucketLifecycleSetting,
  GcpBucketRequesterPaysSetting,
  GcpBucketSoftDeleteSetting,
  RawlsRequestContext,
  SamWorkspaceActions,
  SeparateSubmissionFinalOutputsSetting,
  Workspace,
  WorkspaceName,
  WorkspaceSetting,
  WorkspaceSettingResponse
}
import org.broadinstitute.dsde.rawls.util.WorkspaceSupport
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class WorkspaceSettingService(protected val ctx: RawlsRequestContext,
                              workspaceSettingRepository: WorkspaceSettingRepository,
                              val workspaceRepository: WorkspaceRepository,
                              gcsDAO: GoogleServicesDAO,
                              val samDAO: SamDAO
)(implicit protected val executionContext: ExecutionContext)
    extends WorkspaceSupport
    with LazyLogging {

  // Returns applied settings on a workspace.
  def getWorkspaceSettings(workspaceName: WorkspaceName): Future[List[WorkspaceSetting]] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read).flatMap { workspace =>
      workspaceSettingRepository.getWorkspaceSettings(workspace.workspaceIdAsUUID)
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
        workspaceSettingRepository
          .removePendingSetting(workspace.workspaceIdAsUUID, setting.settingType)
          .map(_ => Some((setting.settingType, ErrorReport(StatusCodes.InternalServerError, e.getMessage))))
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

        case SeparateSubmissionFinalOutputsSetting(SeparateSubmissionFinalOutputsConfig(_)) =>
          // SeparateSubmissionFinalOutputsSetting is not a bucket setting, so we don't need to apply anything here
          Future.successful(())

        case _ => throw new RawlsException("unsupported workspace setting")
      }

    validateSettings(workspaceSettings)
    for {
      workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own)
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
}
