package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceSettingRecord
import org.broadinstitute.dsde.rawls.model.{ErrorReport, RawlsUserSubjectId, WorkspaceSetting}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.WorkspaceSettingType
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Data access for rawls workspace settings
  *
  * The intention of this class is to hide direct dependencies on Slick behind a relatively clean interface
  * to ease testability of higher level business logic.
  */
class WorkspaceSettingRepository(dataSource: SlickDataSource) {

  // Return all applied settings for a workspace. Deleted and pending settings are not returned.
  def getWorkspaceSettings(workspaceId: UUID): Future[List[WorkspaceSetting]] =
    dataSource.inTransaction { access =>
      access.workspaceSettingQuery.listSettingsForWorkspaceByStatus(workspaceId,
                                                                    WorkspaceSettingRecord.SettingStatus.Applied
      )
    }

  // Create new settings for a workspace as pending. If there are any existing pending settings, throw an exception.
  def createWorkspaceSettingsRecords(workspaceId: UUID,
                                     workspaceSettings: List[WorkspaceSetting],
                                     userId: RawlsUserSubjectId
  )(implicit
    ec: ExecutionContext
  ): Future[List[WorkspaceSetting]] =
    dataSource.inTransaction(
      access =>
        for {
          pendingSettingsForWorkspace <- access.workspaceSettingQuery.listSettingsForWorkspaceByStatus(
            workspaceId,
            WorkspaceSettingRecord.SettingStatus.Pending
          )
          _ = if (pendingSettingsForWorkspace.nonEmpty) {
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Conflict, s"Workspace $workspaceId already has pending settings")
            )
          }
          _ <- access.workspaceSettingQuery.saveAll(workspaceId, workspaceSettings, userId)
        } yield workspaceSettings
      // We use Serializable here to ensure that concurrent transactions to create settings
      // for the same workspace are committed in order and do not interfere with each other
      ,
      TransactionIsolation.Serializable
    )

  // Transition old Applied settings to Deleted and Pending settings to Applied
  def markWorkspaceSettingApplied(workspaceId: UUID, workspaceSettingType: WorkspaceSettingType)(implicit
    ec: ExecutionContext
  ): Future[Int] =
    dataSource.inTransaction { access =>
      for {
        _ <- access.workspaceSettingQuery.updateSettingStatus(workspaceId,
                                                              workspaceSettingType,
                                                              WorkspaceSettingRecord.SettingStatus.Applied,
                                                              WorkspaceSettingRecord.SettingStatus.Deleted
        )
        res <- access.workspaceSettingQuery.updateSettingStatus(workspaceId,
                                                                workspaceSettingType,
                                                                WorkspaceSettingRecord.SettingStatus.Pending,
                                                                WorkspaceSettingRecord.SettingStatus.Applied
        )
      } yield res
    }

  // Fully remove all pending records for a workspace from database. Does not transition records to Deleted.
  def removePendingSetting(workspaceId: UUID, workspaceSettingType: WorkspaceSettingType): Future[Int] =
    dataSource.inTransaction { access =>
      access.workspaceSettingQuery.deleteSettingTypeForWorkspaceByStatus(workspaceId,
                                                                         workspaceSettingType,
                                                                         WorkspaceSettingRecord.SettingStatus.Pending
      )
    }
}
