package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceSettingRecord
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  RawlsUserSubjectId,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName,
  WorkspaceSetting,
  WorkspaceState
}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.WorkspaceSettingType
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import org.joda.time.DateTime
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Data access for rawls workspaces
  *
  * The intention of this class is to hide direct dependencies on Slick behind a relatively clean interface
  * to ease testability of higher level business logic.
  */
class WorkspaceRepository(dataSource: SlickDataSource) {

  def getWorkspace(workspaceId: UUID): Future[Option[Workspace]] = getWorkspace(workspaceId, None)

  def getWorkspace(workspaceId: UUID, attributeSpecs: Option[WorkspaceAttributeSpecs]): Future[Option[Workspace]] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.findV2WorkspaceById(workspaceId, attributeSpecs)
    }

  def getWorkspace(workspaceName: WorkspaceName): Future[Option[Workspace]] = getWorkspace(workspaceName, None)

  def getWorkspace(workspaceName: WorkspaceName,
                   attributeSpecs: Option[WorkspaceAttributeSpecs]
  ): Future[Option[Workspace]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.findV2WorkspaceByName(workspaceName, attributeSpecs)
    }

  def createWorkspace(workspace: Workspace): Future[Workspace] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.createOrUpdate(workspace)
    }

  def updateState(workspaceId: UUID, state: WorkspaceState): Future[Int] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.updateState(workspaceId, state)
    }

  def setFailedState(workspaceId: UUID, state: WorkspaceState, message: String): Future[Int] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.updateStateWithErrorMessage(workspaceId, state, message)
    }

  def deleteWorkspace(workspace: Workspace): Future[Boolean] = deleteWorkspace(workspace.toWorkspaceName)

  def deleteWorkspace(workspaceName: WorkspaceName): Future[Boolean] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspaceName)
    }

  def updateCompletedCloneWorkspaceFileTransfer(wsId: UUID, finishTime: DateTime): Future[Int] =
    dataSource.inTransaction(_.workspaceQuery.updateCompletedCloneWorkspaceFileTransfer(wsId, finishTime.toDate))

  def createMCWorkspace(workspaceId: UUID,
                        workspaceName: WorkspaceName,
                        attributes: AttributeMap,
                        parentContext: RawlsRequestContext,
                        state: WorkspaceState = WorkspaceState.Ready
  )(implicit ex: ExecutionContext): Future[Workspace] =
    dataSource.inTransaction { access =>
      for {
        _ <- access.workspaceQuery.getWorkspaceId(workspaceName).map { workspaceId =>
          if (workspaceId.isDefined)
            throw RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Conflict, s"Workspace '$workspaceName' already exists")
            )
        }
        currentDate = DateTime.now
        workspace = Workspace.buildMcWorkspace(
          namespace = workspaceName.namespace,
          name = workspaceName.name,
          workspaceId = workspaceId.toString,
          createdDate = currentDate,
          lastModified = currentDate,
          createdBy = parentContext.userInfo.userEmail.value,
          attributes = attributes,
          state
        )
        newWorkspace <- traceDBIOWithParent("saveMultiCloudWorkspace", parentContext)(_ =>
          access.workspaceQuery.createOrUpdate(workspace)
        )
      } yield newWorkspace
    }

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
                                     user: RawlsUserSubjectId
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
          _ <- access.workspaceSettingQuery.saveAll(workspaceId, workspaceSettings, user)
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
