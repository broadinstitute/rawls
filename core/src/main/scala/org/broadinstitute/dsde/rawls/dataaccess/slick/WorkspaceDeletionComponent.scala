package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceDeletionState.WorkspaceDeletionState
import org.broadinstitute.dsde.rawls.monitor.migration.ColumnName
import slick.lifted.{PrimaryKey, ProvenShape}

import java.sql.Timestamp
import java.util.UUID

// TODO better name  -- this deals solely with external resource orchestration
object WorkspaceDeletionState extends Enumeration {
  type WorkspaceDeletionState = Value
  val NotStarted, AppDeletionStarted, AppDeletionFinished, RuntimeDeletionStarted, RuntimeDeletionFinished,
    WsmDeletionStarted, WsmDeletionFinished = Value
}

final case class WorkspaceDeletionRecord(id: Int,
                                         workspaceId: UUID,
                                         appDeletionStarted: Option[Timestamp],
                                         appDeletionFinished: Option[Timestamp],
                                         runtimeDeletionStarted: Option[Timestamp],
                                         runtimeDeletionFinished: Option[Timestamp],
                                         wsmDeletionStarted: Option[Timestamp],
                                         wsmDeletionFinished: Option[Timestamp]
) {

  def getState(): WorkspaceDeletionState =
    if (appDeletionStarted.isEmpty) {
      WorkspaceDeletionState.NotStarted
    } else if (appDeletionStarted.isDefined && appDeletionFinished.isEmpty) {
      WorkspaceDeletionState.AppDeletionStarted
    } else if (appDeletionFinished.isDefined && runtimeDeletionStarted.isEmpty) {
      WorkspaceDeletionState.AppDeletionFinished
    } else if (runtimeDeletionStarted.isDefined && runtimeDeletionFinished.isEmpty) {
      WorkspaceDeletionState.RuntimeDeletionStarted
    } else if (runtimeDeletionFinished.isDefined && wsmDeletionStarted.isEmpty) {
      WorkspaceDeletionState.RuntimeDeletionFinished
    } else if (wsmDeletionStarted.isDefined && wsmDeletionFinished.isEmpty) {
      WorkspaceDeletionState.WsmDeletionStarted
    } else if (wsmDeletionFinished.isDefined) {
      WorkspaceDeletionState.WsmDeletionFinished
    } else {
      throw new Exception("whoops")
    }

}

trait WorkspaceDeletionComponent {
  this: DriverComponent =>

  import driver.api._

  class WorkspaceDeletionTable(tag: Tag) extends Table[WorkspaceDeletionRecord](tag, "WORKSPACE_DELETION_STATE") {

    def id: Rep[Int] = column[Int]("ID", O.PrimaryKey)

    def workspaceId: Rep[UUID] = column[UUID]("WORKSPACE_ID")

    def appDeletionStarted: Rep[Option[Timestamp]] = column[Option[Timestamp]]("APP_DELETION_STARTED")

    def appDeletionFinished: Rep[Option[Timestamp]] = column[Option[Timestamp]]("APP_DELETION_FINISHED")

    def runtimeDeletionStarted: Rep[Option[Timestamp]] = column[Option[Timestamp]]("RUNTIME_DELETION_STARTED")

    def runtimeDeletionFinished: Rep[Option[Timestamp]] = column[Option[Timestamp]]("RUNTIME_DELETION_FINISHED")

    def wsmDeletionStarted: Rep[Option[Timestamp]] = column[Option[Timestamp]]("WSM_DELETION_STARTED")

    def wsmDeletionFinished: Rep[Option[Timestamp]] = column[Option[Timestamp]]("WSM_DELETION_FINISHED")

    override def * : ProvenShape[WorkspaceDeletionRecord] = (
      id,
      workspaceId,
      appDeletionStarted,
      appDeletionFinished,
      runtimeDeletionStarted,
      runtimeDeletionFinished,
      wsmDeletionStarted,
      wsmDeletionFinished
    ) <> ((WorkspaceDeletionRecord.apply _).tupled, WorkspaceDeletionRecord.unapply)
  }

  object WorkspaceDeletionQuery extends TableQuery(new WorkspaceDeletionTable(_)) {
    val query = TableQuery[WorkspaceDeletionTable]

    def create(deletion: WorkspaceDeletionRecord): WriteAction[Unit] = (query += deletion).map(_ => ())

    def delete(workspaceId: UUID) =
      selectByWorkspaceId(workspaceId).delete
//    }uniqueResult[WorkspaceDeletion](findDeletionRecord(workspaceId)).flatMap {
//        case None => DBIO.successful(false)
//        case Some(workspaceRecord) =>
//          // should we be deleting ALL workspace-related things inside of this method?
//          workspaceAttributes(findByIdQuery(workspaceRecord.id)).result.flatMap(recs =>
//            DBIO.seq(deleteWorkspaceAttributes(recs.map(_._2)))
//          ) andThen {
//            findByIdQuery(workspaceRecord.id).delete
//          } map { count =>
//            count > 0
//          }
//      }
//    }

    def findDeletionRecord(workspaceId: UUID): ReadAction[Option[WorkspaceDeletionRecord]] =
      uniqueResult(selectByWorkspaceId(workspaceId))
    private def selectByWorkspaceId(workspaceId: UUID) =
      query.filter(_.workspaceId === workspaceId)

    def setAppDeletionStarted(workspaceId: UUID, dt: Timestamp) =
      selectByWorkspaceId(workspaceId).map(_.appDeletionStarted).update(Some(dt))

    def setAppDeletionFinished(workspaceId: UUID, dt: Timestamp) =
      selectByWorkspaceId(workspaceId).map(_.appDeletionFinished).update(Some(dt))

    def setRuntimeDeletionStarted(workspaceId: UUID, dt: Timestamp) =
      selectByWorkspaceId(workspaceId).map(_.runtimeDeletionStarted).update(Some(dt))

    def setRuntimeDeletionFinished(workspaceId: UUID, dt: Timestamp) =
      selectByWorkspaceId(workspaceId).map(_.runtimeDeletionFinished).update(Some(dt))

    def setWsmDeletionStarted(workspaceId: UUID, dt: Timestamp) =
      selectByWorkspaceId(workspaceId).map(_.wsmDeletionStarted).update(Some(dt))

    def setWsmDeletionFinished(workspaceId: UUID, dt: Timestamp) =
      selectByWorkspaceId(workspaceId).map(_.wsmDeletionFinished).update(Some(dt))
  }

}
