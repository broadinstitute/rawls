package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PendingCloneWorkspaceFileTransfer}
import org.joda.time.DateTime

import java.sql.Timestamp
import java.util.UUID

case class CloneWorkspaceFileTransferRecord(id: Long,
                                            destWorkspaceId: UUID,
                                            sourceWorkspaceId: UUID,
                                            copyFilesWithPrefix: String,
                                            created: Timestamp,
                                            finished: Option[Timestamp],
                                            outcome: Option[String]
)

trait CloneWorkspaceFileTransferComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class CloneWorkspaceFileTransferTable(tag: Tag)
      extends Table[CloneWorkspaceFileTransferRecord](tag, "CLONE_WORKSPACE_FILE_TRANSFER") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def destWorkspaceId = column[UUID]("DEST_WORKSPACE_ID")
    def sourceWorkspaceId = column[UUID]("SOURCE_WORKSPACE_ID")
    def copyFilesWithPrefix = column[String]("COPY_FILES_WITH_PREFIX", O.Length(254))
    def created = column[Timestamp]("CREATED")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")

    def * = (id,
             destWorkspaceId,
             sourceWorkspaceId,
             copyFilesWithPrefix,
             created,
             finished,
             outcome
    ) <> (CloneWorkspaceFileTransferRecord.tupled, CloneWorkspaceFileTransferRecord.unapply)
  }

  object cloneWorkspaceFileTransferQuery extends TableQuery(new CloneWorkspaceFileTransferTable(_)) {
    def save(destWorkspaceId: UUID, sourceWorkspaceId: UUID, copyFilesWithPrefix: String): ReadWriteAction[Int] =
      findByDestWorkspaceId(destWorkspaceId).result.flatMap { results =>
        if (results.isEmpty) {
          cloneWorkspaceFileTransferQuery.map { t =>
            (t.destWorkspaceId, t.sourceWorkspaceId, t.copyFilesWithPrefix)
          } += (destWorkspaceId, sourceWorkspaceId, copyFilesWithPrefix)
        } else {
          DBIO.successful(0)
        }
      }

    def listPendingTransfers(workspaceId: Option[UUID] = None): ReadAction[Seq[PendingCloneWorkspaceFileTransfer]] = {
      val query = for {
        fileTransfer <-
          cloneWorkspaceFileTransferQuery
            .filter(_.finished.isEmpty)
            .filterOpt(workspaceId) { case (table, workspaceId) =>
              table.destWorkspaceId === workspaceId
            }
        sourceWorkspace <- workspaceQuery if sourceWorkspace.id === fileTransfer.sourceWorkspaceId
        destWorkspace <- workspaceQuery if destWorkspace.id === fileTransfer.destWorkspaceId
      } yield (destWorkspace.id,
               sourceWorkspace.bucketName,
               destWorkspace.bucketName,
               fileTransfer.copyFilesWithPrefix,
               destWorkspace.googleProjectId,
               fileTransfer.created,
               fileTransfer.finished,
               fileTransfer.outcome
      )

      query.result.map(results =>
        results.map {
          case (destWorkspaceId,
                sourceWorkspaceBucketName,
                destWorkspaceBucketName,
                copyFilesWithPrefix,
                destWorkspaceGoogleProjectId,
                created,
                finished,
                outcome
              ) =>
            PendingCloneWorkspaceFileTransfer(
              destWorkspaceId,
              sourceWorkspaceBucketName,
              destWorkspaceBucketName,
              copyFilesWithPrefix,
              GoogleProjectId(destWorkspaceGoogleProjectId),
              new DateTime(created),
              finished.map(new DateTime(_)),
              outcome
            )
        }
      )
    }

    def update(pendingCloneWorkspaceFileTransfer: PendingCloneWorkspaceFileTransfer): ReadWriteAction[Int] =
      findByDestWorkspaceId(pendingCloneWorkspaceFileTransfer.destWorkspaceId)
        .map(ft => (ft.finished, ft.outcome))
        .update(
          (pendingCloneWorkspaceFileTransfer.finished.map(f => new Timestamp(f.getMillis)),
           pendingCloneWorkspaceFileTransfer.outcome
          )
        )

    def delete(destWorkspaceId: UUID): ReadWriteAction[Int] =
      findByDestWorkspaceId(destWorkspaceId).delete

    private def findByDestWorkspaceId(destWorkspaceId: UUID) =
      filter(rec => rec.destWorkspaceId === destWorkspaceId)
  }
}
