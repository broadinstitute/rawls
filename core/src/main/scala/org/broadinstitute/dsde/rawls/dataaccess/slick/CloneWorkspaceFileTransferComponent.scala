package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, PendingCloneWorkspaceFileTransfer}

import java.util.UUID

case class CloneWorkspaceFileTransferRecord(id: Long,
                                            destWorkspaceId: UUID,
                                            sourceWorkspaceId: UUID,
                                            copyFilesWithPrefix: String
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

    def * = (id,
             destWorkspaceId,
             sourceWorkspaceId,
             copyFilesWithPrefix
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
        fileTransfer <- cloneWorkspaceFileTransferQuery.filterOpt(workspaceId) { case (table, workspaceId) =>
          table.destWorkspaceId === workspaceId
        }
        sourceWorkspace <- workspaceQuery if sourceWorkspace.id === fileTransfer.sourceWorkspaceId
        destWorkspace <- workspaceQuery if destWorkspace.id === fileTransfer.destWorkspaceId
      } yield (destWorkspace.id,
               sourceWorkspace.bucketName,
               destWorkspace.bucketName,
               fileTransfer.copyFilesWithPrefix,
               destWorkspace.googleProjectId
      )

      query.result.map(results =>
        results.map {
          case (destWorkspaceId,
                sourceWorkspaceBucketName,
                destWorkspaceBucketName,
                copyFilesWithPrefix,
                destWorkspaceGoogleProjectId
              ) =>
            PendingCloneWorkspaceFileTransfer(destWorkspaceId,
                                              sourceWorkspaceBucketName,
                                              destWorkspaceBucketName,
                                              copyFilesWithPrefix,
                                              GoogleProjectId(destWorkspaceGoogleProjectId)
            )
        }
      )
    }

    def delete(destWorkspaceId: UUID): ReadWriteAction[Int] =
      findByDestWorkspaceId(destWorkspaceId).delete

    private def findByDestWorkspaceId(destWorkspaceId: UUID) =
      filter(rec => rec.destWorkspaceId === destWorkspaceId)
  }
}
