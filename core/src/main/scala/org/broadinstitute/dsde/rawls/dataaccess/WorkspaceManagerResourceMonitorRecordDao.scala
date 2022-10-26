package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.Future

class WorkspaceManagerResourceMonitorRecordDao(val dataSource: SlickDataSource) {

  def create(jobRecordId: UUID, jobType: JobType, billingProjectName: String): Future[Unit] = create(
    WorkspaceManagerResourceMonitorRecord(
      jobRecordId,
      jobType,
      None,
      Option(billingProjectName),
      Timestamp.from(Instant.now())
    )
  )

  def create(job: WorkspaceManagerResourceMonitorRecord): Future[Unit] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.create(job))

  def selectAll(): Future[Seq[WorkspaceManagerResourceMonitorRecord]] =
    dataSource.inTransaction(dataAccess => dataAccess.WorkspaceManagerResourceMonitorRecordQuery.getRecords)

  def delete(job: WorkspaceManagerResourceMonitorRecord): Future[Boolean] = dataSource.inTransaction { dataAccess =>
    dataAccess.WorkspaceManagerResourceMonitorRecordQuery.delete(job)
  }

}
