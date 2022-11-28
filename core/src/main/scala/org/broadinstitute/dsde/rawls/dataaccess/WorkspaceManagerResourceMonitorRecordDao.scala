package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsUserEmail}

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.Future

class WorkspaceManagerResourceMonitorRecordDao(val dataSource: SlickDataSource) {

  def create(jobRecordId: UUID, jobType: JobType, billingProjectName: String, userEmail: RawlsUserEmail): Future[Unit] = create(
    WorkspaceManagerResourceMonitorRecord(
      jobRecordId,
      jobType,
      None,
      Some(billingProjectName),
      Some(userEmail.value),
      Timestamp.from(Instant.now())
    )
  )

  def create(job: WorkspaceManagerResourceMonitorRecord): Future[Unit] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.create(job))

  def selectAll(): Future[Seq[WorkspaceManagerResourceMonitorRecord]] =
    dataSource.inTransaction(dataAccess => dataAccess.WorkspaceManagerResourceMonitorRecordQuery.getRecords)

  def selectByBillingProject(name: RawlsBillingProjectName): Future[Seq[WorkspaceManagerResourceMonitorRecord]] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.selectByBillingProject(name))

  def delete(job: WorkspaceManagerResourceMonitorRecord): Future[Boolean] = dataSource.inTransaction { dataAccess =>
    dataAccess.WorkspaceManagerResourceMonitorRecordQuery.delete(job)
  }

}
