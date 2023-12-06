package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.model.RawlsBillingProjectName

import scala.concurrent.Future

case class WorkspaceManagerResourceMonitorRecordDao(dataSource: SlickDataSource) {

  def create(job: WorkspaceManagerResourceMonitorRecord): Future[Unit] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.create(job))

  def selectAll(): Future[Seq[WorkspaceManagerResourceMonitorRecord]] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.getRecords)

  def update(job: WorkspaceManagerResourceMonitorRecord): Future[Int] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.updateJob(job))

  def delete(job: WorkspaceManagerResourceMonitorRecord): Future[Boolean] =
    dataSource.inTransaction(_.WorkspaceManagerResourceMonitorRecordQuery.delete(job))

}
