package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord

import scala.concurrent.Future

class WorkspaceManagerResourceMonitorRecordDao(val dataSource: SlickDataSource) {

  def selectAll(): Future[Seq[WorkspaceManagerResourceMonitorRecord]] =
    dataSource.inTransaction(dataAccess => dataAccess.WorkspaceManagerResourceMonitorRecordQuery.getRecords)

  def delete(job: WorkspaceManagerResourceMonitorRecord): Future[Boolean] = dataSource.inTransaction { dataAccess =>
    dataAccess.WorkspaceManagerResourceMonitorRecordQuery.delete(job)
  }

}
