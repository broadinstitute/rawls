package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityQuery, ReadWriteAction}

import java.util.UUID

/**
  * This Repository class is a thin wrapper around SlickDataSource, wrapping the functions used by CompactEntityProvider.
  * By acting as an intermediary between CompactEntityProvider and CompactEntityComponent, this Repository class allows
  * for easy mocking in unit tests.
  *
  * @param dataSource slick datasource containing query implementations
  */
class CompactEntityRepository(val dataSource: SlickDataSource) {

  def queries: CompactEntityQuery = dataSource.dataAccess.compactEntityQuery

  // used frequently by entity writes, so included here for convenience and ease of mocking
  def updateLastModified(workspaceId: UUID): ReadWriteAction[Int] =
    dataSource.dataAccess.workspaceQuery.updateLastModified(workspaceId)
}
