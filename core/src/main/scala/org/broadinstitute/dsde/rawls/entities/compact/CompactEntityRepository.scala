package org.broadinstitute.dsde.rawls.entities.compact

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityRecord
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity}

import java.util.UUID
import scala.concurrent.Future

/**
  * This Repository class wraps all the methods in CompactEntityComponent inside a transaction, and returns
  * Futures instead of DBIOs. By acting as an intermediary between CompactEntityProvider and CompactEntityComponent,
  * this Repository class allows for easy mocking in unit tests.
  *
  * It is expected that callers will still declare outer transaction boundaries where appropriate.
  *
  * @param dataSource slick datasource to use for transactions
  */
class CompactEntityRepository(dataSource: SlickDataSource) {

  def createEntity(workspaceId: UUID, entity: Entity): Future[Int] = dataSource.inTransaction { dataAccess =>
    dataAccess.compactEntityQuery.createEntity(workspaceId, entity)
  }

  def getEntity(workspaceId: UUID, entityType: String, entityName: String): Future[Option[CompactEntityRecord]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.compactEntityQuery.getEntity(workspaceId, entityType, entityName)
    }

  def getReferencedIds(workspaceId: UUID, refs: Set[AttributeEntityReference]): Future[Seq[Long]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.compactEntityQuery.getReferencedIds(workspaceId, refs)
    }

  def deleteReferences(fromId: Long, toIds: Set[Long]): Future[Int] = dataSource.inTransaction { dataAccess =>
    dataAccess.compactEntityQuery.deleteReferences(fromId, toIds)
  }

  def upsertReferences(fromId: Long, toIds: Set[Long]): Future[Int] = dataSource.inTransaction { dataAccess =>
    dataAccess.compactEntityQuery.upsertReferences(fromId, toIds)
  }

}
