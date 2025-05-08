package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.model.EntityQuery
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Paginated query that returns zero or one entities by entity name.
  */
class FilterByNameStrategy(override val repository: CompactEntityRepository,
                           workspaceId: UUID,
                           entityType: String,
                           entityQuery: EntityQuery
)(implicit val executionContext: ExecutionContext)
    extends EntityQueryStrategy {
  override def getCountAndSource: Future[CountAndSource] = {
    val entityName = entityQuery.columnFilter.get.term
    repository.dataSource.inTransaction(ReadCommitted) { _ =>
      repository.queries.getEntity(workspaceId, entityType, entityName)
    } map {
      case Some(entityRec) => CountAndSource(1, Source.single(entityRec))
      case None            => CountAndSource(0, Source.empty)
    }
  }
}
