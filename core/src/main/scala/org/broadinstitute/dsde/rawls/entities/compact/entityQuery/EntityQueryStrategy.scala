package org.broadinstitute.dsde.rawls.entities.compact.entityQuery

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityRecord
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import slick.dbio.Effect
import slick.jdbc.TransactionIsolation.ReadCommitted
import slick.jdbc.{ResultSetConcurrency, ResultSetType}
import slick.sql.SqlStreamingAction

import scala.concurrent.Future

case class CountAndSource(count: Int, source: Source[CompactEntityRecord, _])

trait EntityQueryStrategy {
  val repository: CompactEntityRepository

  def getCountAndSource: Future[CountAndSource]

  def streamQuery(
    count: Int,
    query: SqlStreamingAction[Seq[CompactEntityRecord], CompactEntityRecord, Effect.Read]
  ): Source[CompactEntityRecord, NotUsed] = {
    import repository.dataSource.dataAccess.driver.api._
    if (count == 0) {
      // if there are no results, we can just return an empty source
      Source.empty
    } else {
      // otherwise, we need to stream the results
      Source.fromPublisher(
        repository.dataSource.database.stream(
          query.transactionally
            .withTransactionIsolation(ReadCommitted)
            .withStatementParameters(rsType = ResultSetType.ForwardOnly,
                                     rsConcurrency = ResultSetConcurrency.ReadOnly,
                                     fetchSize = repository.dataSource.fetchSize
            )
        )
      )
    }
  }

}
