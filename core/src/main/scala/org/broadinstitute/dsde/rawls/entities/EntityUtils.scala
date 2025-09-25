package org.broadinstitute.dsde.rawls.entities

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.StringValidationUtils
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.model.{AttributeName, Entity, ErrorReportSource}
import slick.dbio.Effect
import slick.jdbc.{ResultSetConcurrency, TransactionIsolation}
import slick.sql.SqlStreamingAction

import java.sql.SQLException
import scala.concurrent.{ExecutionContext, Future}

object EntityUtils extends StringValidationUtils with LazyLogging {
  implicit override val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def validateAttrName(attrName: AttributeName, entityType: String): Unit = {
    validateUserDefinedString(attrName.name)
    validateAttributeName(attrName, entityType)
  }

  def validateEntity(entity: Entity): Unit = {
    validateEntityType(entity.entityType)
    validateEntityName(entity.name)
    entity.attributes.keys.foreach(attrName => validateAttrName(attrName, entity.entityType))
  }

  /**
   * Retries a given query at increasing `sort_buffer_size` allocations. This method is designed to work around
   * the "Out of sort memory, consider increasing server sort buffer size" MySQL error, which manifests as a
   * SQLException with error code 1038.
   *
   * See also: https://bugs.mysql.com/bug.php?id=103318, https://bugs.mysql.com/bug.php?id=103225
   *
   * @param dataSource Slick data source to use for queries
   * @param query the query to execute
   * @param startingSortBufferSize sort_buffer_size value, in bytes, to use for the first attempt
   * @param maxSortBufferSize maximum sort_buffer_size value, in bytes, to allow
   * @param multiplier factor by which each subsequent query attempt increases its sort_buffer_size value
   * @param executionContext context for executing queries
   * @return the query result
   */
  def retryWithSortMemory[T](dataSource: SlickDataSource,
                             startingSortBufferSize: Int = 2097152, // 2MB
                             maxSortBufferSize: Int = 268435456, // 256MB
                             multiplier: Double = 2,
                             isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  )(op: => ReadWriteAction[T])(implicit
    executionContext: ExecutionContext
  ): Future[T] = {

    // helper method to actually execute the query
    def tryQuery(sortBufferSize: Long): Future[T] =
      dataSource
        .inTransaction(isolationLevel) { dataAccess =>
          for {
            // get the current value of MySQL sort_buffer_size
            defaultSortBufferSize <- dataAccess.compactEntityQuery.getSortBufferSetting
            // set sort buffer size to the ${sortBufferSize} argument
            _ <- dataAccess.compactEntityQuery.setSessionSortBuffer(sortBufferSize)
            // execute the requested operation, then reset sort buffer size to its original value
            result <- op andFinally dataAccess.compactEntityQuery.setSessionSortBuffer(defaultSortBufferSize)
          } yield result
        }
        .recoverWith {
          // TODO CORE-708: register some Prometheus metrics?
          // catch and handle error code 1038 "Out of sort memory, consider increasing server sort buffer size"
          case sqlEx: SQLException if sqlEx.getErrorCode == 1038 =>
            val nextMemoryAllocation = Math.round(sortBufferSize * multiplier)
            if (nextMemoryAllocation > maxSortBufferSize) {
              logger.warn(
                s"Out of retries for SQL query; requested sort memory allocation $nextMemoryAllocation is greater than allowed maximum $maxSortBufferSize"
              )
              Future.failed(sqlEx)
            } else {
              logger.warn(s"Retrying SQL query with sort memory allocation $sortBufferSize")
              tryQuery(nextMemoryAllocation)
            }
        }

    tryQuery(startingSortBufferSize)
  }

}
