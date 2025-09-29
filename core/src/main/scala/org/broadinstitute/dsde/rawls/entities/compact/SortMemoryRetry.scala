package org.broadinstitute.dsde.rawls.entities.compact

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.RandomStringUtils
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderMetrics
import slick.jdbc.TransactionIsolation

import java.sql.SQLException
import scala.concurrent.{ExecutionContext, Future}
import scala.math.random

/**
 * Trait that adds support for retrying a given SQL query with increasingly larger `sort_buffer_size`
 * memory allocations.
 */
trait SortMemoryRetry extends EntityProviderMetrics with LazyLogging {

  /**
   * Retries a given query at increasing `sort_buffer_size` allocations. This method is designed to work around
   * the "Out of sort memory, consider increasing server sort buffer size" MySQL error, which manifests as a
   * SQLException with error code 1038.
   *
   * See also: https://bugs.mysql.com/bug.php?id=103318, https://bugs.mysql.com/bug.php?id=103225
   *
   * @param dataSource Slick data source to use for queries
   * @param functionName hint, for metrics, describing the caller who requires retries
   * @param startingSortBufferSize sort_buffer_size value, in bytes, to use for the first attempt
   * @param maxSortBufferSize maximum sort_buffer_size value, in bytes, to allow
   * @param multiplier factor by which each subsequent query attempt increases its sort_buffer_size value
   * @param executionContext context for executing queries
   * @return the query result
   */
  def retryWithSortMemory[T](dataSource: SlickDataSource,
                             functionName: String,
                             startingSortBufferSize: Int = 2097152, // 2MB
                             maxSortBufferSize: Int = 268435456, // 256MB
                             multiplier: Double = 1.66,
                             isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  )(op: => ReadWriteAction[T])(implicit
    executionContext: ExecutionContext
  ): Future[T] = {

    // encapsulate query results and retry metadata
    case class RetryResult(result: T, numAttempts: Int, finalAllocation: Long)

    // generate a short id to uniquely identify this query attempt; this is purely for nice log messages
    val queryId = RandomStringUtils.insecure().nextAlphanumeric(8)
    logger.trace(s"SQL query with retryWithSortMemory [$queryId] attempts starting ...")

    // helper method to actually execute the query
    def tryQuery(sortBufferSize: Long, retryIndex: Int): Future[RetryResult] =
      dataSource
        .inTransaction(isolationLevel) { dataAccess =>
          for {
            // get the current value of MySQL sort_buffer_size
            defaultSortBufferSize <- dataAccess.compactEntityQuery.getSortBufferSetting
            // set sort buffer size to the ${sortBufferSize} argument
            _ <- dataAccess.compactEntityQuery.setSessionSortBuffer(sortBufferSize)
            // execute the requested operation, then reset sort buffer size to its original value
            result <- op andFinally dataAccess.compactEntityQuery.setSessionSortBuffer(defaultSortBufferSize)
          } yield RetryResult(result, retryIndex, sortBufferSize)
        }
        .recoverWith {
          // catch and handle error code 1038 "Out of sort memory, consider increasing server sort buffer size"
          case sqlEx: SQLException if sqlEx.getErrorCode == 1038 =>
            // Generate a jitter factor between 0.85 and 1.15. Since the MySQL bug is not easily predictable, but
            // IS deterministic for a given dataset+memory allocation, we apply jitter here to the allocation to
            // avoid getting stuck in a deterministic failure state.
            val jitter = 0.85 + (random * (1.15 - 0.85))
            val nextMemoryAllocation = Math.round(sortBufferSize * multiplier * jitter)
            if (nextMemoryAllocation > maxSortBufferSize) {
              logger.warn(
                s"Out of retries for SQL query with retryWithSortMemory [$queryId] after ${retryIndex + 1} attempt(s); requested sort memory allocation $nextMemoryAllocation is greater than allowed maximum $maxSortBufferSize"
              )
              Future.failed(sqlEx)
            } else {
              logger.trace(
                s"SQL query with retryWithSortMemory [$queryId] retry attempt #${retryIndex + 1} with sort memory allocation $sortBufferSize"
              )
              tryQuery(nextMemoryAllocation, retryIndex + 1)
            }
        }

    tryQuery(startingSortBufferSize, 0) map { r: RetryResult =>
      if (r.numAttempts > 0) {
        logger.info(s"SQL query with retryWithSortMemory [$queryId] succeeded after ${r.numAttempts} retries.")
      }
      // send metrics to Prometheus
      recordSortMemoryRetryResult(functionName, r.numAttempts, r.finalAllocation)
      r.result
    }
  }
}
