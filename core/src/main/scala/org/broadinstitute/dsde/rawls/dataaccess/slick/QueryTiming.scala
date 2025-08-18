package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.slf4j.{Logger, LoggerFactory}
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import scala.annotation.unused
import scala.concurrent.ExecutionContext

trait QueryTiming {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Time a DBIOAction and log its duration.
   * 
   * @param hint a string to include in the log message to help identify the operation being timed
   * @param op the query to time
   * @param executionContext implicit; the execution context to run the query in.
   *                         Annotated as @unused to avoid IntelliJ warnings but
   *                         actually used implicitly.
   */
  def withTiming[T, U](hint: String)(
    op: => DBIOAction[T, NoStream, U with Effect]
  )(implicit @unused executionContext: ExecutionContext): DBIOAction[T, NoStream, U with Effect] =
    for {
      startTime <- DBIO.successful(System.nanoTime())
      queryResults <- op
      elapsed <- DBIO.successful(System.nanoTime() - startTime)
    } yield {
      logger.info(s"query timer: $hint completed in ${elapsed / 1000000} ms")
      queryResults
    }
}
