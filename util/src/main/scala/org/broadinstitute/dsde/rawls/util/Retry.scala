package org.broadinstitute.dsde.rawls.util

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import akka.pattern._

/**
 * Created by tsharpe on 9/21/15.
 */
trait Retry {
  this: LazyLogging =>
  val system: ActorSystem

  type Predicate[A] = A => Boolean

  def always[A]: Predicate[A] = _ => true

  val defaultErrorMessage = "retry-able operation failed"

  def retry[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: Int => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(0, allBackoffIntervals)(op, pred, failureLogMessage)
  }

  def retryExponentially[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: Int => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(0, exponentialBackOffIntervals)(op, pred, failureLogMessage)
  }

  /**
   * will retry at the given interval until success or the overall timeout has passed
   * @param pred which failures to retry
   * @param interval how often to retry
   * @param timeout how long from now to give up
   * @param op what to try
   * @param executionContext
   * @tparam T
   * @return
   */
  def retryUntilSuccessOrTimeout[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(interval: FiniteDuration, timeout: FiniteDuration)(op: Int => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val trialCount = Math.ceil(timeout / interval).toInt
    retry(0, Seq.fill(trialCount)(interval))(op, pred, failureLogMessage)
  }

  private def retry[T](count: Int, remainingBackoffIntervals: Seq[FiniteDuration])(op: => Int => Future[T], pred: Predicate[Throwable], failureLogMessage: String)(implicit executionContext: ExecutionContext): Future[T] = {
    op(count).recoverWith {
      case t if pred(t) && !remainingBackoffIntervals.isEmpty =>
        logger.info(s"$failureLogMessage: ${remainingBackoffIntervals.size} retries remaining, retrying in ${remainingBackoffIntervals.head}", t)
        after(remainingBackoffIntervals.head, system.scheduler) {
          retry(count + 1, remainingBackoffIntervals.tail)(op, pred, failureLogMessage)
        }

      case t =>
        if (remainingBackoffIntervals.isEmpty) {
          logger.info(s"$failureLogMessage: no retries remaining", t)
        } else {
          logger.info(s"$failureLogMessage: retries remain but predicate failed, not retrying", t)
        }

        Future.failed(t)
    }
  }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)

  private def exponentialBackOffIntervals: Seq[FiniteDuration] = {
    val plainIntervals = Seq(1000 milliseconds, 2000 milliseconds, 4000 milliseconds, 8000 milliseconds, 16000 milliseconds, 32000 milliseconds)
    plainIntervals.map(i => addJitter(i, 1000 milliseconds))
  }

}
