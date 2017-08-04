package org.broadinstitute.dsde.rawls.util

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.pattern._
import cats.data.NonEmptyList

/**
 * Created by tsharpe on 9/21/15.
 */
trait Retry {
  this: LazyLogging =>
  val system: ActorSystem

  type Predicate[A] = A => Boolean

  /**
    * A Future that has potentially been retried, with accumulated errors.
    * There are 3 cases:
    * 1. The future failed 1 or more times, and the final result is an error.
    *   - This is represented as {{{Left(NonEmptyList(errors))}}}
    * 2. The future failed 1 or more times, but eventually succeeded.
    *   - This is represented as {{{Right(List(errors), A)}}}
    * 3. The future succeeded the first time.
    *   - This is represented as {{{Right(List.empty, A)}}}
    */
  type AccumulatingFuture[A] = Future[Either[NonEmptyList[Throwable], (List[Throwable], A)]]

  def always[A]: Predicate[A] = _ => true

  val defaultErrorMessage = "retry-able operation failed"

  def retry[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retryInternal(allBackoffIntervals, pred, failureLogMessage)(op)
  }

  def retryExponentially[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retryInternal(exponentialBackOffIntervals, pred, failureLogMessage)(op)
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
  def retryUntilSuccessOrTimeout[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(interval: FiniteDuration, timeout: FiniteDuration)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val trialCount = Math.ceil(timeout / interval).toInt
    retryInternal(Seq.fill(trialCount)(interval), pred, failureLogMessage)(op)
  }

  private def retryInternal[T](backoffIntervals: Seq[FiniteDuration],
                               pred: Predicate[Throwable],
                               failureLogMessage: String)
                              (op: () => Future[T])
                              (implicit executionContext: ExecutionContext): AccumulatingFuture[T] = {

    def inner(remainingBackoffIntervales: Seq[FiniteDuration], errors: => List[Throwable]): AccumulatingFuture[T] = {
      op().map(Right(errors, _)).recoverWith {
        case t if pred(t) && !remainingBackoffIntervales.isEmpty =>
          logger.info(s"$failureLogMessage: ${remainingBackoffIntervales.size} retries remaining, retrying in ${remainingBackoffIntervales.head}", t)
          after(remainingBackoffIntervales.head, system.scheduler) {
            inner(remainingBackoffIntervales.tail, t :: errors)
          }

        case t =>
          if (remainingBackoffIntervales.isEmpty) {
            logger.info(s"$failureLogMessage: no retries remaining", t)
          } else {
            logger.info(s"$failureLogMessage: retries remain but predicate failed, not retrying", t)
          }

          Future.successful(Left(NonEmptyList(t, errors)))
      }
    }

    inner(backoffIntervals, List.empty)
  }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)

  private def exponentialBackOffIntervals: Seq[FiniteDuration] = {
    val plainIntervals = Seq(1000 milliseconds, 2000 milliseconds, 4000 milliseconds, 8000 milliseconds, 16000 milliseconds, 32000 milliseconds)
    plainIntervals.map(i => addJitter(i, 1000 milliseconds))
  }

  /**
    * Converts an AccumulatingFuture[A] to a Future[A].
    */
  protected implicit def accumulatingFutureToFuture[A](af: AccumulatingFuture[A])(implicit executionContext: ExecutionContext): Future[A] = {
    af.flatMap {
      // take the head (most recent) error
      case Left(NonEmptyList(t, _)) => Future.failed(t)
      // return the successful result, throw out any errors
      case Right((_, a)) => Future.successful(a)
    }
  }

}
