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

  type AccumulatingFuture[A] = Future[Either[NonEmptyList[Throwable], (List[Throwable], A)]]

  def always[A]: Predicate[A] = _ => true

  val defaultErrorMessage = "retry-able operation failed"

  def retry[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retryInternal(allBackoffIntervals, pred, failureLogMessage)(op).flatMap {
      case Left(NonEmptyList(t, ts)) => Future.failed(t)
      case Right((_, a)) => Future.successful(a)
    }
  }

  def retryAccumulating[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): AccumulatingFuture[T] = {
    retryInternal(allBackoffIntervals, pred, failureLogMessage)(op)
  }

  def retryExponentially[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retryInternal(exponentialBackOffIntervals, pred, failureLogMessage)(op).flatMap {
      case Left(NonEmptyList(t, ts)) => Future.failed(t)
      case Right((_, a)) => Future.successful(a)
    }
  }

  def retryExponentiallyAccumulating[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): AccumulatingFuture[T] = {
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
    retryInternal(Seq.fill(trialCount)(interval), pred, failureLogMessage)(op).flatMap {
      case Left(NonEmptyList(t, ts)) => Future.failed(t)
      case Right((_, a)) => Future.successful(a)
    }
  }

  private def retryInternal[T](remainingBackoffIntervals: Seq[FiniteDuration],
                               pred: Predicate[Throwable],
                               failureLogMessage: String)
                              (op: () => Future[T])
                              (implicit executionContext: ExecutionContext): AccumulatingFuture[T] = {

    def inner(intervals: Seq[FiniteDuration], errors: => List[Throwable]): AccumulatingFuture[T] = {
      op().map(Right(errors, _)).recoverWith {
        case t if pred(t) && !remainingBackoffIntervals.isEmpty =>
          logger.info(s"$failureLogMessage: ${remainingBackoffIntervals.size} retries remaining, retrying in ${remainingBackoffIntervals.head}", t)
          after(remainingBackoffIntervals.head, system.scheduler) {
            inner(intervals.tail, t :: errors)
          }

        case t =>
          if (remainingBackoffIntervals.isEmpty) {
            logger.info(s"$failureLogMessage: no retries remaining", t)
          } else {
            logger.info(s"$failureLogMessage: retries remain but predicate failed, not retrying", t)
          }

          Future.successful(Left(NonEmptyList(t, errors)))
      }
    }

    inner(remainingBackoffIntervals, List.empty)
  }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)

  private def exponentialBackOffIntervals: Seq[FiniteDuration] = {
    val plainIntervals = Seq(1000 milliseconds, 2000 milliseconds, 4000 milliseconds, 8000 milliseconds, 16000 milliseconds, 32000 milliseconds)
    plainIntervals.map(i => addJitter(i, 1000 milliseconds))
  }
}
