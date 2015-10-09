package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try,Success,Failure}
import scala.concurrent.duration._

import akka.pattern._

/**
 * Created by tsharpe on 9/21/15.
 */
trait Retry {
  val system: ActorSystem

  def retry[T](pred: (Throwable) => Boolean = always)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(allBackoffIntervals)(op,pred)
  }

  private def retry[T](remainingBackoffIntervals: Seq[FiniteDuration])(op: => () => Future[T], pred: (Throwable) => Boolean )(implicit executionContext: ExecutionContext): Future[T] = {
    op().recoverWith {
      case t if pred(t) && !remainingBackoffIntervals.isEmpty => after(remainingBackoffIntervals.head, system.scheduler) {
        retry(remainingBackoffIntervals.tail)(op, pred)
      }
    }
  }

  private def always( throwable: Throwable ) = { true }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)
}
