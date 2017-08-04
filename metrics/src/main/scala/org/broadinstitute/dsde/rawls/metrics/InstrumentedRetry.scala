package org.broadinstitute.dsde.rawls.metrics

import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Histogram
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 8/3/17.
  */
trait InstrumentedRetry extends Retry {
  this: WorkbenchInstrumented with LazyLogging =>

  /**
    * Converts an AccumulatingFuture[A] to a Future[A].
    * Given an implicit Histogram, instruments the number of failures in the histogram.
    */
  protected implicit def accumulatingFutureToFuture[A](af: AccumulatingFuture[A])(implicit histo: Histogram, executionContext: ExecutionContext): Future[A] = {
    af.flatMap {
      case Left(errors) =>
        histo += errors.toList.size
        Future.failed(errors.head)
      case Right((errors, a)) =>
        histo += errors.size
        Future.successful(a)
    }
  }

}
