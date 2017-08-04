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

//  implicit def updateHisto[A](implicit histo: Histogram): RetryCountConsumer[A] =
//    (a, count) => {
//      histo += count
//      a
//    }

  implicit def instrumentAccumulatingFuture[A](af: AccumulatingFuture[A])(implicit histo: Histogram, executionContext: ExecutionContext): Future[A] = {
    af.flatMap {
      case Left(errors) =>
        histo += (errors.tail.size + 1)
        Future.failed(errors.head)
      case Right((errors, a)) =>
        if (errors.nonEmpty) {
          histo += errors.size
        }
        Future.successful(a)
    }
  }

}
