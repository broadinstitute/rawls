package org.broadinstitute.dsde.rawls.util

import io.opencensus.scala.Tracing
import io.opencensus.scala.Tracing._
import io.opencensus.trace.Status
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsTracingContext}
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import scala.concurrent.{ExecutionContext, Future}

object TracingUtils {
  def traceDBIOWithParent[T, E <: Effect](name: String, parentContext: RawlsRequestContext)(
    f: RawlsRequestContext => DBIOAction[T, NoStream, E]
  )(implicit executor: ExecutionContext): DBIOAction[T, NoStream, E with Effect] = {
    val childContext =
      parentContext.copy(tracingSpan = Option(startSpanWithParent(name, parentContext.tracingSpan.orNull)))
    f(childContext).cleanUp { _ =>
      endSpan(childContext.tracingSpan.orNull, Status.OK)
      DBIO.successful(())
    }
  }

  def traceWithParent[T](name: String, parentContext: RawlsRequestContext)(
    f: RawlsRequestContext => Future[T]
  )(implicit ec: ExecutionContext): Future[T] =
    Tracing.traceWithParent(name, parentContext.tracingSpan.orNull) { span =>
      f(parentContext.copy(tracingSpan = Option(span)))
    }

  def traceDBIOWithParent[T, E <: Effect](name: String, parentContext: RawlsTracingContext)(
    f: RawlsTracingContext => DBIOAction[T, NoStream, E]
  )(implicit executor: ExecutionContext): DBIOAction[T, NoStream, E with Effect] = {
    val childContext =
      parentContext.copy(tracingSpan = Option(startSpanWithParent(name, parentContext.tracingSpan.orNull)))
    f(childContext).cleanUp { _ =>
      endSpan(childContext.tracingSpan.orNull, Status.OK)
      DBIO.successful(())
    }
  }

  def traceWithParent[T](name: String, parentContext: RawlsTracingContext)(
    f: RawlsTracingContext => Future[T]
  )(implicit ec: ExecutionContext): Future[T] =
    Tracing.traceWithParent(name, parentContext.tracingSpan.orNull) { span =>
      f(parentContext.copy(tracingSpan = Option(span)))
    }

  def trace[T](name: String)(f: RawlsTracingContext => Future[T])(implicit ec: ExecutionContext): Future[T] =
    Tracing.trace(name) { span =>
      f(RawlsTracingContext(Option(span)))
    }
}
