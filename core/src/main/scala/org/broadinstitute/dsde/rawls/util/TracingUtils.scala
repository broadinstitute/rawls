package org.broadinstitute.dsde.rawls.util

import cats.effect.IO
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.instrumentation.api.instrumenter.{Instrumenter, SpanKindExtractor}
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, RawlsTracingContext}
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object TracingUtils {
  // lazy to make sure GlobalOpenTelemetry is initialized
  private lazy val instrumenter: Instrumenter[String, Object] =
    Instrumenter
      .builder[String, Object](GlobalOpenTelemetry.get(), "TracingUtils", identity[String])
      .buildInstrumenter(SpanKindExtractor.alwaysInternal())

  def traceDBIOWithParent[T, E <: Effect](name: String, parentContext: RawlsRequestContext)(
    f: RawlsRequestContext => DBIOAction[T, NoStream, E]
  )(implicit executor: ExecutionContext): DBIOAction[T, NoStream, E with Effect] = {
    parentContext.otelContext match {
      case Some(otelContext) if instrumenter.shouldStart(otelContext, name) =>
        for {
          childContext <- DBIO.successful(instrumenter.start(otelContext, name))
          result <- f(parentContext.copy(otelContext = Option(childContext))).cleanUp { maybeThrowable =>
            instrumenter.end(childContext, name, name, maybeThrowable.orNull)
            DBIO.successful(())
          }
          _ = instrumenter.end(childContext, name, name, null)
        } yield result

      case None =>
        f(parentContext)
    }
  }

  def traceFutureWithParent[T](name: String, parentContext: RawlsRequestContext)(
    f: RawlsRequestContext => Future[T]
  )(implicit ec: ExecutionContext): Future[T] =
    parentContext.otelContext match {
      case Some(otelContext) if instrumenter.shouldStart(otelContext, name) =>
        for {
          childContext <- Future(instrumenter.start(otelContext, name))
          result <- f(parentContext.copy(otelContext = Option(childContext))).recoverWith {
            case e: Throwable =>
              instrumenter.end(childContext, name, name, e)
              Future.failed(e)
          }
          _ = instrumenter.end(childContext, name, name, null)
        } yield result

      case None =>
        f(parentContext)
    }

  def traceDBIOWithParent[T, E <: Effect](name: String, parentContext: RawlsTracingContext)(
    f: RawlsTracingContext => DBIOAction[T, NoStream, E]
  )(implicit executor: ExecutionContext): DBIOAction[T, NoStream, E with Effect] = {
    parentContext.otelContext match {
      case Some(otelContext) if instrumenter.shouldStart(otelContext, name) =>
        for {
          childContext <- DBIO.successful(instrumenter.start(otelContext, name))
          result <- f(parentContext.copy(otelContext = Option(childContext))).cleanUp { maybeThrowable =>
              instrumenter.end(childContext, name, name, maybeThrowable.orNull)
              DBIO.successful(())
          }
          _ = instrumenter.end(childContext, name, name, null)
        } yield result

      case None =>
        f(parentContext)
    }
  }

  def traceFutureWithParent[T](name: String, parentContext: RawlsTracingContext)(
    f: RawlsTracingContext => Future[T]
  )(implicit ec: ExecutionContext): Future[T] = {
    parentContext.otelContext match {
      case Some(otelContext) if instrumenter.shouldStart(otelContext, name) =>
        for {
          childContext <- Future(instrumenter.start(otelContext, name))
          result <- f(parentContext.copy(otelContext = Option(childContext))).recoverWith {
            case e: Throwable =>
              instrumenter.end(childContext, name, name, e)
              Future.failed(e)
          }
          _ = instrumenter.end(childContext, name, name, null)
        } yield result

      case None =>
        f(parentContext)
    }
  }

  def traceFuture[T](name: String)(f: RawlsTracingContext => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    traceFutureWithParent(name, RawlsTracingContext(otelContext = Option(Context.root())))(f)
  }

  def traceNakedWithParent[T](name: String, parentContext: RawlsTracingContext)(
    f: RawlsTracingContext => T
  ): T = {
    parentContext.otelContext match {
      case Some(otelContext) if instrumenter.shouldStart(otelContext, name) =>
        val childContext = instrumenter.start(otelContext, name)
        val result = Try(f(parentContext.copy(otelContext = Option(childContext))))
        instrumenter.end(childContext, name, name, result.failed.toOption.orNull)
        result.get
      case None =>
        f(parentContext)
    }
  }

  def traceIOWithContext[T](name: String, tracingContext: RawlsTracingContext
  )(f: RawlsTracingContext => IO[T]): IO[T] =
    tracingContext.otelContext match {
      case Some(otelContext) if instrumenter.shouldStart(otelContext, name) =>
        for {
          childContext <- IO(instrumenter.start(otelContext, name))
          result <- f(tracingContext.copy(otelContext = Option(childContext))).attempt
          _ = instrumenter.end(childContext, name, name, result.toTry.failed.toOption.orNull)
        } yield result.toTry.get
      case None =>
        f(tracingContext)
    }

  // creates a root span
  def traceIO[T](name: String)(f: RawlsTracingContext => IO[T]): IO[T] =
    traceIOWithContext(name, RawlsTracingContext(Option(Context.root())))(f)

  def setTraceSpanAttribute[T](parentContext: RawlsTracingContext, key: AttributeKey[T], value: T): Unit = {
    parentContext.otelContext.map(Span.fromContext).foreach(_.setAttribute(key, value))
  }

  def setTraceSpanAttribute[T](parentContext: RawlsRequestContext, key: AttributeKey[T], value: T): Unit = {
    setTraceSpanAttribute(parentContext.toTracingContext, key, value)
  }

}
