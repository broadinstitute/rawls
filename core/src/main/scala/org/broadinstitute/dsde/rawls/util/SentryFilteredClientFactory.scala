package org.broadinstitute.dsde.rawls.util

import io.sentry.dsn.Dsn
import io.sentry.event.Event
import io.sentry.event.helper.ShouldSendEventCallback
import io.sentry.{DefaultSentryClientFactory, Sentry, SentryClient}

import scala.util.{Failure, Success, Try}

/* Filter for Sentry, which will ignore exceptions from https://github.com/slick/slick/issues/1856
   here at the source, without attempting to send to Sentry.
 */
class RawlsSentryEventFilter extends ShouldSendEventCallback {
  override def shouldSend(event: Event): Boolean = {
    // earlier versions of sentry-logback could pass nulls, so we wrap everything in a Try to be defensive.
    // by default this will run in a separate async thread so failures aren't a problem, but still.
    Try(event.getMessage.contains("requirement failed: count cannot be decreased")) match {
      case Success(send) => send
      case Failure(_) => true
    }
  }
}

/*  Custom Sentry client which will call our custom shouldSend method above.
    This factory class must be specified in sentry.properties with
    factory=org.broadinstitute.dsde.rawls.util.SentryFilteredClientFactory
 */
class SentryFilteredClientFactory extends DefaultSentryClientFactory {
  override def createSentryClient(dsn: Dsn): SentryClient = {
    val sentry: SentryClient = super.createSentryClient(dsn)
    sentry.addShouldSendEventCallback(new RawlsSentryEventFilter)
    sentry
  }
}
