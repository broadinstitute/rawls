package org.broadinstitute.dsde.rawls.util

import io.sentry.dsn.Dsn
import io.sentry.event.Event
import io.sentry.event.helper.ShouldSendEventCallback
import io.sentry.{DefaultSentryClientFactory, SentryClient}

import scala.util.{Failure, Success, Try}

/* Filter for Sentry, which will ignore below exceptions from here at the source, without attempting to send to Sentry.
    - https://github.com/slick/slick/issues/1856 (https://broadworkbench.atlassian.net/browse/AS-217)
    - "pet service account not found" error: We are ignoring this exception because SubmissionMonitor actor is sending
      about 150-200 messages per minute to Sentry and this is depleting most of the error capacity in Sentry and
      putting us over quota (https://broadworkbench.atlassian.net/browse/BW-1125).
 */
class RawlsSentryEventFilter extends ShouldSendEventCallback {
  override def shouldSend(event: Event): Boolean = {
    // earlier versions of sentry-logback could pass nulls, so we wrap everything in a Try to be defensive.
    // by default this will run in a separate async thread so failures aren't a problem, but still.
    Try(!event.getMessage.contains("requirement failed: count cannot be decreased") &&
      !event.getMessage.contains("pet service account not found")
    ) match {
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
