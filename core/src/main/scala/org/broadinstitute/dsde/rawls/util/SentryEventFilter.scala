package org.broadinstitute.dsde.rawls.util

import io.sentry.SentryEvent

object SentryEventFilter {
  /**
   * Client side filtering for spammy Sentry events
   */
  def filterEvent(event: SentryEvent): SentryEvent = {
    Option(event.getMessage) match {
      case Some(msg) if msg.getMessage.contains("requirement failed: count cannot be decreased") ||
        msg.getMessage.contains("pet service account not found") => null
      case _ => event
    }
  }
}
