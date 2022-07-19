package org.broadinstitute.dsde.rawls.util

import io.sentry.SentryEvent

object SentryEventFilter {
  /**
   * Client side filtering for spammy Sentry events
   */
  def filterEvent(event: SentryEvent): SentryEvent = {
    val msg = Option(event.getMessage)
    if (msg.isDefined) {
      val actualMsg = msg.get.getMessage
      if (actualMsg.contains("requirement failed: count cannot be decreased") ||
        actualMsg.contains("pet service account not found")) {
        return null
      }
    }

    event
  }
}
