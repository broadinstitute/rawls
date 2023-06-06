package org.broadinstitute.dsde.rawls.util

import io.sentry.SentryEvent

object SentryEventFilter {

  /**
   * Client side filtering for spammy Sentry events
   */
  def filterEvent(event: SentryEvent): SentryEvent =
    Option(event.getMessage) match {
      case Some(msg)
          if msg.getMessage.contains("requirement failed: count cannot be decreased") ||
            msg.getMessage.contains("pet service account not found") =>
        null
      case _ =>
        Option(event.getLogger) match {
          case Some(loggerName)
              if loggerName.equals("org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO") =>
            Option(event.getThrowable) match {
              case Some(throwable)
                  if throwable.getMessage.contains("requester pays bucket but no user project provided") ||
                    throwable.getMessage.contains("billing account for the owning project is disabled") =>
                null
              case _ =>
                event
            }
          case _ => event
        }
    }
}
