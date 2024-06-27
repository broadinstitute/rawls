package org.broadinstitute.dsde.rawls.util

import com.google.api.client.http.HttpResponseException
import com.google.cloud.http.BaseHttpServiceException
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
              // workbench-libs logs all error responses from Google APIs at ERROR level, including 4xx client errors.
              // these would get picked up by Sentry, but we don't want them to. Check the status code returned by
              // Google and filter out client errors.
              //
              // HttpResponseException covers GoogleJsonResponseException and TokenResponseException
              case Some(googleResponse: HttpResponseException) if isStatusCodeIgnorable(googleResponse.getStatusCode) =>
                null
              // BaseHttpServiceException covers BigQueryException, ResourceManagerException, StorageException
              case Some(googleService: BaseHttpServiceException) if isStatusCodeIgnorable(googleService.getCode) =>
                null
              case _ =>
                event
            }
          case _ => event
        }
    }

  private def isStatusCodeIgnorable(code: Int): Boolean =
    Option(code).nonEmpty && code > 0 && code / 100 < 5

}
